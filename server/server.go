// Package server implements an ATproto labeler, using SQLite as storage.
//
// Example usage:
//
//	    server, err := server.New(ctx, config.DBFile, config.DID, key)
//	    if err != nil {
//		       return fmt.Errorf("instantiating a server: %w", err)
//	    }
//	    http.Handle("/xrpc/com.atproto.label.subscribeLabels", server.Subscribe())
//	    http.Handle("/xrpc/com.atproto.label.queryLabels", server.Query())
//	    http.ListenAndServe(":8080", nil)
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"slices"
	"sync"
	"time"

	"golang.org/x/exp/maps"

	"gitlab.com/yawning/secp256k1-voi/secec"
	bolt "go.etcd.io/bbolt"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	comatproto "github.com/bluesky-social/indigo/api/atproto"

	"bsky.watch/labeler/config"
	"bsky.watch/labeler/sign"
)

const boltBucketName = "Labels"

type Server struct {
	db         *gorm.DB
	did        string
	privateKey *secec.PrivateKey

	mu            sync.RWMutex
	wakeChans     []chan struct{}
	allowedLabels map[string]bool
}

// NewWithConfig creates a new server instance using parameters provided in the config.
func NewWithConfig(ctx context.Context, cfg *config.Config) (*Server, error) {
	cfg.UpdateLabelValues()

	key, err := sign.ParsePrivateKey(cfg.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("parsing private key: %w", err)
	}

	switch {
	case cfg.SQLiteDB != "":
		if cfg.DBFile != "" {
			err := migrateBoltToSQLite(ctx, cfg.DBFile, cfg.SQLiteDB)
			if err != nil {
				return nil, fmt.Errorf("migrating data to sqlite: %w", err)
			}
		}
		return newWithSQLite(ctx, cfg.SQLiteDB, cfg.DID, key)
	default:
		return nil, fmt.Errorf("no database location provided")
	}
}

func newWithSQLite(ctx context.Context, dbpath string, did string, privateKey *secec.PrivateKey) (*Server, error) {
	db, err := gorm.Open(sqlite.Open(dbpath), &gorm.Config{
		SkipDefaultTransaction: true,
		PrepareStmt:            true,
		Logger: logger.New(log.New(os.Stdout, "\r\n", log.LstdFlags), logger.Config{
			SlowThreshold:             10 * time.Second,
			LogLevel:                  logger.Warn,
			IgnoreRecordNotFoundError: false,
			Colorful:                  true,
		}),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to DB: %w", err)
	}

	if err := db.AutoMigrate(&Entry{}); err != nil {
		return nil, fmt.Errorf("failed to update DB schema: %w", err)
	}

	s := &Server{
		db:         db,
		did:        did,
		privateKey: privateKey,
	}

	var lastKey int64
	err = db.Model(&Entry{}).Select("seq").Order("seq desc").Limit(1).Pluck("seq", &lastKey).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, fmt.Errorf("failed to query last existing key: %w", err)
	}
	highestKey.WithLabelValues(s.did).Set(float64(lastKey))
	activeSubscriptions.WithLabelValues(s.did).Set(0)

	return s, nil
}

func migrateBoltToSQLite(ctx context.Context, boltDB string, sqliteDB string) error {
	oldDB, err := bolt.Open(boltDB, 0600, &bolt.Options{
		Timeout:  1 * time.Second,
		ReadOnly: true,
	})
	if err != nil {
		return fmt.Errorf("opening old DB: %w", err)
	}
	var lastBoltKey int64
	err = oldDB.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(boltBucketName)).Cursor()
		k, _ := c.Last()
		if k == nil {
			return nil
		}
		n, err := decodeKey(k)
		if err != nil {
			return err
		}
		lastBoltKey = n
		return nil
	})
	if err != nil {
		return fmt.Errorf("looking up last allocated key in the old DB: %w", err)
	}

	newDB, err := gorm.Open(sqlite.Open(sqliteDB), &gorm.Config{})
	if err != nil {
		return fmt.Errorf("failed to connect to DB: %w", err)
	}
	if err := newDB.AutoMigrate(&Entry{}); err != nil {
		return fmt.Errorf("failed to update DB schema: %w", err)
	}
	var lastSQLiteKey int64
	err = newDB.Model(&Entry{}).Select("seq").Order("seq desc").Limit(1).Pluck("seq", &lastSQLiteKey).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return fmt.Errorf("failed to query last existing key: %w", err)
	}

	if lastBoltKey <= lastSQLiteKey {
		// No migration needed.
		// XXX: we don't check if the labels in SQLite are actually the same.
		return nil
	}
	if lastSQLiteKey != 0 {
		return fmt.Errorf("new DB is not empty and old DB has more entries than the new one. Not sure how to proceed")
	}

	labels := map[int64]comatproto.LabelDefs_Label{}
	err = oldDB.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(boltBucketName)).ForEach(func(k, v []byte) error {
			if len(v) == 0 {
				// XXX: skipping empty padding entries, used only when replacing
				// software for existing labeler.
				return nil
			}
			n, err := decodeKey(k)
			if err != nil {
				return err
			}
			var label comatproto.LabelDefs_Label
			if err := json.Unmarshal(v, &label); err != nil {
				return err
			}

			labels[n] = label
			return nil
		})
	})
	if err != nil {
		return fmt.Errorf("failed to read entries from the old DB: %w", err)
	}

	dummyServer := &Server{
		db: newDB,
	}

	// This will fail if newDB is not empty.
	return dummyServer.ImportEntries(labels)
}

// AddLabel updates the internal state and writes the label to the database.
//
// Note that it will ignore values that have no effect (e.g., if the label already exists,
// or trying to negate a label that doesn't exist). Return value indicates if
// there was a change or not.
func (s *Server) AddLabel(label comatproto.LabelDefs_Label) (bool, error) {
	if label.Src == "" {
		label.Src = s.did
	}
	if label.Src == "" {
		return false, fmt.Errorf("missing `src`")
	}
	if label.Ver == nil {
		var n int64 = 1
		label.Ver = &n
	}
	label.Cts = time.Now().Format(time.RFC3339)
	label.Sig = nil // We don't store signatures and always generate them on demand.

	start := time.Now()
	r, err := s.writeLabel(*(&Entry{}).FromLabel(0, label))
	duration := time.Since(start)
	if err != nil {
		writeLatency.WithLabelValues(s.did, "error").Observe(duration.Seconds())
		return false, err
	}
	if r {
		writeLatency.WithLabelValues(s.did, "written").Observe(duration.Seconds())
	} else {
		writeLatency.WithLabelValues(s.did, "noop").Observe(duration.Seconds())
	}
	if r {
		go s.wakeUpSubs()
	}
	return r, nil
}

func (s *Server) wakeUpSubs() {
	s.mu.Lock()
	for _, ch := range s.wakeChans {
		// Do a non-blocking write. The channel is buffered,
		// so if a write would to block - subscription is going to wake up anyway.
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	s.mu.Unlock()
}

func ptr[T any](v T) *T { return &v }

// LabelEntries returns all non-negated label entries for the provided label name.
// Does not filter out expired entries.
func (s *Server) LabelEntries(ctx context.Context, labelName string) ([]comatproto.LabelDefs_Label, error) {
	var entries []Entry
	err := s.db.Model(&entries).
		Where("val = ?", labelName).
		Order("seq asc").
		Find(&entries).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}

	return entriesToLabels(dedupeAndNegateEntries(entries)), nil
}

// SetAllowedLabels limits what label values can be used for new labels.
// Old existing labels are not affected, and label negations are also always allowed.
// By default all labels are allowed.
func (s *Server) SetAllowedLabels(labels []string) {
	s.mu.Lock()
	s.allowedLabels = map[string]bool{}
	for _, l := range labels {
		s.allowedLabels[l] = true
	}
	s.mu.Unlock()
}

// IsEmpty returns true if there are no labels in the database.
func (s *Server) IsEmpty() (bool, error) {
	count := int64(0)
	err := s.db.Model(&Entry{}).Count(&count).Error
	if err != nil {
		return false, err
	}
	return count == 0, nil
}

// ImportEntries populates an empty server with the given entries. Each entry is written at
// a sequence number equal to the map key.
func (s *Server) ImportEntries(entries map[int64]comatproto.LabelDefs_Label) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	empty, err := s.IsEmpty()
	if err != nil {
		return err
	}
	if !empty {
		return fmt.Errorf("database is not empty")
	}

	keys := maps.Keys(entries)
	slices.Sort(keys)

	for _, ks := range splitInBatches(keys, 1000) {
		values := make([]Entry, 0, len(ks))
		for _, k := range ks {
			values = append(values, *(&Entry{}).FromLabel(k, entries[k]))
		}

		err := s.db.Create(&values).Error
		if err != nil {
			return err
		}
	}
	return nil
}

func splitInBatches[T any](s []T, batchSize int) [][]T {
	var r [][]T
	for i := 0; i < len(s); i += batchSize {
		if i+batchSize < len(s) {
			r = append(r, s[i:i+batchSize])
		} else {
			r = append(r, s[i:])
		}
	}
	return r
}
