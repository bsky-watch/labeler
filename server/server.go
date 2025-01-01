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
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"golang.org/x/exp/maps"

	"github.com/imax9000/gormzerolog"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/rs/zerolog"
	"gitlab.com/yawning/secp256k1-voi/secec"
	"gorm.io/driver/postgres"
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
	log := zerolog.Ctx(ctx)
	cfg.UpdateLabelValues()

	key, err := sign.ParsePrivateKey(cfg.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("parsing private key: %w", err)
	}

	switch {
	case cfg.PostgresURL != "":
		var migrator migrationAdapter
		if cfg.DBFile != "" {
			m, err := newBoltAdapter(ctx, cfg.DBFile)
			if err != nil {
				return nil, fmt.Errorf("creating migration adapter: %w", err)
			}
			migrator = m
		} else if cfg.SQLiteDB != "" {
			m, err := newSqliteAdapter(ctx, cfg.SQLiteDB)
			if err != nil {
				return nil, fmt.Errorf("creating migration adapter: %w", err)
			}
			migrator = m
		}
		if migrator != nil {
			log.Info().Msgf("Found an old database specified in the config file, checking if migration is needed...")
			if err := migrateOldDataToPostgres(ctx, migrator, cfg); err != nil {
				return nil, fmt.Errorf("migrating data from old DB: %w", err)
			}
		}
		return newServer(ctx, cfg.PostgresURL, cfg.DID, key)
	case cfg.SQLiteDB != "":
		var migrator migrationAdapter
		if cfg.DBFile != "" {
			m, err := newBoltAdapter(ctx, cfg.DBFile)
			if err != nil {
				return nil, fmt.Errorf("creating migration adapter: %w", err)
			}
			migrator = m
		}
		if migrator != nil {
			log.Info().Msgf("Found an old database specified in the config file, checking if migration is needed...")
			if err := migrateOldDataToSQLite(ctx, migrator, cfg); err != nil {
				return nil, fmt.Errorf("migrating data from old DB: %w", err)
			}
		}
		return newWithSQLite(ctx, cfg.SQLiteDB, cfg.DID, key)
	default:
		return nil, fmt.Errorf("no database location provided")
	}
}

func newServer(ctx context.Context, dbUrl string, did string, privateKey *secec.PrivateKey) (*Server, error) {
	dbCfg, err := pgxpool.ParseConfig(dbUrl)
	if err != nil {
		return nil, fmt.Errorf("parsing DB URL: %w", err)
	}
	dbCfg.MaxConns = 1024
	dbCfg.MinConns = 3
	dbCfg.MaxConnLifetime = 6 * time.Hour
	conn, err := pgxpool.NewWithConfig(ctx, dbCfg)
	if err != nil {
		return nil, fmt.Errorf("connecting to postgres: %w", err)
	}

	sqldb := stdlib.OpenDBFromPool(conn)

	db, err := gorm.Open(postgres.New(postgres.Config{
		Conn: sqldb,
	}), &gorm.Config{
		SkipDefaultTransaction: true,
		PrepareStmt:            true,
		Logger: gormzerolog.New(&logger.Config{
			SlowThreshold:             3 * time.Second,
			IgnoreRecordNotFoundError: true,
		}, nil),
	})
	if err != nil {
		return nil, fmt.Errorf("connecting to the database: %w", err)
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

func newWithSQLite(ctx context.Context, dbpath string, did string, privateKey *secec.PrivateKey) (*Server, error) {
	db, err := gorm.Open(sqlite.Open(dbpath), &gorm.Config{
		SkipDefaultTransaction: true,
		PrepareStmt:            true,
		Logger: gormzerolog.New(&logger.Config{
			SlowThreshold:             10 * time.Second,
			IgnoreRecordNotFoundError: false,
		}, nil),
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

func migrateOldDataToPostgres(ctx context.Context, source migrationAdapter, cfg *config.Config) error {
	log := zerolog.Ctx(ctx)

	oldLastKey, err := source.LastKey(ctx)
	if err != nil {
		return fmt.Errorf("failed to read the last key from old DB: %w", err)
	}

	dbCfg, err := pgxpool.ParseConfig(cfg.PostgresURL)
	if err != nil {
		return fmt.Errorf("parsing DB URL: %w", err)
	}
	dbCfg.MaxConns = 1024
	dbCfg.MinConns = 3
	dbCfg.MaxConnLifetime = 6 * time.Hour
	conn, err := pgxpool.NewWithConfig(ctx, dbCfg)
	if err != nil {
		return fmt.Errorf("connecting to postgres: %w", err)
	}

	sqldb := stdlib.OpenDBFromPool(conn)

	newDb, err := gorm.Open(postgres.New(postgres.Config{
		Conn: sqldb,
	}), &gorm.Config{
		SkipDefaultTransaction: true,
		PrepareStmt:            true,
		Logger: gormzerolog.New(&logger.Config{
			SlowThreshold:             3 * time.Second,
			IgnoreRecordNotFoundError: true,
		}, nil),
	})
	if err != nil {
		return fmt.Errorf("connecting to the database: %w", err)
	}
	if err := newDb.AutoMigrate(&Entry{}); err != nil {
		return fmt.Errorf("failed to update DB schema: %w", err)
	}

	var lastKey int64
	err = newDb.Model(&Entry{}).Select("seq").Order("seq desc").Limit(1).Pluck("seq", &lastKey).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return fmt.Errorf("failed to query last existing key: %w", err)
	}
	if oldLastKey <= lastKey {
		// No migration needed.
		// XXX: we don't check if the labels in SQLite are actually the same.
		log.Info().Msgf("No migration needed.")
		return nil
	}
	if lastKey != 0 {
		return fmt.Errorf("new DB is not empty and old DB has more entries than the new one. Not sure how to proceed")
	}

	log.Info().Msgf("Starting data migration...")

	labels, err := source.GetLabels(ctx)
	if err != nil {
		return fmt.Errorf("failed to read the labels from the old DB: %w", err)
	}

	dummyServer := &Server{
		db: newDb,
	}

	// This will fail if newDB is not empty.
	return dummyServer.ImportEntries(labels)
}

func migrateOldDataToSQLite(ctx context.Context, source migrationAdapter, cfg *config.Config) error {
	log := zerolog.Ctx(ctx)

	oldLastKey, err := source.LastKey(ctx)
	if err != nil {
		return fmt.Errorf("failed to read the last key from old DB: %w", err)
	}

	newDb, err := gorm.Open(sqlite.Open(cfg.SQLiteDB), &gorm.Config{
		SkipDefaultTransaction: true,
		PrepareStmt:            true,
		Logger: gormzerolog.New(&logger.Config{
			SlowThreshold:             10 * time.Second,
			IgnoreRecordNotFoundError: false,
		}, nil),
	})
	if err != nil {
		return fmt.Errorf("failed to open the new DB: %w", err)
	}

	var lastKey int64
	err = newDb.Model(&Entry{}).Select("seq").Order("seq desc").Limit(1).Pluck("seq", &lastKey).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return fmt.Errorf("failed to query last existing key: %w", err)
	}
	if oldLastKey <= lastKey {
		// No migration needed.
		// XXX: we don't check if the labels in SQLite are actually the same.
		log.Info().Msgf("No migration needed.")
		return nil
	}
	if lastKey != 0 {
		return fmt.Errorf("new DB is not empty and old DB has more entries than the new one. Not sure how to proceed")
	}

	log.Info().Msgf("Starting data migration...")

	labels, err := source.GetLabels(ctx)
	if err != nil {
		return fmt.Errorf("failed to read the labels from the old DB: %w", err)
	}

	dummyServer := &Server{
		db: newDb,
	}

	// This will fail if newDB is not empty.
	return dummyServer.ImportEntries(labels)
}

// AddLabel updates the internal state and writes the label to the database.
//
// Note that it will ignore values that have no effect (e.g., if the label already exists,
// or trying to negate a label that doesn't exist). Return value indicates if
// there was a change or not.
func (s *Server) AddLabel(ctx context.Context, label comatproto.LabelDefs_Label) (bool, error) {
	s.mu.Lock()
	if len(s.allowedLabels) > 0 && !s.allowedLabels[label.Val] {
		s.mu.Unlock()
		return false, fmt.Errorf("we are not allowed to apply the label %q", label.Val)
	}
	s.mu.Unlock()

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
	r, err := s.writeLabel(ctx, *(&Entry{}).FromLabel(0, label))
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
