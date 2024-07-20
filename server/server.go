package server

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"gitlab.com/yawning/secp256k1-voi/secec"
	bolt "go.etcd.io/bbolt"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/rs/zerolog"
)

const bucketName = "Labels"

type Server struct {
	db         *bolt.DB
	did        string
	privateKey *secec.PrivateKey

	mu        sync.RWMutex
	labels    map[string]map[string]map[string]map[string]Entry // src -> uri -> label -> cid -> entry
	wakeChans []chan struct{}
}

func New(ctx context.Context, path string, did string, key *secec.PrivateKey) (*Server, error) {
	if key == nil {
		return nil, fmt.Errorf("signing key is required")
	}

	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		return err
	})
	if err != nil {
		return nil, err
	}

	s := &Server{
		db:         db,
		did:        did,
		privateKey: key,
	}
	if err := s.replayStream(ctx); err != nil {
		return nil, err
	}

	return s, nil
}

type Entry comatproto.LabelDefs_Label

func (s *Server) replayStream(ctx context.Context) error {
	log := zerolog.Ctx(ctx)

	s.mu.Lock()
	defer s.mu.Unlock()

	s.labels = map[string]map[string]map[string]map[string]Entry{}

	log.Info().Msgf("Loading database from disk...")
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bucketName)).ForEach(func(k, v []byte) error {
			entry := &Entry{}
			if err := json.Unmarshal(v, entry); err != nil {
				return err
			}
			if entry.Neg != nil && *entry.Neg {
				s.locked_applyLabelRemoval(*entry, false)
			} else {
				s.locked_applyLabelCreation(*entry, false)
			}
			return nil
		})
	})
	if err != nil {
		return err
	}
	log.Info().Msgf("Loading done.")
	return nil
}

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
	switch {
	case label.Neg != nil && *label.Neg:
		return s.removeLabel(Entry(label))
	default:
		return s.addLabel(Entry(label))
	}
}

func (s *Server) addLabel(entry Entry) (bool, error) {
	// TODO: check if this label is in our config.

	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.locked_applyLabelCreation(entry, true) {
		// Label doesn't actually change the state in any way.
		return false, nil
	}

	err := s.writeEntry(entry)
	if err != nil {
		return false, err
	}

	r := s.locked_applyLabelCreation(entry, false)
	go s.wakeUpSubs()
	return r, nil
}

func (s *Server) removeLabel(entry Entry) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.locked_applyLabelRemoval(entry, true) {
		// Label doesn't actually change the state in any way.
		return false, nil
	}

	err := s.writeEntry(entry)
	if err != nil {
		return false, err
	}

	r := s.locked_applyLabelRemoval(entry, false)
	go s.wakeUpSubs()
	return r, nil
}

func (s *Server) writeEntry(entry Entry) error {
	value, err := json.Marshal(&entry)
	if err != nil {
		return fmt.Errorf("marshaling record: %w", err)
	}

	err = s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		key, _ := b.Cursor().Last()
		var n int64
		if key != nil {
			k, err := decodeKey(key)
			if err != nil {
				return err
			}
			n = k
		}
		n++
		b.Put(encodeKey(n), value)
		return nil
	})
	if err != nil {
		return fmt.Errorf("writing the record to disk: %w", err)
	}
	return nil
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
