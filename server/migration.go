package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
)

type migrationAdapter interface {
	LastKey(context.Context) (int64, error)
	GetLabels(context.Context) (map[int64]comatproto.LabelDefs_Label, error)
}

type boltAdapter struct {
	db *bolt.DB
}

func newBoltAdapter(ctx context.Context, path string) (migrationAdapter, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{
		Timeout:  1 * time.Second,
		ReadOnly: true,
	})
	if err != nil {
		return nil, fmt.Errorf("opening bolt DB: %w", err)
	}
	return &boltAdapter{db: db}, nil
}

func (a *boltAdapter) LastKey(ctx context.Context) (int64, error) {
	var lastKey int64
	err := a.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(boltBucketName)).Cursor()
		k, _ := c.Last()
		if k == nil {
			return nil
		}
		n, err := decodeKey(k)
		if err != nil {
			return err
		}
		lastKey = n
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("looking up last allocated key in the old DB: %w", err)
	}
	return lastKey, nil
}

func (a *boltAdapter) GetLabels(ctx context.Context) (map[int64]comatproto.LabelDefs_Label, error) {
	labels := map[int64]comatproto.LabelDefs_Label{}
	err := a.db.View(func(tx *bolt.Tx) error {
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
		return nil, fmt.Errorf("failed to read entries from the old DB: %w", err)
	}
	return labels, nil
}

type sqliteAdapter struct {
	db *gorm.DB
}

func newSqliteAdapter(ctx context.Context, path string) (migrationAdapter, error) {
	db, err := gorm.Open(sqlite.Open(path), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to DB: %w", err)
	}
	return &sqliteAdapter{db: db}, nil
}

func (a *sqliteAdapter) LastKey(context.Context) (int64, error) {
	var lastKey int64
	err := a.db.Model(&Entry{}).Select("seq").Order("seq desc").Limit(1).Pluck("seq", &lastKey).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return 0, fmt.Errorf("failed to query last existing key: %w", err)
	}
	return lastKey, nil
}

func (a *sqliteAdapter) GetLabels(context.Context) (map[int64]comatproto.LabelDefs_Label, error) {
	var entries []Entry
	err := a.db.Model(&entries).
		Find(&entries).Error
	if err != nil {
		return nil, err
	}

	r := map[int64]comatproto.LabelDefs_Label{}
	for _, e := range entries {
		r[e.Seq] = e.ToLabel()
	}
	return r, nil
}
