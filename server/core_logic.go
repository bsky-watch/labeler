package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/rs/zerolog"
	"gorm.io/gorm"
)

func (s *Server) writeLabel(ctx context.Context, newLabel Entry) (bool, error) {
	log := zerolog.Ctx(ctx)
	updated := false
	lastKey := int64(0)
	var lastErr error
	for i := 0; i < 5; i++ {
		err := s.db.Transaction(func(tx *gorm.DB) error {
			updated = false
			lastKey = 0
			err := tx.Model(&Entry{}).Select("seq").Order("seq desc").Limit(1).Pluck("seq", &lastKey).Error
			if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("failed to query last existing key: %w", err)
			}

			var entries []Entry
			err = tx.Model(&Entry{}).
				Where("src = ? and val = ? and uri = ? and cid = ? and seq <= ?",
					newLabel.Src, newLabel.Val, newLabel.Uri, newLabel.Cid, lastKey).
				Order("seq desc").Limit(1).Find(&entries).Error
			if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("failed to query existing labels: %w", err)
			}

			noOp := false // default for the case we don't find any matches.
			if newLabel.Neg {
				// If the label is a negation - default to not writing it, since we don't
				// have anything to negate in the first place.
				noOp = true
			}
			if len(entries) > 0 {
				e := entries[0]
				noOp = true
				if e.Neg != newLabel.Neg {
					noOp = false
				}
				if e.Exp != newLabel.Exp {
					noOp = false
				}
			}

			if noOp {
				return nil
			}
			updated = true

			if err := tx.Create(&newLabel).Error; err != nil {
				return fmt.Errorf("creating new entry: %w", err)
			}

			// XXX: it's still possible to end up with redundant/duplicate entries:
			// concurrent transactions will not see each other's writes in the next
			// query, but still can be both committed successfully.
			var newEntries int64
			err = tx.Model(&Entry{}).
				Where("src = ? and val = ? and uri = ? and cid = ? and seq > ? and seq < ?",
					newLabel.Src, newLabel.Val, newLabel.Uri, newLabel.Cid, lastKey, newLabel.Seq).
				Count(&newEntries).Error
			if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("failed to query existing labels: %w", err)
			}
			if newEntries > 0 {
				return fmt.Errorf("new labels for the same subject were written concurrently, rolling back")
			}
			return nil
		}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		lastErr = err
		if err != nil {
			log.Info().Err(err).Msgf("Transaction failed: %s", err)
			continue
		}
		if updated {
			highestKey.WithLabelValues(s.did).Set(float64(lastKey + 1))
		}
		return updated, nil
	}
	return false, fmt.Errorf("failed to write the new label: %w", lastErr)
}

func dedupeAndNegateEntries(entries []Entry) []Entry {
	skip := map[string]map[string]map[string]map[string]bool{}
	r := []Entry{}
	for i := len(entries) - 1; i >= 0; i-- {
		e := entries[i]
		if skip[e.Src] == nil {
			skip[e.Src] = map[string]map[string]map[string]bool{}
		}
		if skip[e.Src][e.Val] == nil {
			skip[e.Src][e.Val] = map[string]map[string]bool{}
		}
		if skip[e.Src][e.Val][e.Uri] == nil {
			skip[e.Src][e.Val][e.Uri] = map[string]bool{}
		}

		if e.Neg {
			skip[e.Src][e.Val][e.Uri][e.Cid] = true
		}
		if skip[e.Src][e.Val][e.Uri][e.Cid] {
			continue
		}
		r = append(r, e)
		skip[e.Src][e.Val][e.Uri][e.Cid] = true
	}
	return r
}
