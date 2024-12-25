package server

import (
	"errors"
	"fmt"

	"gorm.io/gorm"
)

func (s *Server) writeLabel(newLabel Entry) (bool, error) {
	updated := false
	lastKey := int64(0)
	for i := 0; i < 5; i++ {
		err := immediateTransaction(s.db, func(tx *gorm.DB) error {
			updated = false
			lastKey = 0
			err := tx.Model(&Entry{}).Select("seq").Order("seq desc").Limit(1).Pluck("seq", &lastKey).Error
			if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("failed to query last existing key: %w", err)
			}

			var entries []Entry
			err = s.db.Model(&Entry{}).
				Where("src = ? and val = ? and uri = ? and cid = ?",
					newLabel.Src, newLabel.Val, newLabel.Uri, newLabel.Cid).
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

			newLabel.Seq = lastKey + 1
			return tx.Create(&newLabel).Error
		})
		if err != nil {
			continue
		}
		if updated {
			highestKey.WithLabelValues(s.did).Set(float64(lastKey + 1))
		}
		return updated, nil
	}
	return false, fmt.Errorf("failed to write the new label")
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
