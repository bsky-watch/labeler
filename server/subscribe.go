package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
	"gorm.io/gorm"

	comatproto "github.com/bluesky-social/indigo/api/atproto"

	"bsky.watch/labeler/sign"
)

// Subscribe returns HTTP handler that implements [com.atproto.label.subscribeLabels](https://github.com/bluesky-social/atproto/blob/main/lexicons/com/atproto/label/subscribeLabels.json) XRPC method.
func (s *Server) Subscribe() http.Handler {
	upgrader := &websocket.Upgrader{
		EnableCompression: true,
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		log := zerolog.Ctx(ctx).With().Str("remote", r.RemoteAddr).Logger()

		remote := r.RemoteAddr
		if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
			remote = fmt.Sprintf("%s (via %s)", forwarded, r.RemoteAddr)
			log = log.With().Str("forwarded_for", forwarded).Logger()
		}

		log.Debug().Msgf("Subscription request from %q", remote)

		cursor := int64(-1)
		if s := r.FormValue("cursor"); s != "" {
			n, err := strconv.ParseUint(s, 10, 64)
			if err != nil {
				http.Error(w, "bad cursor", http.StatusBadRequest)
				log.Debug().Msgf("Bad cursor value: %q", s)
				return
			}
			cursor = int64(n)
		}

		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			http.Error(w, fmt.Sprintf("connection upgrade failed: %s", err), http.StatusBadRequest)
			log.Debug().Err(err).Msgf("Connection upgrade failed: %s", err)
			return
		}
		remote = strings.SplitN(remote, ",", 2)[0]
		s.streamLabels(log.WithContext(ctx), c, cursor, remote)
		log.Debug().Msgf("Connection closed")
	})
}

func (s *Server) streamLabels(ctx context.Context, conn *websocket.Conn, cursor int64, remoteAddr string) {
	log := zerolog.Ctx(ctx)

	conn.EnableWriteCompression(true)
	defer conn.Close()

	wakeCh := make(chan struct{}, 1)

	s.mu.Lock()
	s.wakeChans = append(s.wakeChans, wakeCh)
	activeSubscriptions.WithLabelValues(s.did).Set(float64(len(s.wakeChans)))
	s.mu.Unlock()
	defer func() {
		subscriberCursor.DeleteLabelValues(s.did, remoteAddr)
		s.mu.Lock()
		s.wakeChans = slices.DeleteFunc(s.wakeChans, func(ch chan struct{}) bool { return ch == wakeCh })
		activeSubscriptions.WithLabelValues(s.did).Set(float64(len(s.wakeChans)))
		s.mu.Unlock()
	}()

	var lastKey int64
	if cursor >= 0 {
		futureCursor := false
		if empty, err := s.IsEmpty(); err != nil {
			log.Error().Err(err).Msgf("Failed to check if DB is empty: %s", err)
			return
		} else if !empty {
			var labelCount int64
			err := s.db.Model(&Entry{}).Where("seq >= ?", cursor).Count(&labelCount).Error
			if err != nil {
				log.Error().Err(err).Msgf("Failed to check if the cursor is valid: %s", err)
				return
			}
			futureCursor = labelCount == 0
		} else if empty {
			futureCursor = cursor > 0
		}

		if futureCursor {
			err := conn.WriteMessage(websocket.BinaryMessage, []byte("\xa1bop \xa1eerrorlFutureCursor"))
			if err != nil {
				log.Warn().Err(err).Msgf("Failed to send FutureCursor error to the client: %s", err)
			}
			return
		}

		subscriberCursor.WithLabelValues(s.did, remoteAddr).Set(float64(cursor))

		var entries []Entry
		err := s.db.Model(&entries).Where("seq > ?", cursor).Order("seq asc").FindInBatches(&entries, 100, func(tx *gorm.DB, batch int) error {
			for _, e := range entries {
				if err := s.sendLabel(ctx, conn, e.Seq, e.ToLabel()); err != nil {
					return err
				}
				lastKey = e.Seq
				subscriberCursor.WithLabelValues(s.did, remoteAddr).Set(float64(lastKey))
			}
			return nil
		}).Error
		if err != nil {
			return
		}
	} else {
		err := s.db.Model(&Entry{}).Select("seq").Order("seq desc").Limit(1).Pluck("seq", &lastKey).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				lastKey = 0
			} else {
				log.Error().Err(err).Msgf("Failed to query last existing key: %s", err)
				return
			}
		}
		subscriberCursor.WithLabelValues(s.did, remoteAddr).Set(float64(lastKey))
	}
	err := conn.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(5*time.Second))
	if err != nil {
		log.Error().Err(err).Msgf("Ping failed: %s", err)
		return
	}

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := conn.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(5*time.Second))
			if err != nil {
				log.Error().Err(err).Msgf("Ping failed: %s", err)
				return
			}
		case <-wakeCh:
			log.Trace().Msgf("Waking up")
			var entries []Entry
			err := s.db.Model(&entries).Where("seq > ?", cursor).Order("seq asc").FindInBatches(&entries, 100, func(tx *gorm.DB, batch int) error {
				for _, e := range entries {
					if err := s.sendLabel(ctx, conn, e.Seq, e.ToLabel()); err != nil {
						return err
					}
					lastKey = e.Seq
					subscriberCursor.WithLabelValues(s.did, remoteAddr).Set(float64(lastKey))
				}
				return nil
			}).Error
			if err != nil {
				return
			}
		}
	}
}

func (s *Server) sendLabel(ctx context.Context, conn *websocket.Conn, seq int64, label comatproto.LabelDefs_Label) error {
	if err := sign.Sign(ctx, s.privateKey, &label); err != nil {
		return err
	}

	msg := &comatproto.LabelSubscribeLabels_Labels{
		Seq:    seq,
		Labels: []*comatproto.LabelDefs_Label{&label},
	}

	w, err := conn.NextWriter(websocket.BinaryMessage)
	if err != nil {
		return err
	}

	// Header: {op:1, t:"#labels"}
	_, err = w.Write([]byte("\xa2atg#labelsbop\x01"))
	if err != nil {
		return err
	}

	if err := msg.MarshalCBOR(w); err != nil {
		return err
	}
	return w.Close()
}
