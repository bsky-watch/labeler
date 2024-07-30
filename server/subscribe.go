package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
	bolt "go.etcd.io/bbolt"

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

		log.Debug().Msgf("Subscription request from %q", r.RemoteAddr)

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
		s.streamLabels(log.WithContext(ctx), c, cursor)
		log.Debug().Msgf("Connection closed")
	})
}

func (s *Server) streamLabels(ctx context.Context, conn *websocket.Conn, cursor int64) {
	log := zerolog.Ctx(ctx)

	conn.EnableWriteCompression(true)

	wakeCh := make(chan struct{}, 1)

	s.mu.Lock()
	s.wakeChans = append(s.wakeChans, wakeCh)
	activeSubscriptions.WithLabelValues(s.did).Set(float64(len(s.wakeChans)))
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.wakeChans = slices.DeleteFunc(s.wakeChans, func(ch chan struct{}) bool { return ch == wakeCh })
		activeSubscriptions.WithLabelValues(s.did).Set(float64(len(s.wakeChans)))
		s.mu.Unlock()
	}()

	var lastKey []byte
	if cursor >= 0 {
		futureCursor := false
		err := s.db.View(func(tx *bolt.Tx) error {
			c := tx.Bucket([]byte(bucketName)).Cursor()
			key, value := c.Seek(encodeKey(cursor))
			if key == nil {
				futureCursor = true
				return nil
			}
			if !bytes.Equal(key, encodeKey(cursor)) && len(value) > 0 {
				if err := s.sendLabel(ctx, conn, key, value); err != nil {
					return err
				}
			}
			lastKey = slices.Clone(key)
			for key, value = c.Next(); key != nil; key, value = c.Next() {
				if len(value) == 0 {
					continue
				}
				if err := s.sendLabel(ctx, conn, key, value); err != nil {
					return err
				}
				lastKey = slices.Clone(key)
			}
			return nil
		})
		if err != nil {
			return
		}
		if futureCursor {
			err := conn.WriteMessage(websocket.BinaryMessage, []byte("\xa1bop \xa1eerrorlFutureCursor"))
			if err != nil {
				log.Warn().Err(err).Msgf("Failed to send FutureCursor error to the client: %s", err)
			}
			err = conn.Close()
			if err != nil {
				log.Warn().Err(err).Msgf("conn.Close() returned an error: %s", err)
			}
			return
		}
	} else {
		err := s.db.View(func(tx *bolt.Tx) error {
			c := tx.Bucket([]byte(bucketName)).Cursor()
			lastKey, _ = c.Last()
			lastKey = slices.Clone(lastKey)
			return nil
		})
		if err != nil {
			log.Error().Err(err).Msgf("Failed to get the last key from the database: %s", err)
			conn.Close()
			return
		}
	}
	err := conn.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(5*time.Second))
	if err != nil {
		log.Error().Err(err).Msgf("Ping failed: %s", err)
		conn.Close()
		return
	}

	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := conn.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(5*time.Second))
			if err != nil {
				log.Error().Err(err).Msgf("Ping failed: %s", err)
				conn.Close()
				return
			}
		case <-wakeCh:
			log.Trace().Msgf("Waking up")
			err := s.db.View(func(tx *bolt.Tx) error {
				c := tx.Bucket([]byte(bucketName)).Cursor()
				var key, value []byte
				if lastKey != nil {
					key, value = c.Seek(lastKey)
					if bytes.Equal(key, lastKey) {
						// We've already sent this label, so advance to the next one.
						key, value = c.Next()
					}
				} else {
					key, value = c.First()
				}
				for ; key != nil; key, value = c.Next() {
					if err := s.sendLabel(ctx, conn, key, value); err != nil {
						return err
					}
					lastKey = slices.Clone(key)
				}
				return nil
			})
			if err != nil {
				conn.Close()
				return
			}
		}
	}
}

func (s *Server) sendLabel(ctx context.Context, conn *websocket.Conn, key []byte, value []byte) error {
	label := &comatproto.LabelDefs_Label{}
	if err := json.Unmarshal(value, label); err != nil {
		return err
	}
	if err := sign.Sign(ctx, s.privateKey, label); err != nil {
		return err
	}

	seq, err := decodeKey(key)
	if err != nil {
		return err
	}
	msg := &comatproto.LabelSubscribeLabels_Labels{
		Seq:    seq,
		Labels: []*comatproto.LabelDefs_Label{label},
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
