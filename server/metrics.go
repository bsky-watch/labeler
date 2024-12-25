package server

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	activeSubscriptions = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "labeler",
		Subsystem: "server",
		Name:      "active_subscriptions_count",
		Help:      "Current number of active com.atproto.label.subscribeLabels connections.",
	}, []string{"did"})
	writeLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "labeler",
		Subsystem: "server",
		Name:      "write_duration_seconds",
		Help:      "Latency of writing new labels into the database.",
		Buckets:   prometheus.ExponentialBucketsRange(0.1, 30000, 20),
	}, []string{"did", "status"})
)
