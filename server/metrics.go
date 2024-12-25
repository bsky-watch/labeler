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

	// Not happy about using an IP addr as a label value, but not
	// sure there's any other useful option.
	subscriberCursor = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "labeler",
		Subsystem: "server",
		Name:      "subscriber_cursor",
		Help:      "Last cursor value sent to a subscriber.",
	}, []string{"did", "remote"})

	highestKey = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "labeler",
		Subsystem: "server",
		Name:      "highest_cursor_value",
		Help:      "Cursor value of the last created label.",
	}, []string{"did"})
)
