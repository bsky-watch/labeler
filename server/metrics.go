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
)
