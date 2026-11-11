// Package metrics defines the gateway's Prometheus metrics.
//
// Label values are kept to small, known sets (configured tenants, configured
// providers, status codes). Anything a client controls, such as the model
// name, is deliberately not a label: every distinct label value is a separate
// time series, so a client sending random model names could exhaust
// Prometheus's memory.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Wider than the default buckets at both ends: cache hits take about a
// millisecond and long completions take a minute.
var latencyBuckets = []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60}

var (
	// Requests counts chat requests by outcome. cache is "hit", "miss" or
	// "none" (not cacheable or never reached the cache).
	Requests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_requests_total",
		Help: "Chat requests handled, by tenant, HTTP status and cache result.",
	}, []string{"tenant", "status", "cache"})

	// RequestDuration is end-to-end time in the gateway. For streams it runs
	// until the last chunk, so the two are kept apart by the stream label.
	RequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gateway_request_duration_seconds",
		Help:    "Time to serve a chat request, by whether it was streamed and cache result.",
		Buckets: latencyBuckets,
	}, []string{"stream", "cache"})

	// UpstreamAttempts counts individual calls to providers, including
	// retries. outcome is ok, failed, rate_limited, client_error or cancelled.
	UpstreamAttempts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_upstream_attempts_total",
		Help: "Calls made to upstream providers, by provider and outcome.",
	}, []string{"provider", "outcome"})

	UpstreamDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gateway_upstream_attempt_duration_seconds",
		Help:    "Time for one upstream call to return (for streams, to open).",
		Buckets: latencyBuckets,
	}, []string{"provider"})

	BreakerOpen = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gateway_breaker_open",
		Help: "1 while a provider's circuit breaker is open, else 0.",
	}, []string{"provider"})

	BreakerSkips = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_breaker_skipped_total",
		Help: "Calls not made because the provider's circuit breaker was open.",
	}, []string{"provider"})

	RateLimiterErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gateway_rate_limiter_errors_total",
		Help: "Requests allowed through because the rate limiter (Redis) was unavailable.",
	})

	// Tokens counts tokens actually bought from providers; cache hits are
	// excluded. type is "prompt" or "completion".
	Tokens = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_tokens_total",
		Help: "Tokens consumed from upstream providers, by type.",
	}, []string{"type"})
)
