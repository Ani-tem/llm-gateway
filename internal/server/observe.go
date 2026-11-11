package server

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/Ani-tem/llm-gateway/internal/chat"
	"github.com/Ani-tem/llm-gateway/internal/metrics"
	"github.com/Ani-tem/llm-gateway/internal/usage"
)

// requestInfo collects facts about one request as it moves through the
// handlers, so that a single place (observe) can log and meter it at the end.
// It lives in the request context as a pointer; only the request's own
// goroutine touches it, so it needs no lock.
type requestInfo struct {
	tenant    Tenant
	metered   bool // counts toward usage (chat requests, not /v1/usage itself)
	model     string
	upstream  string
	stream    bool
	cached    bool
	cacheMiss bool
	usage     chat.Usage
}

type infoKey struct{}

// info returns the request's info record. observe wraps every route, so it is
// always present.
func info(ctx context.Context) *requestInfo {
	return ctx.Value(infoKey{}).(*requestInfo)
}

// observe is the outermost middleware: it writes the access log and the usage
// event once the request has finished.
func (s *Server) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ri := &requestInfo{}
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(rec, r.WithContext(context.WithValue(r.Context(), infoKey{}, ri)))
		elapsed := time.Since(start)

		slog.Info("request", "method", r.Method, "path", r.URL.Path, "status", rec.status,
			"duration", elapsed, "tenant", ri.tenant.Name, "model", ri.model,
			"upstream", ri.upstream, "cached", ri.cached)

		if ri.metered {
			cache := "none"
			switch {
			case ri.cached:
				cache = "hit"
			case ri.cacheMiss:
				cache = "miss"
			}
			metrics.Requests.WithLabelValues(ri.tenant.Name, strconv.Itoa(rec.status), cache).Inc()
			metrics.RequestDuration.WithLabelValues(strconv.FormatBool(ri.stream), cache).Observe(elapsed.Seconds())
			if !ri.cached {
				metrics.Tokens.WithLabelValues("prompt").Add(float64(ri.usage.PromptTokens))
				metrics.Tokens.WithLabelValues("completion").Add(float64(ri.usage.CompletionTokens))
			}
		}

		if s.usage != nil && ri.metered && ri.tenant.Name != "" {
			s.usage.Record(usage.Event{
				At:               start,
				Tenant:           ri.tenant.Name,
				Model:            ri.model,
				Upstream:         ri.upstream,
				Status:           rec.status,
				Cached:           ri.cached,
				Stream:           ri.stream,
				PromptTokens:     ri.usage.PromptTokens,
				CompletionTokens: ri.usage.CompletionTokens,
				Latency:          elapsed,
			})
		}
	})
}

// metered marks a route's requests as counting toward usage. It sits outside
// admit so that rejected requests (rate limited) are recorded too.
func metered(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		info(r.Context()).metered = true
		next.ServeHTTP(w, r)
	})
}

// statusRecorder remembers the status code for the access log.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// Unwrap lets http.ResponseController reach the real writer's Flush. Without
// it, wrapping the writer would silently break streaming.
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }
