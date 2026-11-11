// Package server is the gateway's HTTP front door.
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"golang.org/x/sync/singleflight"

	"github.com/Ani-tem/llm-gateway/internal/cache"
	"github.com/Ani-tem/llm-gateway/internal/chat"
	"github.com/Ani-tem/llm-gateway/internal/provider"
	"github.com/Ani-tem/llm-gateway/internal/ratelimit"
	"github.com/Ani-tem/llm-gateway/internal/route"
	"github.com/Ani-tem/llm-gateway/internal/usage"
)

const maxBodyBytes = 4 << 20 // 4 MB

// upstreamHeader tells the client which target actually served the request,
// which makes failover visible.
const upstreamHeader = "X-Gateway-Upstream"

type Config struct {
	Router *route.Router
	// Tenants is keyed by HashKey(apiKey).
	Tenants map[string]Tenant
	Limiter *ratelimit.Limiter
	// Cache is optional; nil disables response caching.
	Cache *cache.Cache
	// Usage and UsageReader are optional; nil disables metering.
	Usage       *usage.Recorder
	UsageReader UsageReader
}

type Server struct {
	router  *route.Router
	tenants map[string]Tenant
	limiter *ratelimit.Limiter
	cache   *cache.Cache
	flight  singleflight.Group

	usage       *usage.Recorder
	usageReader UsageReader
}

// New returns the gateway's handler.
func New(cfg Config) http.Handler {
	s := &Server{
		router: cfg.Router, tenants: cfg.Tenants, limiter: cfg.Limiter, cache: cfg.Cache,
		usage: cfg.Usage, usageReader: cfg.UsageReader,
	}
	mux := http.NewServeMux()
	mux.Handle("POST /v1/chat/completions", metered(s.admit(http.HandlerFunc(s.handleChat))))
	mux.Handle("GET /v1/usage", s.admit(http.HandlerFunc(s.handleUsage)))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		io.WriteString(w, "ok\n")
	})
	return s.observe(mux)
}

func (s *Server) handleChat(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, "invalid_request_error", "request body too large")
			return
		}
		writeError(w, http.StatusBadRequest, "invalid_request_error", "could not read request body")
		return
	}
	req, err := chat.ParseRequest(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}

	ri := info(r.Context())
	ri.model, ri.stream = req.Model, req.Stream

	targets := s.router.Resolve(req.Model)
	if targets == nil {
		writeError(w, http.StatusNotFound, "invalid_request_error",
			fmt.Sprintf("unknown model %q: use a route name or \"provider/model\" with a configured provider", req.Model))
		return
	}

	if req.Stream {
		s.stream(w, r, req, targets)
		return
	}
	if key := s.cacheKey(r, req); key != "" {
		res, hit, err := s.cachedChat(r.Context(), key, req, targets)
		if err != nil {
			writeUpstreamError(w, r, err)
			return
		}
		// A hit still records the tokens the answer contains, flagged as
		// cached, so usage reports can show what the cache saved.
		ri.usage, _, _ = chat.UsageOf(res.body)
		w.Header().Set("Content-Type", "application/json")
		if hit {
			ri.cached = true
			w.Header().Set(cacheHeader, "hit")
		} else {
			ri.cacheMiss = true
			ri.upstream = res.served.String()
			w.Header().Set(cacheHeader, "miss")
			w.Header().Set(upstreamHeader, ri.upstream)
		}
		w.Write(res.body)
		return
	}
	out, served, err := s.router.Chat(r.Context(), req, targets)
	if err != nil {
		writeUpstreamError(w, r, err)
		return
	}
	ri.upstream = served.String()
	ri.usage, _, _ = chat.UsageOf(out)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set(upstreamHeader, ri.upstream)
	w.Write(out)
}

func (s *Server) stream(w http.ResponseWriter, r *http.Request, req *chat.Request, targets []route.Target) {
	// Open the upstream stream before writing anything, so a failure here can
	// still fail over to another target or be reported with a proper status.
	// Providers report token counts for a stream only when asked, so the
	// gateway always asks. If the client did not, the extra usage chunk is
	// read for metering and then withheld, leaving the stream exactly as the
	// client expects it.
	clientAskedForUsage := req.EnsureStreamUsage()

	st, served, err := s.router.ChatStream(r.Context(), req, targets)
	if err != nil {
		writeUpstreamError(w, r, err)
		return
	}
	defer st.Close()
	ri := info(r.Context())
	ri.upstream = served.String()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set(upstreamHeader, served.String())
	rc := http.NewResponseController(w)

	for {
		data, err := st.Recv()
		if errors.Is(err, io.EOF) {
			io.WriteString(w, "data: [DONE]\n\n")
			rc.Flush()
			return
		}
		if err != nil {
			// The 200 is already sent, so the only way left to report a
			// failure is in-band. If the client itself left, say nothing.
			if r.Context().Err() == nil {
				slog.Error("stream failed", "model", req.Model, "err", err)
				fmt.Fprintf(w, "data: %s\n\n", errorJSON("upstream_error", "stream interrupted"))
				rc.Flush()
			}
			return
		}
		// Cheap substring check first, so the JSON of ordinary chunks is
		// never parsed.
		if bytes.Contains(data, []byte(`"usage"`)) {
			if u, usageOnly, ok := chat.UsageOf(data); ok {
				ri.usage = u
				if usageOnly && !clientAskedForUsage {
					continue
				}
			}
		}
		// Flush every chunk: without it net/http buffers and the client sees
		// the whole answer at once instead of token by token.
		if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
			return
		}
		if err := rc.Flush(); err != nil {
			return
		}
	}
}

// writeUpstreamError maps a provider failure to the status the client sees.
func writeUpstreamError(w http.ResponseWriter, r *http.Request, err error) {
	if r.Context().Err() != nil {
		return // client disconnected; nobody is listening
	}
	slog.Error("upstream failed", "err", err)

	var up *provider.UpstreamError
	switch {
	case errors.Is(err, route.ErrNoHealthyUpstream):
		writeError(w, http.StatusServiceUnavailable, "upstream_error", "all upstreams are temporarily unavailable")
	case errors.As(err, &up):
		switch up.Status {
		// These are the client's problem (bad request, unknown model, too
		// fast), so pass them through. Anything else, including a 401 for
		// the gateway's own key, is our side's fault: 502.
		case http.StatusBadRequest, http.StatusNotFound, http.StatusUnprocessableEntity, http.StatusTooManyRequests:
			writeError(w, up.Status, "upstream_error", err.Error())
		default:
			writeError(w, http.StatusBadGateway, "upstream_error", fmt.Sprintf("%s returned %d", up.Provider, up.Status))
		}
	case errors.Is(err, context.DeadlineExceeded):
		writeError(w, http.StatusGatewayTimeout, "upstream_error", "upstream timed out")
	default:
		writeError(w, http.StatusBadGateway, "upstream_error", "upstream unreachable")
	}
}

func writeError(w http.ResponseWriter, status int, kind, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(errorJSON(kind, msg))
}

func errorJSON(kind, msg string) []byte {
	b, _ := json.Marshal(chat.ErrorBody{Error: chat.ErrorDetail{Message: msg, Type: kind}})
	return b
}
