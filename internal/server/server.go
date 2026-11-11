// Package server is the gateway's HTTP front door.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/Ani-tem/llm-gateway/internal/chat"
	"github.com/Ani-tem/llm-gateway/internal/provider"
	"github.com/Ani-tem/llm-gateway/internal/ratelimit"
	"github.com/Ani-tem/llm-gateway/internal/route"
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
}

type Server struct {
	router  *route.Router
	tenants map[string]Tenant
	limiter *ratelimit.Limiter
}

// New returns the gateway's handler.
func New(cfg Config) http.Handler {
	s := &Server{router: cfg.Router, tenants: cfg.Tenants, limiter: cfg.Limiter}
	mux := http.NewServeMux()
	mux.Handle("POST /v1/chat/completions", s.admit(http.HandlerFunc(s.handleChat)))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		io.WriteString(w, "ok\n")
	})
	return logRequests(mux)
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
	out, served, err := s.router.Chat(r.Context(), req, targets)
	if err != nil {
		writeUpstreamError(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set(upstreamHeader, served.String())
	w.Write(out)
}

func (s *Server) stream(w http.ResponseWriter, r *http.Request, req *chat.Request, targets []route.Target) {
	// Open the upstream stream before writing anything, so a failure here can
	// still fail over to another target or be reported with a proper status.
	st, served, err := s.router.ChatStream(r.Context(), req, targets)
	if err != nil {
		writeUpstreamError(w, r, err)
		return
	}
	defer st.Close()

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

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(rec, r)
		slog.Info("request", "method", r.Method, "path", r.URL.Path, "status", rec.status, "duration", time.Since(start))
	})
}
