package server

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/Ani-tem/llm-gateway/internal/cache"
	"github.com/Ani-tem/llm-gateway/internal/chat"
	"github.com/Ani-tem/llm-gateway/internal/route"
)

// cacheHeader reports "hit" or "miss" for requests that went through the
// cache. It is absent when the cache was not used.
const cacheHeader = "X-Gateway-Cache"

// How long an upstream call made on behalf of the cache may run. It needs its
// own budget because it is deliberately detached from any one client.
const sharedCallBudget = 2 * time.Minute

type chatResult struct {
	body   []byte
	served route.Target
}

// cacheKey returns the key for a request, or "" if it must not be cached.
//
// Streamed requests are not cached: replaying one faithfully means storing
// and re-chunking the stream, which is not worth it yet. Clients can opt out
// per request with the standard Cache-Control header.
func (s *Server) cacheKey(r *http.Request, req *chat.Request) string {
	if s.cache == nil || req.Stream {
		return ""
	}
	if cc := r.Header.Get("Cache-Control"); strings.Contains(cc, "no-cache") || strings.Contains(cc, "no-store") {
		return ""
	}
	// BodyForModel re-marshals with sorted top-level keys, so two requests
	// that differ only in field order share an entry.
	body, err := req.BodyForModel(req.Model)
	if err != nil {
		return ""
	}
	return cache.Key(tenantFrom(r.Context()).Name, body)
}

// cachedChat answers from the cache or, on a miss, from upstream.
func (s *Server) cachedChat(ctx context.Context, key string, req *chat.Request, targets []route.Target) (res chatResult, hit bool, err error) {
	getCtx, cancel := context.WithTimeout(ctx, redisTimeout)
	body, ok, err := s.cache.Get(getCtx, key)
	cancel()
	if err != nil {
		// Like the rate limiter, a broken cache degrades to "no cache"
		// rather than failing the request.
		slog.Warn("cache read failed, treating as a miss", "err", err)
	}
	if ok {
		return chatResult{body: body}, true, nil
	}

	// singleflight collapses concurrent identical misses into one upstream
	// call. Without it, N clients asking the same new question at once would
	// all miss and all call the provider (a "cache stampede").
	ch := s.flight.DoChan(key, func() (any, error) {
		// The shared call must not die because the first caller hung up:
		// other callers are waiting on it and the result is worth caching
		// anyway. So it drops that caller's cancellation and gets its own
		// deadline instead.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sharedCallBudget)
		defer cancel()
		body, served, err := s.router.Chat(ctx, req, targets)
		if err != nil {
			return nil, err // failures are never cached
		}
		setCtx, cancel := context.WithTimeout(ctx, redisTimeout)
		defer cancel()
		if err := s.cache.Set(setCtx, key, body); err != nil {
			slog.Warn("cache write failed", "err", err)
		}
		return chatResult{body: body, served: served}, nil
	})

	// Each caller still stops waiting the moment its own client leaves.
	select {
	case r := <-ch:
		if r.Err != nil {
			return chatResult{}, false, r.Err
		}
		return r.Val.(chatResult), false, nil
	case <-ctx.Done():
		return chatResult{}, false, ctx.Err()
	}
}
