// Package route decides which provider serves a request and what happens when
// one fails: retry, fall over to the next target, or give up.
package route

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/Ani-tem/llm-gateway/internal/chat"
	"github.com/Ani-tem/llm-gateway/internal/provider"
)

// ErrNoHealthyUpstream means every target's circuit breaker was open.
var ErrNoHealthyUpstream = errors.New("no healthy upstream available")

// Target is one provider and the model name to ask it for.
type Target struct {
	Provider string
	Model    string
}

func (t Target) String() string { return t.Provider + "/" + t.Model }

type Router struct {
	// MaxAttempts is how many times one target is tried before moving on.
	MaxAttempts int
	// Backoff is the wait before the first retry; it doubles each retry.
	Backoff time.Duration

	providers map[string]provider.Provider
	routes    map[string][]Target
	breakers  map[string]*Breaker
}

// New builds a router. routes maps an alias (e.g. "auto") to targets tried in
// order; "provider/model" always works as a single-target route.
func New(providers map[string]provider.Provider, routes map[string][]Target) *Router {
	r := &Router{
		MaxAttempts: 2,
		Backoff:     200 * time.Millisecond,
		providers:   providers,
		routes:      routes,
		breakers:    make(map[string]*Breaker, len(providers)),
	}
	for name := range providers {
		r.breakers[name] = NewBreaker(5, 30*time.Second)
	}
	return r
}

// Resolve returns the targets for a requested model, or nil if it names
// neither a route nor a configured provider.
func (r *Router) Resolve(model string) []Target {
	if targets, ok := r.routes[model]; ok {
		return targets
	}
	// Cut on the first slash only: upstream model names can contain slashes
	// themselves (OpenRouter's "meta-llama/llama-3.3-70b-instruct").
	name, upstream, _ := strings.Cut(model, "/")
	if _, ok := r.providers[name]; !ok || upstream == "" {
		return nil
	}
	return []Target{{Provider: name, Model: upstream}}
}

// Chat returns the first successful response and the target that produced it.
func (r *Router) Chat(ctx context.Context, req *chat.Request, targets []Target) ([]byte, Target, error) {
	return try(ctx, r, targets, func(p provider.Provider, model string) ([]byte, error) {
		return p.Chat(ctx, req, model)
	})
}

// ChatStream fails over only until a stream opens. Once the first byte has
// gone to the client there is no clean way to switch providers mid-answer.
func (r *Router) ChatStream(ctx context.Context, req *chat.Request, targets []Target) (provider.Stream, Target, error) {
	return try(ctx, r, targets, func(p provider.Provider, model string) (provider.Stream, error) {
		return p.ChatStream(ctx, req, model)
	})
}

// try walks the targets in order. It is generic so Chat and ChatStream share
// one copy of the retry/failover/breaker logic.
func try[T any](ctx context.Context, r *Router, targets []Target, call func(provider.Provider, string) (T, error)) (T, Target, error) {
	var zero T
	lastErr := ErrNoHealthyUpstream
	for _, t := range targets {
		breaker := r.breakers[t.Provider]
		for attempt := 1; attempt <= r.MaxAttempts; attempt++ {
			if !breaker.Allow() {
				break // open: skip this provider without spending any time on it
			}
			out, err := call(r.providers[t.Provider], t.Model)
			if err == nil {
				breaker.Record(true)
				return out, t, nil
			}
			if ctx.Err() != nil {
				// The caller gave up. That says nothing about the provider's
				// health, so the breaker is not told.
				return zero, t, err
			}
			v := classify(err)
			breaker.Record(!v.unhealthy)
			if v.fatal {
				return zero, t, err
			}
			lastErr = err
			slog.Warn("upstream attempt failed", "target", t.String(), "attempt", attempt, "err", err)
			if !v.retry || attempt == r.MaxAttempts {
				break
			}
			if err := sleep(ctx, jittered(r.Backoff<<(attempt-1))); err != nil {
				return zero, t, err
			}
		}
	}
	return zero, Target{}, lastErr
}

type verdict struct {
	fatal     bool // stop: no provider can serve this request
	retry     bool // worth trying the same provider again
	unhealthy bool // counts toward opening the provider's breaker
}

func classify(err error) verdict {
	var up *provider.UpstreamError
	if !errors.As(err, &up) {
		// Connection refused, reset, header timeout: often transient.
		return verdict{retry: true, unhealthy: true}
	}
	switch s := up.Status; {
	case s == 429:
		// Alive but rate limiting us. Retrying immediately would only make
		// it worse, so move straight to the next target.
		return verdict{}
	case s == 401 || s == 403:
		// Our key for this provider is bad; a retry cannot fix that.
		return verdict{unhealthy: true}
	case s == 408 || s >= 500:
		return verdict{retry: true, unhealthy: true}
	default:
		// 400, 404, 422...: the request itself is wrong, and the next
		// provider would reject it too.
		return verdict{fatal: true}
	}
}

// jittered picks a random wait in [d/2, d). Without jitter, clients that
// failed together retry together and hit the recovering provider in waves.
func jittered(d time.Duration) time.Duration {
	if d <= 1 {
		return d
	}
	return d/2 + rand.N(d/2)
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
