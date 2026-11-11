package route

import (
	"sync"
	"time"
)

// Breaker is a circuit breaker for one provider. After Threshold consecutive
// failures it "opens" and rejects calls for Cooldown, so a dead provider costs
// nothing instead of a timeout per request. After the cooldown it lets a
// single probe through ("half-open"): success closes it, failure re-opens it.
//
// The state is per gateway instance, not shared in Redis. Each instance
// learns within a few requests on its own, and keeping it local means the
// hot path takes a mutex rather than a network round trip.
type Breaker struct {
	Threshold int
	Cooldown  time.Duration

	mu        sync.Mutex
	failures  int       // consecutive failures
	openUntil time.Time // calls are rejected before this instant
	now       func() time.Time
}

func NewBreaker(threshold int, cooldown time.Duration) *Breaker {
	return &Breaker{Threshold: threshold, Cooldown: cooldown, now: time.Now}
}

// Allow reports whether a call may go to the provider.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failures < b.Threshold {
		return true // closed
	}
	now := b.now()
	if now.Before(b.openUntil) {
		return false // open
	}
	// Half-open: this caller is the probe. Pushing openUntil forward keeps
	// everyone else out until the probe reports back, and means a probe that
	// never reports (client hung up) cannot wedge the breaker.
	b.openUntil = now.Add(b.Cooldown)
	return true
}

// Record reports the outcome of a call that Allow let through.
func (b *Breaker) Record(ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if ok {
		b.failures = 0
		return
	}
	b.failures++
	if b.failures >= b.Threshold {
		b.openUntil = b.now().Add(b.Cooldown)
	}
}
