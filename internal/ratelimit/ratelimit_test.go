package ratelimit_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/Ani-tem/llm-gateway/internal/ratelimit"
)

// miniredis is an in-process Redis that runs the real Lua script, so these
// tests need no Docker and can move the clock by hand.
func setup(t *testing.T) (*ratelimit.Limiter, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	return ratelimit.New(rdb), mr
}

func allow(t *testing.T, l *ratelimit.Limiter, key string, limit ratelimit.Limit) ratelimit.Result {
	t.Helper()
	res, err := l.Allow(context.Background(), key, limit)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func TestBurstThenDeny(t *testing.T) {
	l, mr := setup(t)
	mr.SetTime(time.Unix(1_000_000, 0))
	limit := ratelimit.Limit{PerMinute: 60, Burst: 3}

	for i, wantRemaining := range []int{2, 1, 0} {
		res := allow(t, l, "a", limit)
		if !res.Allowed || res.Remaining != wantRemaining {
			t.Fatalf("request %d: %+v, want allowed with %d remaining", i+1, res, wantRemaining)
		}
	}
	res := allow(t, l, "a", limit)
	if res.Allowed {
		t.Fatal("4th request allowed, want denied")
	}
	// 60/min is one token per second, and the bucket is exactly empty.
	if res.RetryAfter != time.Second {
		t.Errorf("RetryAfter = %v, want 1s", res.RetryAfter)
	}
}

func TestRefillOverTime(t *testing.T) {
	l, mr := setup(t)
	start := time.Unix(1_000_000, 0)
	mr.SetTime(start)
	limit := ratelimit.Limit{PerMinute: 60, Burst: 2}

	allow(t, l, "a", limit)
	allow(t, l, "a", limit)
	if allow(t, l, "a", limit).Allowed {
		t.Fatal("bucket should be empty")
	}

	mr.SetTime(start.Add(500 * time.Millisecond))
	if res := allow(t, l, "a", limit); res.Allowed || res.RetryAfter != 500*time.Millisecond {
		t.Fatalf("after 0.5s: %+v, want denied with 500ms to wait", res)
	}

	mr.SetTime(start.Add(time.Second))
	if !allow(t, l, "a", limit).Allowed {
		t.Fatal("after 1s one token should be back")
	}

	// A long idle period refills to Burst, never beyond it.
	mr.SetTime(start.Add(time.Hour))
	if res := allow(t, l, "a", limit); res.Remaining != 1 {
		t.Fatalf("after idle: remaining = %d, want 1 (burst 2 minus this request)", res.Remaining)
	}
}

func TestKeysAreIndependent(t *testing.T) {
	l, _ := setup(t)
	limit := ratelimit.Limit{PerMinute: 60, Burst: 1}
	allow(t, l, "a", limit)
	if allow(t, l, "a", limit).Allowed {
		t.Fatal("a should be exhausted")
	}
	if !allow(t, l, "b", limit).Allowed {
		t.Fatal("b has its own bucket and should be allowed")
	}
}

func TestRedisDownReturnsError(t *testing.T) {
	l, mr := setup(t)
	mr.Close()
	if _, err := l.Allow(context.Background(), "a", ratelimit.Limit{PerMinute: 60, Burst: 1}); err == nil {
		t.Fatal("want an error when Redis is unreachable")
	}
}
