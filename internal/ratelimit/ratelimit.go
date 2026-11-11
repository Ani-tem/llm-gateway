// Package ratelimit is a token-bucket rate limiter backed by Redis, so every
// gateway instance shares the same buckets.
package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Limit describes one bucket: it holds up to Burst tokens and refills at
// PerMinute tokens per minute. Each request spends one token.
//
// Token bucket (rather than a fixed window counter) because it allows short
// bursts while still enforcing the average rate, and has no "double the limit
// across a window boundary" problem.
type Limit struct {
	PerMinute int
	Burst     int
}

type Result struct {
	Allowed    bool
	Remaining  int           // whole tokens left after this request
	RetryAfter time.Duration // when denied: how long until one token is back
}

// The read-refill-spend-write sequence runs as one Lua script because Redis
// executes a script atomically. Doing it as separate commands from Go would
// let two concurrent requests both read "1 token left" and both spend it.
//
// The clock is Redis's own (TIME), not the gateway's, so instances with
// skewed clocks still agree on how much to refill.
//
// A missing key means a full bucket, so the key can expire once it would have
// refilled completely; idle tenants cost no memory.
var bucketScript = redis.NewScript(`
local rpm = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])

local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)

local b = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(b[1]) or burst
local ts = tonumber(b[2]) or now
tokens = math.min(burst, tokens + math.max(0, now - ts) * rpm / 60000)

local allowed, retry_ms = 0, 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  retry_ms = math.ceil((1 - tokens) * 60000 / rpm)
end

redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', now)
redis.call('PEXPIRE', KEYS[1], math.ceil(burst * 60000 / rpm) + 1000)
return {allowed, math.floor(tokens), retry_ms}
`)

type Limiter struct {
	rdb redis.Scripter
}

func New(rdb redis.Scripter) *Limiter {
	return &Limiter{rdb: rdb}
}

// Allow spends one token from the bucket named key. An error means Redis
// could not be asked; the caller decides whether to fail open or closed.
func (l *Limiter) Allow(ctx context.Context, key string, limit Limit) (Result, error) {
	vals, err := bucketScript.Run(ctx, l.rdb, []string{"ratelimit:" + key}, limit.PerMinute, limit.Burst).Int64Slice()
	if err != nil {
		return Result{}, err
	}
	if len(vals) != 3 {
		return Result{}, fmt.Errorf("ratelimit: unexpected script reply %v", vals)
	}
	return Result{
		Allowed:    vals[0] == 1,
		Remaining:  int(vals[1]),
		RetryAfter: time.Duration(vals[2]) * time.Millisecond,
	}, nil
}
