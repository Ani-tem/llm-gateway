// Package cache stores complete chat responses in Redis so an identical
// request can be answered without calling (or paying) a provider again.
package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

type Cache struct {
	rdb redis.Cmdable
	ttl time.Duration
}

func New(rdb redis.Cmdable, ttl time.Duration) *Cache {
	return &Cache{rdb: rdb, ttl: ttl}
}

// Key identifies a request. This is an exact-match cache: the same tenant
// sending the same bytes gets the same answer.
//
// The tenant is part of the key so one tenant can never be served, or even
// detect through timing, a response generated for another tenant's prompt.
//
// Hashing keeps Redis keys a fixed small size however long the prompt is.
func Key(tenant string, body []byte) string {
	h := sha256.New()
	h.Write([]byte(tenant))
	h.Write([]byte{0})
	h.Write(body)
	return "cache:" + hex.EncodeToString(h.Sum(nil))
}

// Get returns the cached response, or ok=false on a miss.
func (c *Cache) Get(ctx context.Context, key string) (body []byte, ok bool, err error) {
	body, err = c.rdb.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return body, true, nil
}

// Set stores a response. Entries expire after the TTL, which bounds both
// staleness and memory.
func (c *Cache) Set(ctx context.Context, key string, body []byte) error {
	return c.rdb.Set(ctx, key, body, c.ttl).Err()
}
