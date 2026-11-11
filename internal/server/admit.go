package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Ani-tem/llm-gateway/internal/ratelimit"
)

// Tenant is one API key holder and the rate limit it gets.
type Tenant struct {
	Name  string
	Limit ratelimit.Limit
}

// HashKey is how API keys are stored and looked up. Only the SHA-256 hash is
// kept, so a leaked config file does not leak usable keys. A fast hash is
// fine here (unlike for passwords) because keys are long and random, which
// leaves nothing to brute-force.
func HashKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// How long a request may wait on one Redis call before we give up on it.
const redisTimeout = 250 * time.Millisecond

// admit authenticates the caller and applies its rate limit before the
// request reaches a provider.
func (s *Server) admit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, isBearer := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		tenant, known := s.tenants[HashKey(key)]
		if !isBearer || !known {
			writeError(w, http.StatusUnauthorized, "authentication_error", "missing or invalid API key")
			return
		}

		info(r.Context()).tenant = tenant

		ctx, cancel := context.WithTimeout(r.Context(), redisTimeout)
		res, err := s.limiter.Allow(ctx, tenant.Name, tenant.Limit)
		cancel()
		if err != nil {
			// Fail open: if Redis is down we would rather serve unlimited
			// traffic for a while than turn a limiter outage into a full
			// gateway outage. The upstream's own limits are still a backstop.
			slog.Warn("rate limiter unavailable, allowing request", "tenant", tenant.Name, "err", err)
			next.ServeHTTP(w, r)
			return
		}

		w.Header().Set("X-RateLimit-Limit", strconv.Itoa(tenant.Limit.PerMinute))
		w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(res.Remaining))
		if !res.Allowed {
			// Retry-After is whole seconds; round up so a client that obeys
			// it is never told to come back too early.
			w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(res.RetryAfter.Seconds()))))
			writeError(w, http.StatusTooManyRequests, "rate_limit_error", "rate limit exceeded for tenant "+tenant.Name)
			return
		}
		next.ServeHTTP(w, r)
	})
}
