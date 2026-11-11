package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/Ani-tem/llm-gateway/internal/usage"
)

// UsageReader answers usage queries; *usage.Postgres implements it.
type UsageReader interface {
	Summary(ctx context.Context, tenant string, since time.Time) ([]usage.ModelUsage, error)
}

const maxUsageHours = 24 * 90

// handleUsage reports the calling tenant's own usage. The tenant comes from
// the API key, never from a parameter, so one tenant cannot read another's.
func (s *Server) handleUsage(w http.ResponseWriter, r *http.Request) {
	if s.usageReader == nil {
		writeError(w, http.StatusNotFound, "invalid_request_error", "usage metering is not enabled")
		return
	}
	hours := 24
	if q := r.URL.Query().Get("hours"); q != "" {
		n, err := strconv.Atoi(q)
		if err != nil || n < 1 || n > maxUsageHours {
			writeError(w, http.StatusBadRequest, "invalid_request_error",
				"hours must be a whole number from 1 to "+strconv.Itoa(maxUsageHours))
			return
		}
		hours = n
	}
	tenant := info(r.Context()).tenant.Name
	since := time.Now().Add(-time.Duration(hours) * time.Hour)
	models, err := s.usageReader.Summary(r.Context(), tenant, since)
	if err != nil {
		slog.Error("usage query failed", "err", err)
		writeError(w, http.StatusServiceUnavailable, "api_error", "usage data is temporarily unavailable")
		return
	}
	if models == nil {
		models = []usage.ModelUsage{} // encode as [] rather than null
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(struct {
		Tenant string             `json:"tenant"`
		Since  time.Time          `json:"since"`
		Models []usage.ModelUsage `json:"models"`
	}{tenant, since.UTC().Truncate(time.Second), models})
}
