package usage

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The (tenant, at) index serves the only read pattern: one tenant's events
// over a time range.
const schema = `
CREATE TABLE IF NOT EXISTS usage_events (
	id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	at                TIMESTAMPTZ NOT NULL,
	tenant            TEXT        NOT NULL,
	model             TEXT        NOT NULL,
	upstream          TEXT        NOT NULL,
	status            INT         NOT NULL,
	cached            BOOLEAN     NOT NULL,
	stream            BOOLEAN     NOT NULL,
	prompt_tokens     INT         NOT NULL,
	completion_tokens INT         NOT NULL,
	latency_ms        INT         NOT NULL
);
CREATE INDEX IF NOT EXISTS usage_events_tenant_at ON usage_events (tenant, at);
`

var columns = []string{"at", "tenant", "model", "upstream", "status", "cached", "stream",
	"prompt_tokens", "completion_tokens", "latency_ms"}

type Postgres struct {
	pool *pgxpool.Pool
}

// OpenPostgres connects and creates the table if needed. It fails fast: if
// metering is configured but the database is unreachable at startup, that is
// a deployment mistake worth surfacing immediately.
func OpenPostgres(ctx context.Context, url string) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("usage: %w", err)
	}
	if _, err := pool.Exec(ctx, schema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("usage: creating schema: %w", err)
	}
	return &Postgres{pool: pool}, nil
}

func (p *Postgres) Close() { p.pool.Close() }

// Insert writes a batch with COPY, Postgres's bulk-load protocol. It is much
// faster than row-by-row INSERTs and the whole batch commits or fails as one.
func (p *Postgres) Insert(ctx context.Context, events []Event) error {
	_, err := p.pool.CopyFrom(ctx, pgx.Identifier{"usage_events"}, columns,
		pgx.CopyFromSlice(len(events), func(i int) ([]any, error) {
			e := events[i]
			return []any{e.At, e.Tenant, e.Model, e.Upstream, e.Status, e.Cached, e.Stream,
				e.PromptTokens, e.CompletionTokens, e.Latency.Milliseconds()}, nil
		}))
	return err
}

// ModelUsage is one tenant's totals for one model.
type ModelUsage struct {
	Model            string `json:"model"`
	Requests         int64  `json:"requests"`
	CachedRequests   int64  `json:"cached_requests"`
	FailedRequests   int64  `json:"failed_requests"`
	PromptTokens     int64  `json:"prompt_tokens"`
	CompletionTokens int64  `json:"completion_tokens"`
	// Tokens that cache hits avoided sending upstream.
	TokensSavedByCache int64 `json:"tokens_saved_by_cache"`
}

// Summary totals a tenant's usage per model since the given time.
func (p *Postgres) Summary(ctx context.Context, tenant string, since time.Time) ([]ModelUsage, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT model,
		       count(*),
		       count(*) FILTER (WHERE cached),
		       count(*) FILTER (WHERE status >= 400),
		       coalesce(sum(prompt_tokens)     FILTER (WHERE NOT cached), 0),
		       coalesce(sum(completion_tokens) FILTER (WHERE NOT cached), 0),
		       coalesce(sum(prompt_tokens + completion_tokens) FILTER (WHERE cached), 0)
		FROM usage_events
		WHERE tenant = $1 AND at >= $2
		GROUP BY model
		ORDER BY count(*) DESC, model`, tenant, since)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (ModelUsage, error) {
		var m ModelUsage
		err := row.Scan(&m.Model, &m.Requests, &m.CachedRequests, &m.FailedRequests,
			&m.PromptTokens, &m.CompletionTokens, &m.TokensSavedByCache)
		return m, err
	})
}
