package route

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Ani-tem/llm-gateway/internal/chat"
	"github.com/Ani-tem/llm-gateway/internal/provider"
)

// stub is a provider that fails with err (if set) and counts its calls.
type stub struct {
	err   error
	calls int
}

func (s *stub) Chat(context.Context, *chat.Request, string) ([]byte, error) {
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	return []byte("ok"), nil
}

func (s *stub) ChatStream(context.Context, *chat.Request, string) (provider.Stream, error) {
	panic("not used")
}

func status(code int) error {
	return &provider.UpstreamError{Provider: "stub", Status: code}
}

func newRouter(a, b *stub) (*Router, []Target) {
	r := New(map[string]provider.Provider{"a": a, "b": b}, nil)
	r.Backoff = time.Microsecond
	return r, []Target{{"a", "m"}, {"b", "m"}}
}

func TestFailoverPolicy(t *testing.T) {
	cases := []struct {
		name        string
		firstErr    error
		wantACalls  int
		wantBCalls  int
		wantServed  string
		wantFailure bool
	}{
		{"healthy: first target serves", nil, 1, 0, "a", false},
		{"503: retry once, then fail over", status(503), 2, 1, "b", false},
		{"network error: retry once, then fail over", errors.New("connection refused"), 2, 1, "b", false},
		{"429: fail over without retrying", status(429), 1, 1, "b", false},
		{"401: fail over without retrying", status(401), 1, 1, "b", false},
		{"400: give up, the request is bad", status(400), 1, 0, "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, b := &stub{err: tc.firstErr}, &stub{}
			r, targets := newRouter(a, b)
			_, served, err := r.Chat(context.Background(), nil, targets)
			if (err != nil) != tc.wantFailure {
				t.Fatalf("err = %v, wantFailure = %v", err, tc.wantFailure)
			}
			if a.calls != tc.wantACalls || b.calls != tc.wantBCalls {
				t.Errorf("calls a=%d b=%d, want a=%d b=%d", a.calls, b.calls, tc.wantACalls, tc.wantBCalls)
			}
			if err == nil && served.Provider != tc.wantServed {
				t.Errorf("served by %q, want %q", served.Provider, tc.wantServed)
			}
		})
	}
}

func TestAllTargetsFailingReturnsLastError(t *testing.T) {
	a, b := &stub{err: status(503)}, &stub{err: status(502)}
	r, targets := newRouter(a, b)
	_, _, err := r.Chat(context.Background(), nil, targets)
	var up *provider.UpstreamError
	if !errors.As(err, &up) || up.Status != 502 {
		t.Fatalf("err = %v, want the last target's 502", err)
	}
}

func TestBreakerStopsCallingDeadProvider(t *testing.T) {
	a, b := &stub{err: status(503)}, &stub{}
	r, targets := newRouter(a, b)

	// Threshold is 5 and each request makes 2 attempts, so the breaker opens
	// during the third request.
	for range 3 {
		r.Chat(context.Background(), nil, targets)
	}
	if a.calls != 5 {
		t.Fatalf("a called %d times, want 5 (then the breaker opens)", a.calls)
	}
	for range 10 {
		if _, served, err := r.Chat(context.Background(), nil, targets); err != nil || served.Provider != "b" {
			t.Fatalf("served = %v err = %v, want b", served, err)
		}
	}
	if a.calls != 5 {
		t.Errorf("a called %d times while its breaker was open, want still 5", a.calls)
	}
}

func TestAllBreakersOpen(t *testing.T) {
	a := &stub{err: status(503)}
	r := New(map[string]provider.Provider{"a": a}, nil)
	r.Backoff = time.Microsecond
	targets := []Target{{"a", "m"}}
	for range 3 {
		r.Chat(context.Background(), nil, targets)
	}
	if _, _, err := r.Chat(context.Background(), nil, targets); !errors.Is(err, ErrNoHealthyUpstream) {
		t.Fatalf("err = %v, want ErrNoHealthyUpstream", err)
	}
}

func TestCallerCancelDoesNotCountAgainstProvider(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	a, b := &stub{err: context.Canceled}, &stub{}
	r, targets := newRouter(a, b)
	for range 10 {
		r.Chat(ctx, nil, targets)
	}
	if a.calls != 10 || b.calls != 0 {
		t.Errorf("calls a=%d b=%d, want a=10 b=0: a cancelled request must not fail over or trip the breaker", a.calls, b.calls)
	}
}

func TestResolve(t *testing.T) {
	r := New(map[string]provider.Provider{"a": &stub{}},
		map[string][]Target{"auto": {{"a", "x"}}})
	cases := []struct {
		model string
		want  string
	}{
		{"auto", "a/x"},
		{"a/org/model", "a/org/model"},
		{"a", ""},
		{"a/", ""},
		{"nope/x", ""},
	}
	for _, tc := range cases {
		got := ""
		if ts := r.Resolve(tc.model); len(ts) == 1 {
			got = ts[0].String()
		}
		if got != tc.want {
			t.Errorf("Resolve(%q) = %q, want %q", tc.model, got, tc.want)
		}
	}
}

func TestBreakerLifecycle(t *testing.T) {
	now := time.Unix(0, 0)
	b := NewBreaker(2, 10*time.Second)
	b.now = func() time.Time { return now }

	b.Record(false)
	b.Record(true) // a success resets the count: failures must be consecutive
	b.Record(false)
	if !b.Allow() {
		t.Fatal("closed breaker should allow")
	}
	b.Record(false)
	if b.Allow() {
		t.Fatal("breaker should be open after 2 consecutive failures")
	}

	now = now.Add(10 * time.Second)
	if !b.Allow() {
		t.Fatal("after the cooldown one probe should be allowed")
	}
	if b.Allow() {
		t.Fatal("only one probe at a time")
	}
	b.Record(false)
	if b.Allow() {
		t.Fatal("a failed probe should re-open the breaker")
	}

	now = now.Add(10 * time.Second)
	if !b.Allow() {
		t.Fatal("second probe should be allowed")
	}
	b.Record(true)
	if !b.Allow() || !b.Allow() {
		t.Fatal("a successful probe should close the breaker")
	}
}
