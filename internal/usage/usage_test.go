package usage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

// memStore records the batches it is given.
type memStore struct {
	mu      sync.Mutex
	batches [][]Event
	err     error
	block   chan struct{} // if set, Insert waits on it
}

func (m *memStore) Insert(_ context.Context, events []Event) error {
	if m.block != nil {
		<-m.block
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.batches = append(m.batches, append([]Event(nil), events...))
	return nil
}

func (m *memStore) total() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, b := range m.batches {
		n += len(b)
	}
	return n
}

func TestCloseFlushesEverythingInBatches(t *testing.T) {
	store := &memStore{}
	r := NewRecorder(store)
	const n = batchSize*2 + 7
	for i := range n {
		r.Record(Event{Tenant: "t", Status: i})
	}
	r.Close()

	if got := store.total(); got != n {
		t.Fatalf("stored %d events, want %d", got, n)
	}
	// Events arrive in order and no batch exceeds the batch size.
	i := 0
	for _, b := range store.batches {
		if len(b) > batchSize {
			t.Errorf("batch of %d exceeds batch size %d", len(b), batchSize)
		}
		for _, e := range b {
			if e.Status != i {
				t.Fatalf("event %d out of order (got %d)", i, e.Status)
			}
			i++
		}
	}
}

func TestFlushesOnTimerWithoutClose(t *testing.T) {
	store := &memStore{}
	r := NewRecorder(store)
	defer r.Close()
	r.Record(Event{Tenant: "t"})

	deadline := time.Now().Add(flushInterval + 2*time.Second)
	for store.total() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("a lone event was never flushed by the timer")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestFullQueueDropsInsteadOfBlocking(t *testing.T) {
	store := &memStore{block: make(chan struct{})}
	r := NewRecorder(store)

	// The writer is stuck in Insert, so the queue fills up. Record must
	// return immediately for every event regardless.
	done := make(chan struct{})
	go func() {
		for range queueSize + batchSize + 100 {
			r.Record(Event{Tenant: "t"})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Record blocked on a full queue")
	}
	if r.Dropped() == 0 {
		t.Error("expected some events to be dropped")
	}
	close(store.block)
	r.Close()
}

func TestStoreFailureCountsEventsAsDropped(t *testing.T) {
	r := NewRecorder(&memStore{err: errors.New("db down")})
	for range 3 {
		r.Record(Event{Tenant: "t"})
	}
	r.Close()
	if got := r.Dropped(); got != 3 {
		t.Errorf("Dropped() = %d, want 3", got)
	}
}

// Runs only against a real database:
//
//	TEST_DATABASE_URL=postgres://gateway:gateway@localhost:5434/gateway go test ./internal/usage/
func TestPostgresInsertAndSummary(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	pg, err := OpenPostgres(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close()

	// Unique tenant names keep runs from seeing each other's rows.
	mine := fmt.Sprintf("test-%d", time.Now().UnixNano())
	other := mine + "-other"
	now := time.Now()
	events := []Event{
		{At: now, Tenant: mine, Model: "a", Upstream: "p/a", Status: 200, PromptTokens: 10, CompletionTokens: 20, Latency: 30 * time.Millisecond},
		{At: now, Tenant: mine, Model: "a", Status: 200, Cached: true, PromptTokens: 10, CompletionTokens: 20},
		{At: now, Tenant: mine, Model: "a", Status: 502},
		{At: now, Tenant: mine, Model: "b", Upstream: "p/b", Status: 200, Stream: true, PromptTokens: 1, CompletionTokens: 2},
		{At: now.Add(-48 * time.Hour), Tenant: mine, Model: "a", Upstream: "p/a", Status: 200, PromptTokens: 999},
		{At: now, Tenant: other, Model: "a", Upstream: "p/a", Status: 200, PromptTokens: 999},
	}
	if err := pg.Insert(ctx, events); err != nil {
		t.Fatal(err)
	}

	got, err := pg.Summary(ctx, mine, now.Add(-24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	want := []ModelUsage{
		{Model: "a", Requests: 3, CachedRequests: 1, FailedRequests: 1, PromptTokens: 10, CompletionTokens: 20, TokensSavedByCache: 30},
		{Model: "b", Requests: 1, PromptTokens: 1, CompletionTokens: 2},
	}
	if len(got) != len(want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("row %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
}
