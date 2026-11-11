// Package usage meters what each tenant consumes: one event per request,
// written to a store in the background.
package usage

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"
)

// Event is one finished request. Storing raw events rather than running
// counters means any breakdown (per model, per hour, cache savings) can be
// computed later with SQL, including ones nobody thought of today.
type Event struct {
	At               time.Time
	Tenant           string
	Model            string // as the client requested it; "" if rejected before parsing
	Upstream         string // "provider/model" that served it; "" for cache hits and failures
	Status           int
	Cached           bool
	Stream           bool
	PromptTokens     int
	CompletionTokens int
	Latency          time.Duration
}

// Store persists batches of events.
type Store interface {
	Insert(ctx context.Context, events []Event) error
}

const (
	queueSize     = 10_000
	batchSize     = 500
	flushInterval = time.Second
	insertTimeout = 5 * time.Second
)

// Recorder takes events off the request path. Record only puts the event on
// an in-memory queue; a single background goroutine writes them to the store
// in batches, so a request never waits on the database, and the database sees
// one bulk write per second instead of one write per request.
//
// The price is durability: events still in the queue are lost if the process
// is killed, and are dropped if the store is down or cannot keep up. That is
// acceptable for usage dashboards. Exact billing would need a durable log in
// front of the store instead of a channel.
type Recorder struct {
	store   Store
	events  chan Event
	quit    chan struct{}
	done    chan struct{}
	dropped atomic.Int64
}

func NewRecorder(store Store) *Recorder {
	r := &Recorder{
		store:  store,
		events: make(chan Event, queueSize),
		quit:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	go r.run()
	return r
}

// Record queues an event and never blocks. If the queue is full the event is
// dropped: serving traffic matters more than metering it.
func (r *Recorder) Record(e Event) {
	select {
	case r.events <- e:
	default:
		if n := r.dropped.Add(1); n == 1 || n%1000 == 0 {
			slog.Warn("usage queue full, dropping events", "dropped_total", n)
		}
	}
}

// Dropped is the number of events lost to a full queue or a failed write.
func (r *Recorder) Dropped() int64 { return r.dropped.Load() }

// Close writes out everything still queued and stops the background writer.
// Call it after the HTTP server has stopped.
func (r *Recorder) Close() {
	close(r.quit)
	<-r.done
}

func (r *Recorder) run() {
	defer close(r.done)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	batch := make([]Event, 0, batchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), insertTimeout)
		defer cancel()
		if err := r.store.Insert(ctx, batch); err != nil {
			r.dropped.Add(int64(len(batch)))
			slog.Error("usage write failed, events lost", "count", len(batch), "err", err)
		}
		batch = batch[:0]
	}
	add := func(e Event) {
		batch = append(batch, e)
		if len(batch) == batchSize {
			flush()
		}
	}

	for {
		select {
		case e := <-r.events:
			add(e)
		case <-ticker.C:
			flush()
		case <-r.quit:
			// Drain whatever is already queued, then stop.
			for {
				select {
				case e := <-r.events:
					add(e)
				default:
					flush()
					return
				}
			}
		}
	}
}
