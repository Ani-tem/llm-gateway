// Package provider defines what the gateway needs from an upstream LLM and
// the adapters that satisfy it.
package provider

import (
	"context"
	"fmt"

	"github.com/Ani-tem/llm-gateway/internal/chat"
)

// Provider is one upstream. The model argument is the upstream's own model
// name, with the gateway's "provider/" prefix already stripped.
type Provider interface {
	// Chat returns a complete OpenAI-format response body.
	Chat(ctx context.Context, req *chat.Request, model string) ([]byte, error)
	// ChatStream starts a streamed response. An error here means nothing has
	// been sent to the client yet, so the caller can still fail over.
	ChatStream(ctx context.Context, req *chat.Request, model string) (Stream, error)
}

// Stream is pull-based (like gRPC's Recv) rather than a channel: the consumer
// sets the pace, so a slow client naturally slows the upstream read, and there
// is no producer goroutine to leak if the consumer walks away.
type Stream interface {
	// Recv returns the JSON payload of the next chunk, or io.EOF when the
	// stream ended cleanly.
	Recv() ([]byte, error)
	Close() error
}

// UpstreamError is a non-200 response from an upstream.
type UpstreamError struct {
	Provider string
	Status   int
	Body     []byte
}

func (e *UpstreamError) Error() string {
	return fmt.Sprintf("%s returned %d: %s", e.Provider, e.Status, e.Body)
}
