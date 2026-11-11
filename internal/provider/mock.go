package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Ani-tem/llm-gateway/internal/chat"
)

// Mock is a fake upstream that echoes the last user message. It costs nothing
// and behaves predictably, which makes it the target for tests and load tests.
type Mock struct {
	// Latency is the delay before a full response, and between stream chunks.
	Latency time.Duration
	// FailRate is the fraction of calls (0 to 1) that fail with a 503, for
	// exercising retries, failover and the circuit breaker.
	FailRate float64
	nextID   atomic.Int64
}

func (m *Mock) Chat(ctx context.Context, req *chat.Request, model string) ([]byte, error) {
	if err := sleep(ctx, m.Latency); err != nil {
		return nil, err
	}
	if err := m.injectedFailure(); err != nil {
		return nil, err
	}
	prompt := req.LastUserText()
	reply := "echo: " + prompt
	stop := "stop"
	in, out := len(strings.Fields(prompt)), len(strings.Fields(reply))
	return json.Marshal(chat.Completion{
		ID:      m.id(),
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   model,
		Choices: []chat.Choice{{
			Message:      &chat.Message{Role: "assistant", Content: reply},
			FinishReason: &stop,
		}},
		// Word count stands in for a real tokenizer.
		Usage: &chat.Usage{PromptTokens: in, CompletionTokens: out, TotalTokens: in + out},
	})
}

func (m *Mock) ChatStream(ctx context.Context, req *chat.Request, model string) (Stream, error) {
	if err := m.injectedFailure(); err != nil {
		return nil, err
	}
	return &mockStream{
		ctx:     ctx,
		id:      m.id(),
		model:   model,
		words:   strings.Fields("echo: " + req.LastUserText()),
		prompt:  len(strings.Fields(req.LastUserText())),
		latency: m.Latency,
	}, nil
}

func (m *Mock) injectedFailure() error {
	if rand.Float64() < m.FailRate {
		return &UpstreamError{Provider: "mock", Status: 503, Body: []byte("injected failure")}
	}
	return nil
}

func (m *Mock) id() string {
	return fmt.Sprintf("chatcmpl-mock-%d", m.nextID.Add(1))
}

type mockStream struct {
	ctx     context.Context
	id      string
	model   string
	words   []string
	next    int // index of the next word; then a "stop" chunk, then a usage chunk
	prompt  int // prompt word count, for the usage chunk
	latency time.Duration
}

func (s *mockStream) Recv() ([]byte, error) {
	if s.next > len(s.words)+1 {
		return nil, io.EOF
	}
	if err := sleep(s.ctx, s.latency); err != nil {
		return nil, err
	}
	if s.next == len(s.words)+1 {
		// Like real providers: usage arrives last, in a chunk with no choices.
		s.next++
		return json.Marshal(chat.Completion{
			ID:      s.id,
			Object:  "chat.completion.chunk",
			Created: time.Now().Unix(),
			Model:   s.model,
			Choices: []chat.Choice{},
			Usage: &chat.Usage{PromptTokens: s.prompt, CompletionTokens: len(s.words),
				TotalTokens: s.prompt + len(s.words)},
		})
	}
	choice := chat.Choice{Delta: &chat.Message{}}
	switch {
	case s.next == len(s.words):
		stop := "stop"
		choice.FinishReason = &stop
	case s.next == 0:
		choice.Delta = &chat.Message{Role: "assistant", Content: s.words[0]}
	default:
		choice.Delta.Content = " " + s.words[s.next]
	}
	s.next++
	return json.Marshal(chat.Completion{
		ID:      s.id,
		Object:  "chat.completion.chunk",
		Created: time.Now().Unix(),
		Model:   s.model,
		Choices: []chat.Choice{choice},
	})
}

func (s *mockStream) Close() error { return nil }

// sleep waits for d but returns early if the request is cancelled.
func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
