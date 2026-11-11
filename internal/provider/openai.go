package provider

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Ani-tem/llm-gateway/internal/chat"
)

// OpenAI talks to any upstream exposing the OpenAI chat-completions API.
// Ollama, Groq, OpenRouter and Gemini's compatibility endpoint all do, so this
// one adapter covers them; only the base URL and key differ.
type OpenAI struct {
	name    string
	baseURL string
	apiKey  string
	client  *http.Client
}

func NewOpenAI(name, baseURL, apiKey string) *OpenAI {
	// No http.Client.Timeout: it caps the whole exchange including the body,
	// which would cut off long streams. Instead we bound only the wait for
	// response headers, and rely on the request context for everything else.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = 60 * time.Second
	return &OpenAI{
		name:    name,
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		client:  &http.Client{Transport: transport},
	}
}

func (p *OpenAI) Chat(ctx context.Context, req *chat.Request, model string) ([]byte, error) {
	resp, err := p.send(ctx, req, model)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func (p *OpenAI) ChatStream(ctx context.Context, req *chat.Request, model string) (Stream, error) {
	resp, err := p.send(ctx, req, model)
	if err != nil {
		return nil, err
	}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024) // default 64KB line cap is too small for big chunks
	return &sseStream{body: resp.Body, scanner: sc}, nil
}

// send returns a 200 response whose body the caller must close.
func (p *OpenAI) send(ctx context.Context, req *chat.Request, model string) (*http.Response, error) {
	body, err := req.BodyForModel(model)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if p.apiKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+p.apiKey)
	}
	resp, err := p.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", p.name, err)
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 8*1024))
		return nil, &UpstreamError{Provider: p.name, Status: resp.StatusCode, Body: msg}
	}
	return resp, nil
}

// sseStream reads Server-Sent Events: lines of "data: <json>", ending with
// "data: [DONE]". Blank lines and ":" comments (keep-alives) are skipped.
type sseStream struct {
	body    io.ReadCloser
	scanner *bufio.Scanner
}

func (s *sseStream) Recv() ([]byte, error) {
	for s.scanner.Scan() {
		data, ok := bytes.CutPrefix(s.scanner.Bytes(), []byte("data:"))
		if !ok {
			continue
		}
		data = bytes.TrimSpace(data)
		if string(data) == "[DONE]" {
			return nil, io.EOF
		}
		return bytes.Clone(data), nil // the scanner reuses its buffer
	}
	if err := s.scanner.Err(); err != nil {
		return nil, err
	}
	return nil, io.ErrUnexpectedEOF // upstream hung up without [DONE]
}

func (s *sseStream) Close() error { return s.body.Close() }
