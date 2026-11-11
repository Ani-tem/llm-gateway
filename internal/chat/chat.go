// Package chat holds the OpenAI chat-completions wire format, which is the
// gateway's single public API and also what most upstream providers speak.
package chat

import (
	"encoding/json"
	"errors"
	"maps"
	"strings"
)

// Request is a parsed chat-completions request.
//
// Only the fields the gateway itself needs are typed. Everything else stays as
// raw JSON in fields and is forwarded untouched, so new upstream features
// (tools, response_format, ...) work without a gateway release.
type Request struct {
	Model  string
	Stream bool
	fields map[string]json.RawMessage
}

// ParseRequest validates the minimum the gateway needs to route a request.
func ParseRequest(data []byte) (*Request, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, errors.New("request body must be a JSON object")
	}
	r := &Request{fields: fields}

	if raw, ok := fields["model"]; ok {
		if err := json.Unmarshal(raw, &r.Model); err != nil {
			return nil, errors.New("model must be a string")
		}
	}
	if r.Model == "" {
		return nil, errors.New("model is required")
	}
	if raw, ok := fields["stream"]; ok {
		if err := json.Unmarshal(raw, &r.Stream); err != nil {
			return nil, errors.New("stream must be a boolean")
		}
	}
	var msgs []json.RawMessage
	if err := json.Unmarshal(fields["messages"], &msgs); err != nil || len(msgs) == 0 {
		return nil, errors.New("messages must be a non-empty array")
	}
	return r, nil
}

// BodyForModel returns the request body to send upstream, identical to what
// the client sent except for the model name.
func (r *Request) BodyForModel(model string) ([]byte, error) {
	out := maps.Clone(r.fields)
	name, err := json.Marshal(model)
	if err != nil {
		return nil, err
	}
	out["model"] = name
	return json.Marshal(out)
}

// LastUserText returns the text of the last user message, or "" if none.
func (r *Request) LastUserText() string {
	var msgs []struct {
		Role    string          `json:"role"`
		Content json.RawMessage `json:"content"`
	}
	if err := json.Unmarshal(r.fields["messages"], &msgs); err != nil {
		return ""
	}
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == "user" {
			return contentText(msgs[i].Content)
		}
	}
	return ""
}

// contentText handles both content shapes: a plain string, or an array of
// parts like [{"type":"text","text":"..."}].
func contentText(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(raw, &parts) != nil {
		return ""
	}
	var texts []string
	for _, p := range parts {
		if p.Type == "text" {
			texts = append(texts, p.Text)
		}
	}
	return strings.Join(texts, " ")
}

// Completion is a non-streaming response.
type Completion struct {
	ID      string   `json:"id"`
	Object  string   `json:"object"`
	Created int64    `json:"created"`
	Model   string   `json:"model"`
	Choices []Choice `json:"choices"`
	Usage   *Usage   `json:"usage,omitempty"`
}

// Choice carries Message in a full response and Delta in a streamed chunk.
type Choice struct {
	Index        int      `json:"index"`
	Message      *Message `json:"message,omitempty"`
	Delta        *Message `json:"delta,omitempty"`
	FinishReason *string  `json:"finish_reason"`
}

type Message struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content"`
}

type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// ErrorBody is the OpenAI error envelope.
type ErrorBody struct {
	Error ErrorDetail `json:"error"`
}

type ErrorDetail struct {
	Message string `json:"message"`
	Type    string `json:"type"`
}
