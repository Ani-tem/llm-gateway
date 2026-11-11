package server_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/Ani-tem/llm-gateway/internal/cache"
	"github.com/Ani-tem/llm-gateway/internal/chat"
	"github.com/Ani-tem/llm-gateway/internal/provider"
	"github.com/Ani-tem/llm-gateway/internal/ratelimit"
	"github.com/Ani-tem/llm-gateway/internal/route"
	"github.com/Ani-tem/llm-gateway/internal/server"
)

const (
	testKey  = "test-key"
	otherKey = "other-key"
	cacheTTL = time.Minute
)

// newGatewayWithLimit starts a gateway with one tenant (key testKey) whose
// rate limiter runs against an in-process Redis.
func newGatewayWithLimit(t *testing.T, providers map[string]provider.Provider, limit ratelimit.Limit) (*httptest.Server, *miniredis.Miniredis) {
	t.Helper()
	routes := map[string][]route.Target{}
	if _, ok := providers["fake"]; ok {
		routes["fallback"] = []route.Target{{Provider: "fake", Model: "m"}, {Provider: "mock", Model: "echo"}}
	}
	router := route.New(providers, routes)
	router.Backoff = time.Millisecond
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	gw := httptest.NewServer(server.New(server.Config{
		Router: router,
		Tenants: map[string]server.Tenant{
			server.HashKey(testKey):  {Name: "test", Limit: limit},
			server.HashKey(otherKey): {Name: "other", Limit: limit},
		},
		Limiter: ratelimit.New(rdb),
		Cache:   cache.New(rdb, cacheTTL),
	}))
	t.Cleanup(gw.Close)
	return gw, mr
}

func newGateway(t *testing.T, providers map[string]provider.Provider) *httptest.Server {
	t.Helper()
	gw, _ := newGatewayWithLimit(t, providers, ratelimit.Limit{PerMinute: 6000, Burst: 1000})
	return gw
}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	return postWithKey(t, url, testKey, body)
}

func postWithKey(t *testing.T, url, key, body string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest("POST", url+"/v1/chat/completions", strings.NewReader(body))
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

const echoBody = `{"model":"mock/echo","messages":[{"role":"user","content":"x"}]}`

func TestRejectsMissingOrWrongKey(t *testing.T) {
	gw := newGateway(t, map[string]provider.Provider{"mock": &provider.Mock{}})
	for _, key := range []string{"", "wrong-key"} {
		if resp := postWithKey(t, gw.URL, key, echoBody); resp.StatusCode != 401 {
			t.Errorf("key %q: status = %d, want 401", key, resp.StatusCode)
		}
	}
}

func TestRateLimitReturns429(t *testing.T) {
	gw, _ := newGatewayWithLimit(t, map[string]provider.Provider{"mock": &provider.Mock{}},
		ratelimit.Limit{PerMinute: 60, Burst: 2})
	for i := range 2 {
		if resp := post(t, gw.URL, echoBody); resp.StatusCode != 200 {
			t.Fatalf("request %d: status = %d, want 200", i+1, resp.StatusCode)
		}
	}
	resp := post(t, gw.URL, echoBody)
	if resp.StatusCode != 429 {
		t.Fatalf("status = %d, want 429", resp.StatusCode)
	}
	if ra := resp.Header.Get("Retry-After"); ra != "1" {
		t.Errorf("Retry-After = %q, want \"1\"", ra)
	}
	if rem := resp.Header.Get("X-RateLimit-Remaining"); rem != "0" {
		t.Errorf("X-RateLimit-Remaining = %q, want \"0\"", rem)
	}
}

// A Redis outage must not take the gateway down with it.
func TestRedisDownFailsOpen(t *testing.T) {
	gw, mr := newGatewayWithLimit(t, map[string]provider.Provider{"mock": &provider.Mock{}},
		ratelimit.Limit{PerMinute: 60, Burst: 1})
	mr.Close()
	for i := range 3 {
		if resp := post(t, gw.URL, echoBody); resp.StatusCode != 200 {
			t.Fatalf("request %d with Redis down: status = %d, want 200", i+1, resp.StatusCode)
		}
	}
}

func TestChatMock(t *testing.T) {
	gw := newGateway(t, map[string]provider.Provider{"mock": &provider.Mock{}})
	resp := post(t, gw.URL, `{"model":"mock/echo","messages":[{"role":"user","content":"hello there"}]}`)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var got chat.Completion
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if c := got.Choices[0].Message.Content; c != "echo: hello there" {
		t.Errorf("content = %q", c)
	}
	if got.Usage.TotalTokens != 5 {
		t.Errorf("total tokens = %d, want 5", got.Usage.TotalTokens)
	}
}

func TestChatStreamMock(t *testing.T) {
	gw := newGateway(t, map[string]provider.Provider{"mock": &provider.Mock{}})
	resp := post(t, gw.URL, `{"model":"mock/echo","stream":true,"messages":[{"role":"user","content":"a b"}]}`)
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("content-type = %q", ct)
	}
	raw, _ := io.ReadAll(resp.Body)
	events := strings.Split(strings.TrimSpace(string(raw)), "\n\n")
	if last := events[len(events)-1]; last != "data: [DONE]" {
		t.Fatalf("last event = %q", last)
	}
	var text string
	for _, ev := range events[:len(events)-1] {
		var chunk chat.Completion
		if err := json.Unmarshal([]byte(strings.TrimPrefix(ev, "data: ")), &chunk); err != nil {
			t.Fatalf("bad chunk %q: %v", ev, err)
		}
		text += chunk.Choices[0].Delta.Content
	}
	if text != "echo: a b" {
		t.Errorf("streamed text = %q", text)
	}
}

func TestChatRejectsBadRequests(t *testing.T) {
	gw := newGateway(t, map[string]provider.Provider{"mock": &provider.Mock{}})
	cases := []struct {
		name, body string
		want       int
	}{
		{"not json", `nope`, 400},
		{"no model", `{"messages":[{"role":"user","content":"x"}]}`, 400},
		{"no messages", `{"model":"mock/echo","messages":[]}`, 400},
		{"unknown provider", `{"model":"nope/x","messages":[{"role":"user","content":"x"}]}`, 404},
		{"no prefix", `{"model":"echo","messages":[{"role":"user","content":"x"}]}`, 404},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := post(t, gw.URL, tc.body)
			if resp.StatusCode != tc.want {
				t.Errorf("status = %d, want %d", resp.StatusCode, tc.want)
			}
			var e chat.ErrorBody
			if err := json.NewDecoder(resp.Body).Decode(&e); err != nil || e.Error.Message == "" {
				t.Errorf("want OpenAI-style error body, got err=%v body=%+v", err, e)
			}
		})
	}
}

// fakeUpstream is an OpenAI-compatible server the adapter can point at.
func fakeUpstream(t *testing.T, h http.HandlerFunc) provider.Provider {
	t.Helper()
	up := httptest.NewServer(h)
	t.Cleanup(up.Close)
	return provider.NewOpenAI("fake", up.URL, "secret")
}

func TestOpenAIAdapterForwardsRequest(t *testing.T) {
	var gotAuth, gotPath string
	var gotBody map[string]any
	p := fakeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		gotAuth, gotPath = r.Header.Get("Authorization"), r.URL.Path
		json.NewDecoder(r.Body).Decode(&gotBody)
		io.WriteString(w, `{"id":"x","choices":[]}`)
	})
	gw := newGateway(t, map[string]provider.Provider{"fake": p})

	resp := post(t, gw.URL, `{"model":"fake/org/big-model","temperature":0.2,"tools":[{"type":"function"}],"messages":[{"role":"user","content":"x"}]}`)
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || string(body) != `{"id":"x","choices":[]}` {
		t.Fatalf("status = %d body = %s", resp.StatusCode, body)
	}
	if gotAuth != "Bearer secret" || gotPath != "/chat/completions" {
		t.Errorf("auth = %q path = %q", gotAuth, gotPath)
	}
	if gotBody["model"] != "org/big-model" {
		t.Errorf("upstream model = %v, want prefix stripped", gotBody["model"])
	}
	if gotBody["temperature"] != 0.2 || gotBody["tools"] == nil {
		t.Errorf("fields the gateway doesn't know were dropped: %v", gotBody)
	}
}

func TestOpenAIAdapterStream(t *testing.T) {
	p := fakeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, ": keep-alive\n\ndata: {\"n\":1}\n\ndata: {\"n\":2}\n\ndata: [DONE]\n\n")
	})
	gw := newGateway(t, map[string]provider.Provider{"fake": p})
	resp := post(t, gw.URL, `{"model":"fake/m","stream":true,"messages":[{"role":"user","content":"x"}]}`)
	raw, _ := io.ReadAll(resp.Body)
	want := "data: {\"n\":1}\n\ndata: {\"n\":2}\n\ndata: [DONE]\n\n"
	if string(raw) != want {
		t.Errorf("got %q, want %q", raw, want)
	}
}

func TestUpstreamErrorMapping(t *testing.T) {
	cases := []struct{ upstream, want int }{
		{429, 429}, // client is going too fast: tell them
		{400, 400},
		{401, 502}, // our key is bad: not the client's fault
		{500, 502},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprint(tc.upstream), func(t *testing.T) {
			p := fakeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "boom", tc.upstream)
			})
			gw := newGateway(t, map[string]provider.Provider{"fake": p})
			for _, stream := range []bool{false, true} {
				resp := post(t, gw.URL, fmt.Sprintf(`{"model":"fake/m","stream":%v,"messages":[{"role":"user","content":"x"}]}`, stream))
				if resp.StatusCode != tc.want {
					t.Errorf("stream=%v: status = %d, want %d", stream, resp.StatusCode, tc.want)
				}
			}
		})
	}
}

// When the client hangs up mid-stream the gateway must cancel the upstream
// call; otherwise we keep paying for tokens nobody will read.
func TestClientDisconnectCancelsUpstream(t *testing.T) {
	upstreamCancelled := make(chan struct{})
	p := fakeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "data: {\"n\":1}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(upstreamCancelled)
	})
	gw := newGateway(t, map[string]provider.Provider{"fake": p})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "POST", gw.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"fake/m","stream":true,"messages":[{"role":"user","content":"x"}]}`))
	req.Header.Set("Authorization", "Bearer "+testKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// The first chunk arriving proves it was flushed rather than buffered.
	line, err := bufio.NewReader(resp.Body).ReadString('\n')
	if err != nil || line != "data: {\"n\":1}\n" {
		t.Fatalf("first chunk = %q, err = %v", line, err)
	}
	cancel()

	select {
	case <-upstreamCancelled:
	case <-time.After(3 * time.Second):
		t.Fatal("upstream request was not cancelled after client disconnect")
	}
}

// End to end: the first target is down, so the "fallback" route is served by
// the second, for both plain and streamed requests.
func TestRouteFailsOverToNextTarget(t *testing.T) {
	down := fakeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", 503)
	})
	gw := newGateway(t, map[string]provider.Provider{"fake": down, "mock": &provider.Mock{}})
	for _, stream := range []bool{false, true} {
		resp := post(t, gw.URL, fmt.Sprintf(`{"model":"fallback","stream":%v,"messages":[{"role":"user","content":"x"}]}`, stream))
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 || !strings.Contains(string(body), "echo:") {
			t.Errorf("stream=%v: status = %d body = %s", stream, resp.StatusCode, body)
		}
		if got := resp.Header.Get("X-Gateway-Upstream"); got != "mock/echo" {
			t.Errorf("stream=%v: X-Gateway-Upstream = %q, want mock/echo", stream, got)
		}
	}
}

func TestAllBreakersOpenReturns503(t *testing.T) {
	down := fakeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", 503)
	})
	gw := newGateway(t, map[string]provider.Provider{"fake": down})
	body := `{"model":"fake/m","messages":[{"role":"user","content":"x"}]}`
	for range 3 { // 2 attempts each: the 5-failure breaker opens on the third
		post(t, gw.URL, body)
	}
	if resp := post(t, gw.URL, body); resp.StatusCode != 503 {
		t.Errorf("status = %d, want 503 once the breaker is open", resp.StatusCode)
	}
}
