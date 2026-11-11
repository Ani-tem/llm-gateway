package server_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Ani-tem/llm-gateway/internal/provider"
	"github.com/Ani-tem/llm-gateway/internal/ratelimit"
	"github.com/Ani-tem/llm-gateway/internal/usage"
)

// eventLog is an in-memory usage.Store.
type eventLog struct {
	mu     sync.Mutex
	events []usage.Event
}

func (l *eventLog) Insert(_ context.Context, events []usage.Event) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, events...)
	return nil
}

// Each test gets its own recorder and log. recorded() flushes and returns
// what the gateway metered during the test.
var (
	testLogs      sync.Map // *testing.T -> *eventLog
	testRecorders sync.Map // *testing.T -> func() that flushes and closes the recorder
)

func testUsage(t *testing.T) *usage.Recorder {
	log := &eventLog{}
	rec := usage.NewRecorder(log)
	testLogs.Store(t, log)
	var once sync.Once
	closeRec := func() { once.Do(rec.Close) }
	t.Cleanup(closeRec)
	testRecorders.Store(t, closeRec)
	return rec
}

func recorded(t *testing.T) []usage.Event {
	t.Helper()
	closeRec, _ := testRecorders.Load(t)
	closeRec.(func())() // flush
	log, _ := testLogs.Load(t)
	return log.(*eventLog).events
}

// fakeReader answers usage queries with the tenant it was asked about, so a
// test can see whose data the handler requested.
type fakeReader struct{}

func (fakeReader) Summary(_ context.Context, tenant string, since time.Time) ([]usage.ModelUsage, error) {
	return []usage.ModelUsage{{Model: "asked-for:" + tenant, Requests: int64(time.Since(since).Round(time.Hour).Hours())}}, nil
}

func TestUsageRecordedForChat(t *testing.T) {
	gw := newGateway(t, map[string]provider.Provider{"mock": &provider.Mock{}})
	body := `{"model":"mock/echo","messages":[{"role":"user","content":"one two three"}]}`
	post(t, gw.URL, body)                                                            // miss
	post(t, gw.URL, body)                                                            // cache hit
	post(t, gw.URL, `{"model":"nope/x","messages":[{"role":"user","content":"x"}]}`) // 404

	events := recorded(t)
	if len(events) != 3 {
		t.Fatalf("recorded %d events, want 3: %+v", len(events), events)
	}
	miss, hit, notFound := events[0], events[1], events[2]

	if miss.Tenant != "test" || miss.Model != "mock/echo" || miss.Upstream != "mock/echo" ||
		miss.Status != 200 || miss.Cached || miss.PromptTokens != 3 || miss.CompletionTokens != 4 {
		t.Errorf("miss event wrong: %+v", miss)
	}
	if !hit.Cached || hit.Upstream != "" || hit.PromptTokens != 3 || hit.CompletionTokens != 4 {
		t.Errorf("hit event wrong: %+v", hit)
	}
	if notFound.Status != 404 || notFound.Model != "nope/x" || notFound.PromptTokens != 0 {
		t.Errorf("404 event wrong: %+v", notFound)
	}
}

func TestUsageRecordedForRateLimitedRequest(t *testing.T) {
	gw, _ := newGatewayWithLimit(t, map[string]provider.Provider{"mock": &provider.Mock{}},
		ratelimit.Limit{PerMinute: 60, Burst: 1})
	post(t, gw.URL, echoBody)
	post(t, gw.URL, echoBody)                     // 429
	postWithKey(t, gw.URL, "wrong-key", echoBody) // 401: no tenant, so nothing to meter

	events := recorded(t)
	if len(events) != 2 || events[1].Status != 429 || events[1].Tenant != "test" {
		t.Fatalf("want [200, 429] for tenant test, got %+v", events)
	}
}

// Stream token counts come from a usage chunk the gateway asks the provider
// for. A client that did not ask for it must not see it.
func TestStreamUsageIsMeteredButHiddenUnlessRequested(t *testing.T) {
	cases := []struct {
		name          string
		streamOptions string
		wantChunk     bool
	}{
		{"client did not ask", ``, false},
		{"client asked", `"stream_options":{"include_usage":true},`, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gw := newGateway(t, map[string]provider.Provider{"mock": &provider.Mock{}})
			resp := post(t, gw.URL, `{"model":"mock/echo","stream":true,`+tc.streamOptions+`"messages":[{"role":"user","content":"a b"}]}`)
			raw, _ := io.ReadAll(resp.Body)
			if got := strings.Contains(string(raw), `"usage"`); got != tc.wantChunk {
				t.Errorf("usage chunk forwarded = %v, want %v\n%s", got, tc.wantChunk, raw)
			}
			if !strings.HasSuffix(string(raw), "data: [DONE]\n\n") {
				t.Errorf("stream did not end with [DONE]: %q", raw)
			}

			events := recorded(t)
			if len(events) != 1 {
				t.Fatalf("recorded %d events, want 1", len(events))
			}
			e := events[0]
			if !e.Stream || e.PromptTokens != 2 || e.CompletionTokens != 3 || e.Upstream != "mock/echo" {
				t.Errorf("stream event wrong: %+v", e)
			}
		})
	}
}

// The gateway must ask the upstream for stream usage even if the client did
// not, and must keep any other stream options the client set.
func TestStreamRequestAsksUpstreamForUsage(t *testing.T) {
	var got struct {
		StreamOptions map[string]any `json:"stream_options"`
	}
	up := fakeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&got)
		io.WriteString(w, "data: [DONE]\n\n")
	})
	gw := newGateway(t, map[string]provider.Provider{"fake": up})
	post(t, gw.URL, `{"model":"fake/m","stream":true,"stream_options":{"other":1},"messages":[{"role":"user","content":"x"}]}`)
	if got.StreamOptions["include_usage"] != true || got.StreamOptions["other"] != float64(1) {
		t.Errorf("upstream stream_options = %v, want include_usage:true and other:1", got.StreamOptions)
	}
}

func TestUsageEndpointIsScopedToCallingTenant(t *testing.T) {
	gw := newGateway(t, map[string]provider.Provider{"mock": &provider.Mock{}})
	get := func(key, query string) (int, string) {
		req, _ := http.NewRequest("GET", gw.URL+"/v1/usage"+query, nil)
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}

	if code, body := get(testKey, "?hours=48"); code != 200 ||
		!strings.Contains(body, `"asked-for:test"`) || !strings.Contains(body, `"requests":48`) {
		t.Errorf("test tenant: %d %s", code, body)
	}
	if code, body := get(otherKey, ""); code != 200 || !strings.Contains(body, `"asked-for:other"`) {
		t.Errorf("other tenant: %d %s", code, body)
	}
	if code, _ := get("wrong-key", ""); code != 401 {
		t.Errorf("no valid key: status %d, want 401", code)
	}
	if code, _ := get(testKey, "?hours=abc"); code != 400 {
		t.Errorf("bad hours: status %d, want 400", code)
	}
	// Reading usage is not itself a metered request.
	if events := recorded(t); len(events) != 0 {
		t.Errorf("usage queries were metered: %+v", events)
	}
}
