package server_test

import (
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/Ani-tem/llm-gateway/internal/provider"
	"github.com/Ani-tem/llm-gateway/internal/ratelimit"
)

// countingGateway has one upstream ("fake") that counts its calls and answers
// according to *status, so tests can see whether the cache spared the call.
func countingGateway(t *testing.T, delay time.Duration) (url string, calls *atomic.Int64, status *atomic.Int64, mr *miniredis.Miniredis) {
	t.Helper()
	calls, status = new(atomic.Int64), new(atomic.Int64)
	status.Store(200)
	up := fakeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		time.Sleep(delay)
		if code := int(status.Load()); code != 200 {
			http.Error(w, "boom", code)
			return
		}
		io.WriteString(w, `{"id":"x","choices":[]}`)
	})
	gw, mr := newGatewayWithLimit(t, map[string]provider.Provider{"fake": up},
		ratelimit.Limit{PerMinute: 6000, Burst: 1000})
	return gw.URL, calls, status, mr
}

func send(t *testing.T, url, key, body string, header ...string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest("POST", url+"/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+key)
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

const fakeBody = `{"model":"fake/m","messages":[{"role":"user","content":"x"}]}`

func TestCacheHitSkipsUpstream(t *testing.T) {
	url, calls, _, _ := countingGateway(t, 0)

	first := send(t, url, testKey, fakeBody)
	if got := first.Header.Get("X-Gateway-Cache"); got != "miss" {
		t.Errorf("first request: X-Gateway-Cache = %q, want miss", got)
	}
	// Same request with the fields in a different order is the same entry.
	second := send(t, url, testKey, `{"messages":[{"role":"user","content":"x"}],"model":"fake/m"}`)
	if got := second.Header.Get("X-Gateway-Cache"); got != "hit" {
		t.Errorf("second request: X-Gateway-Cache = %q, want hit", got)
	}
	a, _ := io.ReadAll(first.Body)
	b, _ := io.ReadAll(second.Body)
	if string(a) != string(b) || len(a) == 0 {
		t.Errorf("cached body %q differs from original %q", b, a)
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("upstream called %d times, want 1", n)
	}
}

func TestCacheMisses(t *testing.T) {
	stream := `{"model":"fake/m","stream":true,"messages":[{"role":"user","content":"x"}]}`
	cases := []struct {
		name   string
		second func(t *testing.T, url string, mr *miniredis.Miniredis) *http.Response
	}{
		{"different prompt", func(t *testing.T, url string, _ *miniredis.Miniredis) *http.Response {
			return send(t, url, testKey, `{"model":"fake/m","messages":[{"role":"user","content":"y"}]}`)
		}},
		{"different parameter", func(t *testing.T, url string, _ *miniredis.Miniredis) *http.Response {
			return send(t, url, testKey, `{"model":"fake/m","temperature":1,"messages":[{"role":"user","content":"x"}]}`)
		}},
		{"different tenant", func(t *testing.T, url string, _ *miniredis.Miniredis) *http.Response {
			return send(t, url, otherKey, fakeBody)
		}},
		{"client sent no-cache", func(t *testing.T, url string, _ *miniredis.Miniredis) *http.Response {
			return send(t, url, testKey, fakeBody, "Cache-Control", "no-cache")
		}},
		{"streamed request", func(t *testing.T, url string, _ *miniredis.Miniredis) *http.Response {
			return send(t, url, testKey, stream)
		}},
		{"entry expired", func(t *testing.T, url string, mr *miniredis.Miniredis) *http.Response {
			mr.FastForward(cacheTTL + time.Second)
			return send(t, url, testKey, fakeBody)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			url, calls, _, mr := countingGateway(t, 0)
			send(t, url, testKey, fakeBody)
			resp := tc.second(t, url, mr)
			if got := resp.Header.Get("X-Gateway-Cache"); got == "hit" {
				t.Error("served from cache, want a fresh upstream call")
			}
			if n := calls.Load(); n != 2 {
				t.Errorf("upstream called %d times, want 2", n)
			}
		})
	}
}

func TestErrorsAreNotCached(t *testing.T) {
	url, calls, status, _ := countingGateway(t, 0)
	status.Store(400)
	if resp := send(t, url, testKey, fakeBody); resp.StatusCode != 400 {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	status.Store(200)
	resp := send(t, url, testKey, fakeBody)
	if resp.StatusCode != 200 || resp.Header.Get("X-Gateway-Cache") != "miss" {
		t.Errorf("status = %d cache = %q, want a fresh 200", resp.StatusCode, resp.Header.Get("X-Gateway-Cache"))
	}
	if n := calls.Load(); n != 2 {
		t.Errorf("upstream called %d times, want 2", n)
	}
}

// Twenty clients ask the same new question at the same moment. Only one
// upstream call should be made; the rest wait for and share its result.
func TestConcurrentMissesShareOneUpstreamCall(t *testing.T) {
	url, calls, _, _ := countingGateway(t, 200*time.Millisecond)
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			if resp := send(t, url, testKey, fakeBody); resp.StatusCode != 200 {
				t.Errorf("status = %d", resp.StatusCode)
			}
		})
	}
	wg.Wait()
	if n := calls.Load(); n != 1 {
		t.Errorf("upstream called %d times for 20 identical concurrent requests, want 1", n)
	}
}

func TestCacheDownStillServes(t *testing.T) {
	url, calls, _, mr := countingGateway(t, 0)
	mr.Close()
	for range 2 {
		if resp := send(t, url, testKey, fakeBody); resp.StatusCode != 200 {
			t.Fatalf("status = %d with Redis down, want 200", resp.StatusCode)
		}
	}
	if n := calls.Load(); n != 2 {
		t.Errorf("upstream called %d times, want 2 (no cache available)", n)
	}
}
