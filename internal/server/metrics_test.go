package server_test

import (
	"net/http"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/Ani-tem/llm-gateway/internal/metrics"
	"github.com/Ani-tem/llm-gateway/internal/provider"
)

// Metrics are process-wide, so tests compare before and after rather than
// asserting absolute values.
func TestMetricsCountRequestsCacheAndUpstreams(t *testing.T) {
	down := fakeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", 503)
	})
	gw := newGateway(t, map[string]provider.Provider{"fake": down, "mock": &provider.Mock{}})

	miss := metrics.Requests.WithLabelValues("test", "200", "miss")
	hit := metrics.Requests.WithLabelValues("test", "200", "hit")
	unauth := metrics.Requests.WithLabelValues("", "401", "none")
	failed := metrics.UpstreamAttempts.WithLabelValues("fake", "failed")
	ok := metrics.UpstreamAttempts.WithLabelValues("mock", "ok")
	prompt := metrics.Tokens.WithLabelValues("prompt")
	before := []float64{testutil.ToFloat64(miss), testutil.ToFloat64(hit), testutil.ToFloat64(unauth),
		testutil.ToFloat64(failed), testutil.ToFloat64(ok), testutil.ToFloat64(prompt)}

	body := `{"model":"fallback","messages":[{"role":"user","content":"one two"}]}`
	post(t, gw.URL, body) // fake fails twice, mock serves: a miss
	post(t, gw.URL, body) // served from cache: no upstream call at all
	postWithKey(t, gw.URL, "wrong-key", body)

	after := []float64{testutil.ToFloat64(miss), testutil.ToFloat64(hit), testutil.ToFloat64(unauth),
		testutil.ToFloat64(failed), testutil.ToFloat64(ok), testutil.ToFloat64(prompt)}
	want := []float64{1, 1, 1, 2, 1, 2} // prompt tokens: 2 words, counted once (the hit is free)
	names := []string{"miss", "hit", "unauthenticated", "fake failed attempts", "mock ok attempts", "prompt tokens"}
	for i := range want {
		if got := after[i] - before[i]; got != want[i] {
			t.Errorf("%s: increased by %v, want %v", names[i], got, want[i])
		}
	}
	if got := testutil.ToFloat64(metrics.BreakerOpen.WithLabelValues("fake")); got != 0 {
		t.Errorf("breaker gauge for fake = %v, want 0 (only 2 failures so far)", got)
	}
}
