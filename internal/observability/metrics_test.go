package observability

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/aakashsyadav1999/llmgate/internal/usage"
)

func scrape(m *Metrics) string {
	r := httptest.NewRecorder()
	m.Handler().ServeHTTP(r, httptest.NewRequest("GET", "/metrics", nil))
	return r.Body.String()
}

func TestAccountingAndPrivacy(t *testing.T) {
	m := New(map[string]bool{"secret-client-key": true}, map[string]map[string]usage.Price{"*": {"demo": {Input: 2, Output: 5}}})
	o := &usage.Observer{Found: true, Tokens: usage.Tokens{Input: 1000000, Output: 200000}}
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() { m.Account("secret-client-key", "a", "demo", o) })
	}
	wg.Wait()
	m.Account("secret-client-key", "a", "untrusted-model-name", o)
	m.Account("secret-client-key", "a", "demo", new(usage.Observer))
	m.Cache("hit")
	m.Reject("secret-client-key")
	body := scrape(m)
	for _, want := range []string{`kind="input",model="demo",upstream="a"} 1e+07`, `model="demo",upstream="a"} 30`, `result="missing"`, `result="unpriced"`, `llmgate_cache_requests_total{result="hit"} 1`, `llmgate_rate_limit_rejections_total{`} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %s in %s", want, body)
		}
	}
	if strings.Contains(body, "secret-client-key") || strings.Contains(body, "untrusted-model-name") {
		t.Fatal("unbounded or secret label")
	}
}

func TestMiddlewareStatusCardinalityAndStreaming(t *testing.T) {
	m := New(nil, nil)
	h := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("data: ok\n\n"))
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Error(err)
		}
		w.WriteHeader(500)
	}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("CUSTOM", "/arbitrary-unique-value", nil))
	if !w.Flushed || w.Code != 200 {
		t.Fatal("stream/status broken")
	}
	body := scrape(m)
	if !strings.Contains(body, `llmgate_requests_total{method="other",route="other",status="200"} 1`) || strings.Contains(body, "arbitrary-unique-value") {
		t.Fatal(body)
	}
}
