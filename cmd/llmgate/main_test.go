package main

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aakashsyadav1999/llmgate/internal/config"
)

func TestFullChainStreamingMetricsAndHealth(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":4}}\n\ndata: [DONE]\n\n")
	}))
	defer up.Close()
	env := map[string]string{"LLMGATE_UPSTREAM_URL": up.URL, "LLMGATE_UPSTREAM_API_KEY": "up-secret", "LLMGATE_CLIENT_API_KEYS": "client-secret", "LLMGATE_RATE_LIMIT_BURST": "1", "LLMGATE_RATE_LIMIT_PER_SECOND": "0.001", "LLMGATE_MODEL_PRICES": `{"default":{"demo":{"input_usd_per_million":2,"output_usd_per_million":5}}}`}
	cfg, err := config.Load(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	h := newHandler(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	for i := range 2 {
		r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"demo","stream":true}`))
		r.Header.Set("Authorization", "Bearer client-secret")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if i == 0 && (w.Code != 200 || !w.Flushed || !strings.Contains(w.Body.String(), "[DONE]")) {
			t.Fatalf("stream %d: %s", w.Code, w.Body.String())
		}
		if i == 1 && w.Code != 429 {
			t.Fatalf("rate limit %d", w.Code)
		}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	for _, name := range []string{"llmgate_tokens_total", "llmgate_estimated_cost_usd_total", "llmgate_rate_limit_rejections_total", "llmgate_request_duration_seconds_bucket"} {
		if !strings.Contains(w.Body.String(), name) {
			t.Fatal("missing " + name)
		}
	}
	if strings.Contains(w.Body.String(), "client-secret") || strings.Contains(w.Body.String(), "up-secret") {
		t.Fatal("secret exposed")
	}
	srv := httptest.NewServer(h)
	defer srv.Close()
	if err := healthcheck(strings.TrimPrefix(srv.URL, "http://")); err != nil {
		t.Fatal(err)
	}
}

func TestPriceValidation(t *testing.T) {
	for _, raw := range []string{`{"unknown":{"demo":{}}}`, `{"default":{"demo":{"input_usd_per_million":-1}}}`, `{"default":{"demo":{"typo":1}}}`, `{} {}`} {
		_, err := config.Load(func(k string) string {
			switch k {
			case "LLMGATE_UPSTREAM_API_KEY", "LLMGATE_CLIENT_API_KEYS":
				return "test"
			case "LLMGATE_MODEL_PRICES":
				return raw
			}
			return ""
		})
		if err == nil {
			t.Fatal("accepted " + raw)
		}
	}
}
