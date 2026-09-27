package tests

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/aakashsyadav1999/llmgate/internal/proxy"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestProxy_ForwardsAndReplacesAuth(t *testing.T) {
	var gotAuth, gotPath string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		w.Write([]byte("hello"))
	}))
	defer upstream.Close()

	u, _ := url.Parse(upstream.URL)
	h := proxy.New(u, "upstream-secret", discardLogger())

	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader("{}"))
	req.Header.Set("Authorization", "Bearer client-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
	if rec.Body.String() != "hello" {
		t.Errorf("body = %q, want hello", rec.Body.String())
	}
	if gotAuth != "Bearer upstream-secret" {
		t.Errorf("upstream saw Authorization %q, want the gateway key", gotAuth)
	}
	if gotPath != "/v1/chat/completions" {
		t.Errorf("upstream saw path %q", gotPath)
	}
}

func TestProxy_UpstreamDown_Returns502(t *testing.T) {
	upstream := httptest.NewServer(http.NotFoundHandler())
	u, _ := url.Parse(upstream.URL)
	upstream.Close() // nothing is listening on this address any more

	h := proxy.New(u, "k", discardLogger())
	req := httptest.NewRequest("GET", "/v1/models", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", rec.Code)
	}
}
