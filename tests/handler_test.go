package tests

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aakashsyadav1999/llmgate/internal/proxy"
)

func newGateway(t *testing.T, upstream *httptest.Server) *httptest.Server {
	t.Helper()
	u, _ := url.Parse(upstream.URL)
	gw := httptest.NewServer(proxy.NewHandler(u, "upstream-secret", discardLogger()))
	t.Cleanup(gw.Close)
	return gw
}

func TestHandler_ForwardsBodyAndReplacesAuth(t *testing.T) {
	var gotAuth, gotBody, gotPath string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotAuth, gotBody, gotPath = r.Header.Get("Authorization"), string(b), r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()
	gw := newGateway(t, upstream)

	req, _ := http.NewRequest("POST", gw.URL+"/v1/chat/completions", strings.NewReader(`{"model":"m"}`))
	req.Header.Set("Authorization", "Bearer client-key")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)

	if string(out) != `{"ok":true}` {
		t.Errorf("body = %q", out)
	}
	if gotAuth != "Bearer upstream-secret" {
		t.Errorf("upstream saw Authorization %q", gotAuth)
	}
	if gotBody != `{"model":"m"}` {
		t.Errorf("upstream saw body %q", gotBody)
	}
	if gotPath != "/v1/chat/completions" {
		t.Errorf("upstream saw path %q", gotPath)
	}
}

func TestHandler_StreamsIncrementally(t *testing.T) {
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte("data: one\n\n"))
		w.(http.Flusher).Flush()
		select {
		case <-release:
			w.Write([]byte("data: two\n\n"))
		case <-r.Context().Done():
		}
	}))
	defer upstream.Close()
	gw := newGateway(t, upstream)

	// If the gateway buffered, the first read would block until the
	// upstream finished, and the upstream is waiting on us: a deadlock
	// that the timeout turns into a failure.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "POST", gw.URL+"/v1/chat/completions", strings.NewReader(`{"stream":true}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("no response before upstream finished (buffering?): %v", err)
	}
	defer resp.Body.Close()

	first := make([]byte, len("data: one\n\n"))
	if _, err := io.ReadFull(resp.Body, first); err != nil {
		t.Fatalf("first chunk did not arrive while upstream was still open: %v", err)
	}
	if string(first) != "data: one\n\n" {
		t.Errorf("first chunk = %q", first)
	}

	close(release)
	rest, _ := io.ReadAll(resp.Body)
	if string(rest) != "data: two\n\n" {
		t.Errorf("rest = %q", rest)
	}
}

func TestHandler_ClientDisconnectCancelsUpstream(t *testing.T) {
	upstreamDone := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte("data: hi\n\n"))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(upstreamDone)
	}))
	defer upstream.Close()
	gw := newGateway(t, upstream)

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, "POST", gw.URL+"/v1/chat/completions", strings.NewReader(`{"stream":true}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	io.ReadFull(resp.Body, make([]byte, len("data: hi\n\n")))

	cancel() // the client hangs up mid-stream

	select {
	case <-upstreamDone:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream request was not cancelled after the client disconnected")
	}
}

func TestHandler_UpstreamDown_Returns502(t *testing.T) {
	upstream := httptest.NewServer(http.NotFoundHandler())
	gw := newGateway(t, upstream)
	upstream.Close() // nothing is listening on the upstream address any more

	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", resp.StatusCode)
	}
}
