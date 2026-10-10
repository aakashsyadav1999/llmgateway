package tests

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aakashsyadav1999/llmgate/internal/proxy"
	"github.com/aakashsyadav1999/llmgate/internal/resilience"
)

// ---- Breaker state machine ----

func TestBreaker_TripsAfterThresholdAndFailsFast(t *testing.T) {
	b := resilience.NewBreaker(3, time.Hour, nil)

	for i := 0; i < 3; i++ {
		if !b.Allow() {
			t.Fatalf("request %d rejected while closed", i+1)
		}
		b.Record(resilience.Failure)
	}
	if b.State() != resilience.Open {
		t.Fatalf("state = %v after 3 failures, want open", b.State())
	}
	if b.Allow() {
		t.Error("open breaker should reject requests")
	}
	if b.RetryIn() <= 0 {
		t.Error("RetryIn should be positive while open")
	}
}

func TestBreaker_SuccessResetsFailureCount(t *testing.T) {
	b := resilience.NewBreaker(3, time.Hour, nil)
	for _, o := range []resilience.Outcome{resilience.Failure, resilience.Failure, resilience.Success, resilience.Failure, resilience.Failure} {
		b.Allow()
		b.Record(o)
	}
	if b.State() != resilience.Closed {
		t.Errorf("state = %v; failures must be consecutive to trip", b.State())
	}
}

func TestBreaker_HalfOpenAllowsOneProbe(t *testing.T) {
	b := resilience.NewBreaker(1, 20*time.Millisecond, nil)
	b.Allow()
	b.Record(resilience.Failure)
	time.Sleep(30 * time.Millisecond)

	if !b.Allow() {
		t.Fatal("after cooldown the first request should be let through as a probe")
	}
	if b.State() != resilience.HalfOpen {
		t.Fatalf("state = %v, want half-open", b.State())
	}
	if b.Allow() {
		t.Error("a second request must not get through while the probe is in flight")
	}
}

func TestBreaker_ProbeSuccessCloses(t *testing.T) {
	b := resilience.NewBreaker(1, 20*time.Millisecond, nil)
	b.Allow()
	b.Record(resilience.Failure)
	time.Sleep(30 * time.Millisecond)

	b.Allow()
	b.Record(resilience.Success)
	if b.State() != resilience.Closed || !b.Allow() {
		t.Errorf("state = %v; a successful probe should close the breaker", b.State())
	}
}

func TestBreaker_ProbeFailureReopens(t *testing.T) {
	b := resilience.NewBreaker(1, 20*time.Millisecond, nil)
	b.Allow()
	b.Record(resilience.Failure)
	time.Sleep(30 * time.Millisecond)

	b.Allow()
	b.Record(resilience.Failure)
	if b.State() != resilience.Open || b.Allow() {
		t.Errorf("state = %v; a failed probe should re-open the breaker", b.State())
	}
}

func TestBreaker_NeutralProbeReleasesSlot(t *testing.T) {
	b := resilience.NewBreaker(1, 20*time.Millisecond, nil)
	b.Allow()
	b.Record(resilience.Failure)
	time.Sleep(30 * time.Millisecond)

	b.Allow()
	b.Record(resilience.Neutral) // e.g. the probing client hung up
	if !b.Allow() {
		t.Error("after a neutral probe the next request should become the probe, not be stuck rejected")
	}
}

// ---- Backoff ----

func TestRetryPolicy_BackoffIsJitteredAndCapped(t *testing.T) {
	p := resilience.RetryPolicy{MaxAttempts: 10, BaseDelay: 100 * time.Millisecond, MaxDelay: 400 * time.Millisecond}
	ceilings := map[int]time.Duration{1: 100 * time.Millisecond, 2: 200 * time.Millisecond, 3: 400 * time.Millisecond, 8: 400 * time.Millisecond}

	for attempt, ceiling := range ceilings {
		seen := map[time.Duration]bool{}
		for i := 0; i < 200; i++ {
			d, ok := p.Wait(attempt, "")
			if !ok || d < 0 || d > ceiling {
				t.Fatalf("attempt %d: wait %v (ok=%v), want within [0, %v]", attempt, d, ok, ceiling)
			}
			seen[d] = true
		}
		if len(seen) < 10 {
			t.Errorf("attempt %d: only %d distinct waits; backoff should be jittered", attempt, len(seen))
		}
	}
}

func TestRetryPolicy_HonoursRetryAfter(t *testing.T) {
	p := resilience.RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 4 * time.Second}

	if d, ok := p.Wait(1, "2"); !ok || d != 2*time.Second {
		t.Errorf("Retry-After 2: got %v ok=%v, want 2s and retry", d, ok)
	}
	if _, ok := p.Wait(1, "60"); ok {
		t.Error("Retry-After 60 exceeds MaxDelay: should not retry, client should see it")
	}
}

// ---- End to end: flaky and dead upstreams ----

// scriptedUpstream answers with statuses[i] on call i, then repeats the
// last one. 200s return body.
func scriptedUpstream(t *testing.T, body string, statuses ...int) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := int(calls.Add(1)) - 1
		status := statuses[min(n, len(statuses)-1)]
		if status == http.StatusTooManyRequests {
			w.Header().Set("Retry-After", "0")
		}
		w.WriteHeader(status)
		if status == 200 {
			io.WriteString(w, body)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

func resilientGateway(t *testing.T, upstreamURL string, b *resilience.Breaker) *httptest.Server {
	t.Helper()
	u, _ := url.Parse(upstreamURL)
	h := proxy.NewHandler(u, "upstream-secret", 100, 5*time.Second, discardLogger()).
		WithResilience(resilience.RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 10 * time.Millisecond}, b)
	gw := httptest.NewServer(h)
	t.Cleanup(gw.Close)
	return gw
}

func call(t *testing.T, gw *httptest.Server) (*http.Response, string) {
	t.Helper()
	resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"model":"m"}`))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, string(b)
}

func TestRetry_RecoversFromTransientErrors(t *testing.T) {
	up, calls := scriptedUpstream(t, `{"ok":true}`, 503, 502, 200)
	gw := resilientGateway(t, up.URL, nil)

	resp, body := call(t, gw)
	if resp.StatusCode != 200 || body != `{"ok":true}` {
		t.Errorf("got %d %q, want 200 after retries", resp.StatusCode, body)
	}
	if calls.Load() != 3 {
		t.Errorf("upstream called %d times, want 3", calls.Load())
	}
}

func TestRetry_RetriesRateLimitWithRetryAfter(t *testing.T) {
	up, calls := scriptedUpstream(t, `{"ok":true}`, 429, 200)
	gw := resilientGateway(t, up.URL, nil)

	if resp, _ := call(t, gw); resp.StatusCode != 200 || calls.Load() != 2 {
		t.Errorf("got %d after %d calls, want 200 after 2", resp.StatusCode, calls.Load())
	}
}

func TestRetry_GivesUpAfterMaxAttempts(t *testing.T) {
	up, calls := scriptedUpstream(t, "", 503)
	gw := resilientGateway(t, up.URL, nil)

	if resp, _ := call(t, gw); resp.StatusCode != 503 {
		t.Errorf("status = %d, want the upstream's final 503", resp.StatusCode)
	}
	if calls.Load() != 3 {
		t.Errorf("upstream called %d times, want 3 (MaxAttempts)", calls.Load())
	}
}

func TestRetry_ClientErrorsNotRetried(t *testing.T) {
	up, calls := scriptedUpstream(t, "", 400)
	gw := resilientGateway(t, up.URL, nil)

	call(t, gw)
	if calls.Load() != 1 {
		t.Errorf("upstream called %d times; a 400 will fail the same way again, so retry is wasted", calls.Load())
	}
}

func TestRetry_TimeoutsNotRetried(t *testing.T) {
	var calls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		select { // "still generating" past the gateway's header timeout
		case <-time.After(300 * time.Millisecond):
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(up.Close)

	u, _ := url.Parse(up.URL)
	h := proxy.NewHandler(u, "k", 100, 50*time.Millisecond, discardLogger()).
		WithResilience(resilience.RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 10 * time.Millisecond}, nil)
	gw := httptest.NewServer(h)
	t.Cleanup(gw.Close)

	if resp, _ := call(t, gw); resp.StatusCode != http.StatusBadGateway {
		t.Errorf("status %d, want 502", resp.StatusCode)
	}
	if calls.Load() != 1 {
		t.Errorf("upstream called %d times; a timed-out request may still be running (and billed) upstream", calls.Load())
	}
}

func TestRetry_StreamSucceedsAfterFailure(t *testing.T) {
	sse := "data: {\"delta\":\"hi\"}\n\ndata: [DONE]\n\n"
	var calls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(502)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, sse)
	}))
	t.Cleanup(up.Close)
	gw := resilientGateway(t, up.URL, nil)

	resp, body := call(t, gw)
	if resp.StatusCode != 200 || body != sse {
		t.Errorf("got %d %q, want the full stream after one retry", resp.StatusCode, body)
	}
}

func TestBreaker_DeadUpstreamFailsFast(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close() // connections to it are now refused

	b := resilience.NewBreaker(3, time.Hour, nil)
	gw := resilientGateway(t, deadURL, b)

	// One request = 3 failed attempts = threshold reached.
	if resp, _ := call(t, gw); resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("first request: status %d, want 502", resp.StatusCode)
	}
	if b.State() != resilience.Open {
		t.Fatalf("breaker %v after 3 failures, want open", b.State())
	}

	start := time.Now()
	resp, _ := call(t, gw)
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") == "" {
		t.Errorf("open breaker: status %d Retry-After %q, want 503 with Retry-After",
			resp.StatusCode, resp.Header.Get("Retry-After"))
	}
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Errorf("open breaker took %v; it should fail fast", d)
	}
}

func TestBreaker_RecoversWhenUpstreamReturns(t *testing.T) {
	var healthy atomic.Bool
	var calls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if !healthy.Load() {
			w.WriteHeader(500)
			return
		}
		io.WriteString(w, `{"ok":true}`)
	}))
	t.Cleanup(up.Close)

	b := resilience.NewBreaker(3, 50*time.Millisecond, nil)
	gw := resilientGateway(t, up.URL, b)

	call(t, gw) // 3 x 500 -> open
	if resp, _ := call(t, gw); resp.StatusCode != 503 {
		t.Fatalf("status %d while open, want 503", resp.StatusCode)
	}
	before := calls.Load()

	healthy.Store(true)
	time.Sleep(60 * time.Millisecond) // cooldown over

	if resp, _ := call(t, gw); resp.StatusCode != 200 {
		t.Errorf("status %d after recovery, want 200", resp.StatusCode)
	}
	if b.State() != resilience.Closed {
		t.Errorf("breaker %v after successful probe, want closed", b.State())
	}
	if calls.Load() != before+1 {
		t.Errorf("upstream got %d calls after recovery, want 1 (the probe)", calls.Load()-before)
	}
}
