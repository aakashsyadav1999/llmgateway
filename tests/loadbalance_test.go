package tests

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aakashsyadav1999/llmgate/internal/cache"
	"github.com/aakashsyadav1999/llmgate/internal/middleware"
	"github.com/aakashsyadav1999/llmgate/internal/proxy"
	"github.com/aakashsyadav1999/llmgate/internal/resilience"
)

func poolUpstream(t *testing.T, server *httptest.Server, name, key string, breaker *resilience.Breaker) proxy.Upstream {
	t.Helper()
	u, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	return proxy.Upstream{Name: name, URL: u, APIKey: key, Breaker: breaker}
}

func poolGateway(t *testing.T, attempts int, timeout time.Duration, upstreams ...proxy.Upstream) *httptest.Server {
	t.Helper()
	h := proxy.NewPoolHandler(upstreams, 100, timeout, discardLogger()).WithRetryPolicy(
		resilience.RetryPolicy{MaxAttempts: attempts, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond},
	)
	gw := httptest.NewServer(h)
	t.Cleanup(gw.Close)
	return gw
}

func callPool(gw *httptest.Server) (int, string, http.Header, error) {
	req, err := http.NewRequest("POST", gw.URL+chatPath+"?trace=yes", strings.NewReader(`{"model":"m","messages":[]}`))
	if err != nil {
		return 0, "", nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer client-secret")
	resp, err := gw.Client().Do(req)
	if err != nil {
		return 0, "", nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body), resp.Header, err
}

func TestPool_RoundRobinWithSeparateCredentials(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		fmt.Fprintf(w, "%s|%s|%s|%s", r.Header.Get("Authorization"), r.URL.Path, r.URL.RawQuery, body)
	}))
	t.Cleanup(server.Close)
	gw := poolGateway(t, 3, time.Second,
		poolUpstream(t, server, "a", "key-a", nil),
		poolUpstream(t, server, "b", "key-b", nil))
	for _, key := range []string{"key-a", "key-b", "key-a", "key-b"} {
		status, body, _, err := callPool(gw)
		want := "Bearer " + key + `|/v1/chat/completions|trace=yes|{"model":"m","messages":[]}`
		if err != nil || status != 200 || body != want {
			t.Fatalf("got %d %q (%v), want 200 %q", status, body, err, want)
		}
	}
}

func TestPool_ConcurrentRoundRobinIsBalanced(t *testing.T) {
	a, callsA := scriptedUpstream(t, "a", 200)
	b, callsB := scriptedUpstream(t, "b", 200)
	gw := poolGateway(t, 3, time.Second,
		poolUpstream(t, a, "a", "key-a", nil), poolUpstream(t, b, "b", "key-b", nil))
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, _, _, err := callPool(gw)
			if err != nil || status != 200 {
				t.Errorf("request = %d (%v), want 200", status, err)
			}
		}()
	}
	wg.Wait()
	if callsA.Load() != 20 || callsB.Load() != 20 {
		t.Fatalf("distribution = %d/%d, want 20/20", callsA.Load(), callsB.Load())
	}
}

func TestPool_FailsOverAndSkipsTrippedUpstream(t *testing.T) {
	a, callsA := scriptedUpstream(t, "bad", 503)
	b, callsB := scriptedUpstream(t, "good", 200)
	breakerA := resilience.NewBreaker(1, time.Minute, nil)
	breakerB := resilience.NewBreaker(1, time.Minute, nil)
	gw := poolGateway(t, 3, time.Second,
		poolUpstream(t, a, "a", "key-a", breakerA), poolUpstream(t, b, "b", "key-b", breakerB))
	for i := 0; i < 3; i++ {
		status, body, _, err := callPool(gw)
		if err != nil || status != 200 || body != "good" {
			t.Fatalf("failover = %d %q (%v), want 200 good", status, body, err)
		}
	}
	if callsA.Load() != 1 || callsB.Load() != 3 {
		t.Fatalf("calls = %d/%d, want 1/3", callsA.Load(), callsB.Load())
	}
	if breakerA.State() != resilience.Open || breakerB.State() != resilience.Closed {
		t.Fatal("one failed upstream should not trip another upstream's breaker")
	}
}

func TestPool_FailsOverOnConnectionFailure(t *testing.T) {
	a := httptest.NewServer(http.NotFoundHandler())
	a.Close()
	b, callsB := scriptedUpstream(t, "good", 200)
	gw := poolGateway(t, 3, time.Second,
		poolUpstream(t, a, "a", "key-a", resilience.NewBreaker(1, time.Minute, nil)),
		poolUpstream(t, b, "b", "key-b", nil))
	status, body, _, err := callPool(gw)
	if err != nil || status != 200 || body != "good" || callsB.Load() != 1 {
		t.Fatalf("connection failover = %d %q (%v), calls %d", status, body, err, callsB.Load())
	}
}

func TestPool_FailoverPreservesBodyAndChangesCredentials(t *testing.T) {
	type forwarded struct{ auth, body string }
	seen := make(chan forwarded, 2)
	makeServer := func(status int) *httptest.Server {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			seen <- forwarded{auth: r.Header.Get("Authorization"), body: string(body)}
			w.WriteHeader(status)
		}))
		t.Cleanup(server.Close)
		return server
	}
	gw := poolGateway(t, 3, time.Second,
		poolUpstream(t, makeServer(503), "a", "key-a", nil),
		poolUpstream(t, makeServer(200), "b", "key-b", nil))
	status, _, _, err := callPool(gw)
	if err != nil || status != 200 {
		t.Fatalf("failover = %d (%v), want 200", status, err)
	}
	for _, auth := range []string{"Bearer key-a", "Bearer key-b"} {
		got := <-seen
		if got.auth != auth || got.body != `{"model":"m","messages":[]}` {
			t.Fatalf("forwarded %v, want credentials %q and the original body", got, auth)
		}
	}
}

func TestPool_AllOpenFailsWithoutUpstreamCalls(t *testing.T) {
	a, callsA := scriptedUpstream(t, "a", 200)
	b, callsB := scriptedUpstream(t, "b", 200)
	breakerA := resilience.NewBreaker(1, time.Minute, nil)
	breakerB := resilience.NewBreaker(1, time.Minute, nil)
	breakerA.Record(resilience.Failure)
	breakerB.Record(resilience.Failure)
	gw := poolGateway(t, 3, time.Second,
		poolUpstream(t, a, "a", "key-a", breakerA), poolUpstream(t, b, "b", "key-b", breakerB))
	status, _, headers, err := callPool(gw)
	if err != nil || status != 503 || headers.Get("Retry-After") == "" {
		t.Fatalf("all open = %d (%v), headers %v", status, err, headers)
	}
	if callsA.Load() != 0 || callsB.Load() != 0 {
		t.Fatal("open upstreams must not receive requests")
	}
}

func TestPool_RecoveryRejoinsRotation(t *testing.T) {
	var healthy atomic.Bool
	a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !healthy.Load() {
			w.WriteHeader(503)
			return
		}
		io.WriteString(w, "a")
	}))
	t.Cleanup(a.Close)
	b, _ := scriptedUpstream(t, "b", 200)
	breaker := resilience.NewBreaker(1, 50*time.Millisecond, nil)
	gw := poolGateway(t, 3, time.Second,
		poolUpstream(t, a, "a", "key-a", breaker), poolUpstream(t, b, "b", "key-b", nil))
	status, body, _, err := callPool(gw)
	if err != nil || status != 200 || body != "b" {
		t.Fatalf("initial failover = %d %q (%v)", status, body, err)
	}
	healthy.Store(true)
	time.Sleep(80 * time.Millisecond)
	for _, want := range []string{"a", "b", "a"} {
		status, body, _, err = callPool(gw)
		if err != nil || status != 200 || body != want {
			t.Fatalf("recovered rotation = %d %q (%v), want %q", status, body, err, want)
		}
	}
	if breaker.State() != resilience.Closed {
		t.Fatal("successful probe should close the breaker")
	}
}

func TestPool_OnlyOneHalfOpenProbe(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var callsA atomic.Int32
	a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if callsA.Add(1) == 1 {
			close(started)
		}
		select {
		case <-release:
		case <-r.Context().Done():
		}
		io.WriteString(w, "a")
	}))
	t.Cleanup(a.Close)
	b, callsB := scriptedUpstream(t, "b", 200)
	breaker := resilience.NewBreaker(1, time.Millisecond, nil)
	breaker.Record(resilience.Failure)
	time.Sleep(5 * time.Millisecond)
	gw := poolGateway(t, 3, time.Second,
		poolUpstream(t, a, "a", "key-a", breaker), poolUpstream(t, b, "b", "key-b", nil))
	finished := make(chan error, 1)
	go func() {
		status, _, _, err := callPool(gw)
		if err == nil && status != 200 {
			err = fmt.Errorf("probe status %d", status)
		}
		finished <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("half-open probe did not start")
	}
	for i := 0; i < 10; i++ {
		status, body, _, err := callPool(gw)
		if err != nil || status != 200 || body != "b" {
			t.Fatalf("request during probe = %d %q (%v)", status, body, err)
		}
	}
	once.Do(func() { close(release) })
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if callsA.Load() != 1 || callsB.Load() != 10 {
		t.Fatalf("probe calls = %d/%d, want 1/10", callsA.Load(), callsB.Load())
	}
}

func TestPool_RetryBudgetIsSharedAcrossUpstreams(t *testing.T) {
	a, callsA := scriptedUpstream(t, "bad", 503)
	b, callsB := scriptedUpstream(t, "bad", 503)
	gw := poolGateway(t, 3, time.Second,
		poolUpstream(t, a, "a", "key-a", nil), poolUpstream(t, b, "b", "key-b", nil))
	status, _, _, err := callPool(gw)
	if err != nil || status != 503 || callsA.Load()+callsB.Load() != 3 {
		t.Fatalf("got status %d (%v), total attempts %d, want 503 and 3", status, err, callsA.Load()+callsB.Load())
	}
}

func TestPool_ClientErrorsDoNotFailOver(t *testing.T) {
	a, _ := scriptedUpstream(t, "invalid request", 400)
	b, callsB := scriptedUpstream(t, "good", 200)
	gw := poolGateway(t, 3, time.Second,
		poolUpstream(t, a, "a", "key-a", nil), poolUpstream(t, b, "b", "key-b", nil))
	status, _, _, err := callPool(gw)
	if err != nil || status != 400 || callsB.Load() != 0 {
		t.Fatalf("client error = %d (%v), backup calls %d", status, err, callsB.Load())
	}
}

func TestPool_TimeoutsDoNotFailOver(t *testing.T) {
	a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
		case <-time.After(200 * time.Millisecond):
		}
	}))
	t.Cleanup(a.Close)
	b, callsB := scriptedUpstream(t, "good", 200)
	gw := poolGateway(t, 3, 20*time.Millisecond,
		poolUpstream(t, a, "a", "key-a", nil), poolUpstream(t, b, "b", "key-b", nil))
	status, _, _, err := callPool(gw)
	if err != nil || status != 502 || callsB.Load() != 0 {
		t.Fatalf("timeout = %d (%v), backup calls %d", status, err, callsB.Load())
	}
}

func TestPool_DoesNotReplayStartedStream(t *testing.T) {
	partial := "data: {\"delta\":\"hello\"}\n\n"
	a, _ := fakeUpstream(t, 200, "text/event-stream", partial)
	b, callsB := scriptedUpstream(t, "backup", 200)
	gw := poolGateway(t, 3, time.Second,
		poolUpstream(t, a, "a", "key-a", nil), poolUpstream(t, b, "b", "key-b", nil))
	status, body, _, err := callPool(gw)
	if err != nil || status != 200 || body != partial || callsB.Load() != 0 {
		t.Fatalf("stream = %d %q (%v), backup calls %d", status, body, err, callsB.Load())
	}
}

func TestPool_CacheScopedByCredentials(t *testing.T) {
	rdb := redisClient(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"upstream":%q}`, r.Header.Get("Authorization"))
	}))
	t.Cleanup(server.Close)
	clientKey := testClient(t, "pool-scope")
	for _, key := range []string{"key-a", "key-b"} {
		h := proxy.NewPoolHandler([]proxy.Upstream{poolUpstream(t, server, "provider", key, nil)},
			100, time.Second, discardLogger()).WithCache(cache.New(rdb, time.Minute, time.Second, discardLogger()))
		gw := httptest.NewServer(middleware.Auth(map[string]bool{clientKey: true})(h))
		t.Cleanup(gw.Close)
		response, body := post(t, gw, clientKey, deterministic)
		if response.Header.Get("X-Cache") != "MISS" || !strings.Contains(body, "Bearer "+key) {
			t.Fatalf("pool %s reused another pool's cache: %s %q", key, response.Header.Get("X-Cache"), body)
		}
		response, _ = post(t, gw, clientKey, deterministic)
		if response.Header.Get("X-Cache") != "HIT" {
			t.Fatal("same pool should reuse its cached response")
		}
	}
}
