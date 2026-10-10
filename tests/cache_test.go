package tests

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aakashsyadav1999/llmgate/internal/cache"
	"github.com/aakashsyadav1999/llmgate/internal/middleware"
	"github.com/aakashsyadav1999/llmgate/internal/proxy"
	"github.com/redis/go-redis/v9"
)

const chatPath = "/v1/chat/completions"

// ---- Key: what is cacheable, and what counts as "the same request" ----

func TestCacheKey_Policy(t *testing.T) {
	cases := []struct {
		name, method, path, body string
		want                     bool
	}{
		{"temperature 0", "POST", chatPath, `{"model":"m","temperature":0}`, true},
		{"temperature 0.0", "POST", chatPath, `{"model":"m","temperature":0.0}`, true},
		{"temperature unset (OpenAI default is 1)", "POST", chatPath, `{"model":"m"}`, false},
		{"temperature 0.7", "POST", chatPath, `{"model":"m","temperature":0.7}`, false},
		{"n=1", "POST", chatPath, `{"model":"m","temperature":0,"n":1}`, true},
		{"n=2 wants different answers", "POST", chatPath, `{"model":"m","temperature":0,"n":2}`, false},
		{"embeddings always", "POST", "/v1/embeddings", `{"model":"e","input":"hi"}`, true},
		{"other endpoints never", "POST", "/v1/images/generations", `{"prompt":"x"}`, false},
		{"GET never", "GET", "/v1/models", ``, false},
		{"invalid JSON", "POST", chatPath, `{nope`, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, got := cache.Key("client", c.method, c.path, []byte(c.body))
			if got != c.want {
				t.Errorf("cacheable = %v, want %v", got, c.want)
			}
		})
	}
}

func TestCacheKey_IgnoresFieldOrderAndWhitespace(t *testing.T) {
	k1, _ := cache.Key("c", "POST", chatPath, []byte(`{"model":"m","temperature":0,"messages":[{"role":"user","content":"hi"}]}`))
	k2, _ := cache.Key("c", "POST", chatPath, []byte(`{ "temperature": 0,
		"messages": [ {"content":"hi", "role":"user"} ], "model": "m" }`))
	if k1 != k2 {
		t.Error("same request with different field order/whitespace should share a key")
	}
}

func TestCacheKey_DiffersWhenOutputCouldDiffer(t *testing.T) {
	base := `{"model":"m","temperature":0,"messages":[{"role":"user","content":"hi"}]}`
	k, _ := cache.Key("c", "POST", chatPath, []byte(base))

	others := map[string][3]string{
		"other client":     {"other", chatPath, base},
		"other path":       {"c", "/v1/embeddings", base},
		"other max_tokens": {"c", chatPath, `{"model":"m","temperature":0,"max_tokens":5,"messages":[{"role":"user","content":"hi"}]}`},
		"stream vs not":    {"c", chatPath, `{"model":"m","temperature":0,"stream":true,"messages":[{"role":"user","content":"hi"}]}`},
	}
	for name, o := range others {
		if k2, _ := cache.Key(o[0], "POST", o[1], []byte(o[2])); k2 == k {
			t.Errorf("%s: should get a different key", name)
		}
	}
}

// ---- End to end through the handler, against a real local Redis ----

// fakeUpstream counts calls and answers with the given content type/body.
func fakeUpstream(t *testing.T, status int, contentType, body string) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", contentType)
		w.WriteHeader(status)
		io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

// cachingGateway is the handler behind Auth (which supplies the client key
// the cache is scoped by), using rdb for caching.
func cachingGateway(t *testing.T, upstream *httptest.Server, rdb *redis.Client, clientKeys ...string) *httptest.Server {
	t.Helper()
	u, _ := url.Parse(upstream.URL)
	h := proxy.NewHandler(u, "upstream-secret", 100, 5*time.Second, discardLogger()).
		WithCache(cache.New(rdb, 10*time.Second, time.Minute, discardLogger()))

	keys := map[string]bool{}
	for _, k := range clientKeys {
		keys[k] = true
	}
	gw := httptest.NewServer(middleware.Auth(keys)(h))
	t.Cleanup(gw.Close)
	return gw
}

// testClient returns a client key unique to this test run, so entries
// cached by earlier runs can never produce a false HIT.
func testClient(t *testing.T, name string) string {
	return fmt.Sprintf("%s-%s-%d", t.Name(), name, time.Now().UnixNano())
}

func post(t *testing.T, gw *httptest.Server, clientKey, body string, header ...string) (*http.Response, string) {
	t.Helper()
	req, _ := http.NewRequest("POST", gw.URL+chatPath, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+clientKey)
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, string(b)
}

const deterministic = `{"model":"m","temperature":0,"messages":[{"role":"user","content":"hi"}]}`

func TestCache_SecondIdenticalRequestIsHit(t *testing.T) {
	rdb := redisClient(t)
	up, calls := fakeUpstream(t, 200, "application/json", `{"answer":42}`)
	client := testClient(t, "a")
	gw := cachingGateway(t, up, rdb, client)

	r1, b1 := post(t, gw, client, deterministic)
	r2, b2 := post(t, gw, client, deterministic)

	if r1.Header.Get("X-Cache") != "MISS" || r2.Header.Get("X-Cache") != "HIT" {
		t.Errorf("X-Cache = %q then %q, want MISS then HIT", r1.Header.Get("X-Cache"), r2.Header.Get("X-Cache"))
	}
	if b2 != b1 || r2.Header.Get("Content-Type") != "application/json" {
		t.Errorf("cached response differs: %q (%s) vs %q", b2, r2.Header.Get("Content-Type"), b1)
	}
	if calls.Load() != 1 {
		t.Errorf("upstream called %d times, want 1", calls.Load())
	}
}

func TestCache_StreamIsRecordedAndReplayed(t *testing.T) {
	rdb := redisClient(t)
	sse := "data: {\"delta\":\"Hel\"}\n\ndata: {\"delta\":\"lo\"}\n\ndata: [DONE]\n\n"
	up, calls := fakeUpstream(t, 200, "text/event-stream", sse)
	client := testClient(t, "a")
	gw := cachingGateway(t, up, rdb, client)
	streamReq := `{"model":"m","temperature":0,"stream":true,"messages":[]}`

	post(t, gw, client, streamReq)
	r2, b2 := post(t, gw, client, streamReq)

	if r2.Header.Get("X-Cache") != "HIT" || b2 != sse {
		t.Errorf("replay: X-Cache=%q body=%q, want HIT and the original stream", r2.Header.Get("X-Cache"), b2)
	}
	if ct := r2.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q, want text/event-stream", ct)
	}
	if calls.Load() != 1 {
		t.Errorf("upstream called %d times, want 1", calls.Load())
	}
}

func TestCache_TruncatedStreamNotStored(t *testing.T) {
	rdb := redisClient(t)
	up, calls := fakeUpstream(t, 200, "text/event-stream", "data: {\"delta\":\"Hel\"}\n\n") // no [DONE]
	client := testClient(t, "a")
	gw := cachingGateway(t, up, rdb, client)
	streamReq := `{"model":"m","temperature":0,"stream":true,"messages":[]}`

	post(t, gw, client, streamReq)
	post(t, gw, client, streamReq)

	if calls.Load() != 2 {
		t.Errorf("upstream called %d times, want 2: a cut-off stream must not be cached", calls.Load())
	}
}

func TestCache_ErrorsNotStored(t *testing.T) {
	rdb := redisClient(t)
	up, calls := fakeUpstream(t, 500, "application/json", `{"error":"boom"}`)
	client := testClient(t, "a")
	gw := cachingGateway(t, up, rdb, client)

	post(t, gw, client, deterministic)
	post(t, gw, client, deterministic)

	if calls.Load() != 2 {
		t.Errorf("upstream called %d times, want 2: errors must not be cached", calls.Load())
	}
}

func TestCache_ScopedPerClient(t *testing.T) {
	rdb := redisClient(t)
	up, calls := fakeUpstream(t, 200, "application/json", `{"answer":42}`)
	a, b := testClient(t, "a"), testClient(t, "b")
	gw := cachingGateway(t, up, rdb, a, b)

	post(t, gw, a, deterministic)
	r, _ := post(t, gw, b, deterministic)

	if r.Header.Get("X-Cache") != "MISS" || calls.Load() != 2 {
		t.Errorf("client b got X-Cache=%q (upstream calls %d); want MISS: caches are per client", r.Header.Get("X-Cache"), calls.Load())
	}
}

func TestCache_NonDeterministicBypasses(t *testing.T) {
	rdb := redisClient(t)
	up, calls := fakeUpstream(t, 200, "application/json", `{"answer":42}`)
	client := testClient(t, "a")
	gw := cachingGateway(t, up, rdb, client)
	noTemp := `{"model":"m","messages":[]}`

	r, _ := post(t, gw, client, noTemp)
	post(t, gw, client, noTemp)

	if r.Header.Get("X-Cache") != "BYPASS" || calls.Load() != 2 {
		t.Errorf("X-Cache=%q, upstream calls %d; want BYPASS and 2", r.Header.Get("X-Cache"), calls.Load())
	}
}

func TestCache_NoCacheSkipsReadButRefreshes(t *testing.T) {
	rdb := redisClient(t)
	up, calls := fakeUpstream(t, 200, "application/json", `{"answer":42}`)
	client := testClient(t, "a")
	gw := cachingGateway(t, up, rdb, client)

	r1, _ := post(t, gw, client, deterministic, "Cache-Control", "no-cache")
	r2, _ := post(t, gw, client, deterministic)

	if r1.Header.Get("X-Cache") != "BYPASS" {
		t.Errorf("no-cache request: X-Cache=%q, want BYPASS", r1.Header.Get("X-Cache"))
	}
	if r2.Header.Get("X-Cache") != "HIT" || calls.Load() != 1 {
		t.Errorf("next request: X-Cache=%q, calls %d; want HIT and 1 (no-cache still stores)", r2.Header.Get("X-Cache"), calls.Load())
	}
}

func TestCache_RedisDownStillServes(t *testing.T) {
	// Port 1 refuses connections: Redis "stopped".
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", ContextTimeoutEnabled: true, MaxRetries: -1})
	t.Cleanup(func() { rdb.Close() })
	up, calls := fakeUpstream(t, 200, "application/json", `{"answer":42}`)
	gw := cachingGateway(t, up, rdb, "c")

	for i := 0; i < 2; i++ {
		if r, body := post(t, gw, "c", deterministic); r.StatusCode != 200 || body != `{"answer":42}` {
			t.Fatalf("request %d: status %d body %q; a Redis outage must not fail requests", i+1, r.StatusCode, body)
		}
	}
	if calls.Load() != 2 {
		t.Errorf("upstream called %d times, want 2", calls.Load())
	}
}
