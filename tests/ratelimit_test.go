package tests

import (
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/aakashsyadav1999/llmgate/internal/middleware"
	"github.com/redis/go-redis/v9"
)

func TestLimiter_AllowsBurstUpToCapacity(t *testing.T) {
	// Slow refill so the bucket can't recover mid-test and mask a bug.
	l := middleware.NewLimiter(2, 0.001)

	if !l.Allow("client-a") {
		t.Error("1st request should be allowed (bucket starts full)")
	}
	if !l.Allow("client-a") {
		t.Error("2nd request should be allowed (capacity is 2)")
	}
	if l.Allow("client-a") {
		t.Error("3rd request should be rejected (bucket is empty)")
	}
}

func TestLimiter_RefillsOverTime(t *testing.T) {
	// capacity 1, refill 20/s: after ~50ms roughly 1 token is back.
	l := middleware.NewLimiter(1, 20)

	if !l.Allow("client-a") {
		t.Fatal("1st request should be allowed (bucket starts full)")
	}
	if l.Allow("client-a") {
		t.Fatal("2nd request should be rejected immediately (bucket just emptied)")
	}

	time.Sleep(100 * time.Millisecond) // comfortably more than the ~50ms needed

	if !l.Allow("client-a") {
		t.Error("request after waiting should be allowed (bucket should have refilled)")
	}
}

func TestLimiter_ClientsHaveSeparateBuckets(t *testing.T) {
	l := middleware.NewLimiter(1, 0.001)

	if !l.Allow("client-a") {
		t.Fatal("client-a's 1st request should be allowed")
	}
	if l.Allow("client-a") {
		t.Fatal("client-a's 2nd request should be rejected (bucket empty)")
	}
	if !l.Allow("client-b") {
		t.Error("client-b should have its own bucket and not be affected by client-a")
	}
}

func TestLimiter_ConcurrentAccessIsSafe(t *testing.T) {
	l := middleware.NewLimiter(100, 1)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l.Allow("shared-client")
		}()
	}
	wg.Wait() // if this panics or -race flags it, the locking is broken
}

func newFallback(t *testing.T, redisAddr string, capacity float64) *middleware.FallbackLimiter {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr, ContextTimeoutEnabled: true, MaxRetries: -1})
	t.Cleanup(func() { rdb.Close() })
	return middleware.NewFallbackLimiter(
		middleware.NewRedisLimiter(rdb, capacity, 0.001),
		middleware.NewLimiter(capacity, 0.001),
		time.Minute,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
}

func TestFallbackLimiter_StillLimitsWhenRedisDown(t *testing.T) {
	// Port 1 refuses connections: Redis "stopped".
	l := newFallback(t, "127.0.0.1:1", 2)

	if !l.Allow("client-a") || !l.Allow("client-a") {
		t.Fatal("first 2 requests should be allowed by the in-memory fallback")
	}
	if l.Allow("client-a") {
		t.Error("3rd request should be rejected: fallback must enforce the limit, not fail open")
	}
}

func TestFallbackLimiter_SkipsHungRedis(t *testing.T) {
	// Accepts connections but never replies: Redis "hung".
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { c.Close() })
		}
	}()

	l := newFallback(t, ln.Addr().String(), 100)

	start := time.Now()
	l.Allow("client-a") // pays the timeout once, then marks Redis down
	if d := time.Since(start); d > time.Second {
		t.Fatalf("first request took %v; the 100ms Redis timeout isn't being applied", d)
	}

	start = time.Now()
	for i := 0; i < 10; i++ {
		l.Allow("client-a")
	}
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Errorf("10 requests took %v; Redis should be skipped during the retry window", d)
	}
}
