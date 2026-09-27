package tests

import (
	"sync"
	"testing"
	"time"

	"github.com/aakashsyadav1999/llmgate/internal/middleware"
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
