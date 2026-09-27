package tests

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aakashsyadav1999/llmgate/internal/middleware"
)

func TestAuthThenRateLimit_ValidKeyRespectsLimit(t *testing.T) {
	called := 0
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called++
		w.WriteHeader(http.StatusOK)
	})

	validKeys := map[string]bool{"good-key": true}
	limiter := middleware.NewLimiter(2, 0.001) // capacity 2, refill effectively frozen for the test
	chain := middleware.Auth(validKeys)(middleware.RateLimit(limiter)(next))

	doReq := func() int {
		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("Authorization", "Bearer good-key")
		rec := httptest.NewRecorder()
		chain.ServeHTTP(rec, req)
		return rec.Code
	}

	if code := doReq(); code != http.StatusOK {
		t.Fatalf("1st request status = %d, want 200", code)
	}
	if code := doReq(); code != http.StatusOK {
		t.Fatalf("2nd request status = %d, want 200", code)
	}
	if code := doReq(); code != http.StatusTooManyRequests {
		t.Fatalf("3rd request status = %d, want 429", code)
	}
	if called != 2 {
		t.Errorf("next was called %d times, want 2 (the 3rd should have been blocked before reaching it)", called)
	}
}

func TestAuthThenRateLimit_ClientsHaveSeparateBuckets(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	validKeys := map[string]bool{"key-a": true, "key-b": true}
	limiter := middleware.NewLimiter(1, 0.001)
	chain := middleware.Auth(validKeys)(middleware.RateLimit(limiter)(next))

	doReq := func(key string) int {
		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("Authorization", "Bearer "+key)
		rec := httptest.NewRecorder()
		chain.ServeHTTP(rec, req)
		return rec.Code
	}

	if code := doReq("key-a"); code != http.StatusOK {
		t.Fatalf("key-a's 1st request status = %d, want 200", code)
	}
	if code := doReq("key-a"); code != http.StatusTooManyRequests {
		t.Fatalf("key-a's 2nd request status = %d, want 429", code)
	}
	if code := doReq("key-b"); code != http.StatusOK {
		t.Errorf("key-b's request status = %d, want 200 (must not share key-a's bucket)", code)
	}
}

func TestAuthThenRateLimit_RejectedAuthNeverConsumesABucketToken(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	validKeys := map[string]bool{"good-key": true}
	limiter := middleware.NewLimiter(1, 0.001)
	chain := middleware.Auth(validKeys)(middleware.RateLimit(limiter)(next))

	// A wrong key is rejected by Auth and must never reach RateLimit,
	// so it must not spend a token from any bucket.
	badReq := httptest.NewRequest("GET", "/", nil)
	badReq.Header.Set("Authorization", "Bearer wrong-key")
	badRec := httptest.NewRecorder()
	chain.ServeHTTP(badRec, badReq)
	if badRec.Code != http.StatusUnauthorized {
		t.Fatalf("bad key status = %d, want 401", badRec.Code)
	}

	goodReq := httptest.NewRequest("GET", "/", nil)
	goodReq.Header.Set("Authorization", "Bearer good-key")
	goodRec := httptest.NewRecorder()
	chain.ServeHTTP(goodRec, goodReq)
	if goodRec.Code != http.StatusOK {
		t.Errorf("good key status = %d, want 200 (its bucket should still be full)", goodRec.Code)
	}
}
