package tests

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aakashsyadav1999/llmgate/internal/middleware"
	"github.com/redis/go-redis/v9"
)

// redisClient connects to a real local Redis, skipping the test if none is
// running so `go test ./...` still passes on machines without Redis.
// Override the address with LLMGATE_TEST_REDIS_ADDR.
func redisClient(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("LLMGATE_TEST_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr, ContextTimeoutEnabled: true})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		rdb.Close()
		t.Skipf("no Redis at %s: %v", addr, err)
	}
	t.Cleanup(func() { rdb.Close() })
	return rdb
}

// uniqueKey gives each test its own bucket so runs never see each other's
// state, and deletes it afterwards.
func uniqueKey(t *testing.T, rdb *redis.Client, name string) string {
	t.Helper()
	key := fmt.Sprintf("test-%s-%s-%d", t.Name(), name, time.Now().UnixNano())
	t.Cleanup(func() { rdb.Del(context.Background(), hashedRedisKey(key)) })
	return key
}

func hashedRedisKey(clientKey string) string {
	return fmt.Sprintf("ratelimit:%x", sha256.Sum256([]byte(clientKey)))
}

func TestRedisLimiter_DoesNotPersistClientKey(t *testing.T) {
	rdb := redisClient(t)
	clientKey := uniqueKey(t, rdb, "secret-client-key")
	limiter := middleware.NewRedisLimiter(rdb, 2, 1)
	if !mustAllow(t, limiter, clientKey) {
		t.Fatal("first request should be allowed")
	}
	ctx := context.Background()
	exists, err := rdb.Exists(ctx, hashedRedisKey(clientKey)).Result()
	if err != nil || exists != 1 {
		t.Fatalf("expected a SHA-256 bucket: exists=%d, err=%v", exists, err)
	}
	var cursor uint64
	for {
		keys, next, err := rdb.Scan(ctx, cursor, "*"+clientKey+"*", 100).Result()
		if err != nil {
			t.Fatal(err)
		}
		if len(keys) != 0 {
			t.Fatal("raw client credential found in Redis key names")
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	fields, err := rdb.HGetAll(ctx, hashedRedisKey(clientKey)).Result()
	if err != nil {
		t.Fatal(err)
	}
	for field, value := range fields {
		if field == clientKey || value == clientKey {
			t.Fatal("raw client credential found in bucket data")
		}
	}
}

func mustAllow(t *testing.T, l *middleware.RedisLimiter, key string) bool {
	t.Helper()
	ok, err := l.Allow(key)
	if err != nil {
		t.Fatalf("redis error: %v", err)
	}
	return ok
}

func TestRedisLimiter_AllowsBurstUpToCapacity(t *testing.T) {
	rdb := redisClient(t)
	l := middleware.NewRedisLimiter(rdb, 2, 0.001)
	key := uniqueKey(t, rdb, "a")

	if !mustAllow(t, l, key) || !mustAllow(t, l, key) {
		t.Fatal("first 2 requests should be allowed (bucket starts full)")
	}
	if mustAllow(t, l, key) {
		t.Error("3rd request should be rejected (bucket is empty)")
	}
}

func TestRedisLimiter_RefillsOverTime(t *testing.T) {
	rdb := redisClient(t)
	l := middleware.NewRedisLimiter(rdb, 1, 20) // ~1 token back every 50ms
	key := uniqueKey(t, rdb, "a")

	if !mustAllow(t, l, key) {
		t.Fatal("1st request should be allowed")
	}
	if mustAllow(t, l, key) {
		t.Fatal("2nd request should be rejected immediately")
	}

	time.Sleep(100 * time.Millisecond)

	if !mustAllow(t, l, key) {
		t.Error("request after waiting should be allowed (bucket should have refilled)")
	}
}

func TestRedisLimiter_ClientsHaveSeparateBuckets(t *testing.T) {
	rdb := redisClient(t)
	l := middleware.NewRedisLimiter(rdb, 1, 0.001)
	a, b := uniqueKey(t, rdb, "a"), uniqueKey(t, rdb, "b")

	if !mustAllow(t, l, a) {
		t.Fatal("client-a's 1st request should be allowed")
	}
	if mustAllow(t, l, a) {
		t.Fatal("client-a's 2nd request should be rejected")
	}
	if !mustAllow(t, l, b) {
		t.Error("client-b should have its own bucket")
	}
}

func TestRedisLimiter_SetsTTL(t *testing.T) {
	rdb := redisClient(t)
	l := middleware.NewRedisLimiter(rdb, 10, 1) // full after 10s, so TTL = 11s
	key := uniqueKey(t, rdb, "a")

	mustAllow(t, l, key)

	ttl, err := rdb.TTL(context.Background(), hashedRedisKey(key)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl <= 0 || ttl > 11*time.Second {
		t.Errorf("TTL = %v, want (0, 11s]: idle buckets must expire", ttl)
	}
}

// Two limiters with separate connections stand in for two gateway
// instances: they must draw from the same bucket.
func TestRedisLimiter_SharedAcrossInstances(t *testing.T) {
	rdb1, rdb2 := redisClient(t), redisClient(t)
	gw1 := middleware.NewRedisLimiter(rdb1, 3, 0.001)
	gw2 := middleware.NewRedisLimiter(rdb2, 3, 0.001)
	key := uniqueKey(t, rdb1, "a")

	allowed := 0
	for i := 0; i < 6; i++ {
		gw := gw1
		if i%2 == 1 {
			gw = gw2
		}
		if mustAllow(t, gw, key) {
			allowed++
		}
	}
	if allowed != 3 {
		t.Errorf("allowed %d of 6 across two instances, want 3 (limit must be shared)", allowed)
	}
}

// Many concurrent requests must never overspend the bucket: this is what
// running refill-and-consume as one atomic script buys over GET-then-SET.
func TestRedisLimiter_ConcurrentRequestsAreAtomic(t *testing.T) {
	rdb := redisClient(t)
	l := middleware.NewRedisLimiter(rdb, 10, 0.001)
	key := uniqueKey(t, rdb, "a")

	var allowed atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := l.Allow(key)
			if err != nil {
				t.Errorf("redis error: %v", err)
				return
			}
			if ok {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()

	if got := allowed.Load(); got != 10 {
		t.Errorf("allowed %d of 50 concurrent requests, want exactly 10", got)
	}
}
