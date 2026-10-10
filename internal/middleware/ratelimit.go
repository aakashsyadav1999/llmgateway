package middleware

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/aakashsyadav1999/llmgate/internal/observability"
)

type bucket struct {
	tokens     float64
	capacity   float64
	refillRate float64 // tokens per second
	lastRefill time.Time
	mu         sync.Mutex
}

// sweepInterval is how often Allow drops idle buckets, so the map doesn't
// grow forever with every client key ever seen.
const sweepInterval = time.Minute

type Limiter struct {
	mu        sync.Mutex
	buckets   map[string]*bucket
	capacity  float64
	refill    float64
	lastSweep time.Time
}

type RateLimiter interface {
	Allow(clientKey string) bool
}

func RateLimit(limiter RateLimiter) func(http.Handler) http.Handler {
	return RateLimitWithMetrics(limiter, nil)
}

func RateLimitWithMetrics(limiter RateLimiter, metrics *observability.Metrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := ClientKeyFromContext(r.Context())
			if !limiter.Allow(key) {
				metrics.Reject(key)
				http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func (b *bucket) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(b.lastRefill).Seconds()
	b.tokens = min(b.capacity, b.tokens+elapsed*b.refillRate)
	b.lastRefill = now

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

func NewLimiter(capacity, refillPerSecond float64) *Limiter {
	return &Limiter{
		buckets:   make(map[string]*bucket),
		capacity:  capacity,
		refill:    refillPerSecond,
		lastSweep: time.Now(),
	}
}

func (l *Limiter) Allow(clientKey string) bool {
	l.mu.Lock()
	if now := time.Now(); now.Sub(l.lastSweep) >= sweepInterval {
		l.sweep(now)
		l.lastSweep = now
	}
	b, ok := l.buckets[clientKey]
	if !ok {
		b = &bucket{tokens: l.capacity, capacity: l.capacity, refillRate: l.refill, lastRefill: time.Now()}
		l.buckets[clientKey] = b
	}
	l.mu.Unlock()

	return b.Allow()
}

// sweep deletes buckets idle long enough to have refilled completely.
// A full bucket is the same as a missing one, so this never changes a
// decision; it matches the TTL the Redis script sets. Caller holds l.mu.
func (l *Limiter) sweep(now time.Time) {
	if l.refill <= 0 {
		return
	}
	fullAfter := time.Duration(l.capacity / l.refill * float64(time.Second))
	for key, b := range l.buckets {
		b.mu.Lock()
		idle := now.Sub(b.lastRefill)
		b.mu.Unlock()
		if idle >= fullAfter {
			delete(l.buckets, key)
		}
	}
}

func ClientKeyFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if key, ok := ctx.Value(clientKeyContextKey).(string); ok {
		return key
	}
	return ""
}
