package middleware

import (
	"context"
	"net/http"
	"sync"
	"time"
)

type bucket struct {
	tokens     float64
	capacity   float64
	refillRate float64 // tokens per second
	lastRefill time.Time
	mu         sync.Mutex
}

type Limiter struct {
	mu       sync.Mutex
	buckets  map[string]*bucket
	capacity float64
	refill   float64
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
		buckets:  make(map[string]*bucket),
		capacity: capacity,
		refill:   refillPerSecond,
	}
}

func (l *Limiter) Allow(clientKey string) bool {
	l.mu.Lock()
	b, ok := l.buckets[clientKey]
	if !ok {
		b = &bucket{tokens: l.capacity, capacity: l.capacity, refillRate: l.refill, lastRefill: time.Now()}
		l.buckets[clientKey] = b
	}
	l.mu.Unlock()

	return b.Allow()
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

func RateLimit(limiter *Limiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := ClientKeyFromContext(r.Context())
			if !limiter.Allow(key) {
				w.Header().Set("X-RateLimit-Limit", "1")
				http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
