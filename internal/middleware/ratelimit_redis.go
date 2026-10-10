package middleware

import (
	"context"
	_ "embed"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed ratelimit.lua
var rateLimitLua string

var rateLimitScript = redis.NewScript(rateLimitLua)

type RedisLimiter struct {
	client   *redis.Client
	capacity float64
	refill   float64
}

var _ RateLimiter = (*FallbackLimiter)(nil)
var _ RateLimiter = (*Limiter)(nil)

func NewRedisLimiter(client *redis.Client, capacity, refillPerSecond float64) *RedisLimiter {
	return &RedisLimiter{client: client, capacity: capacity, refill: refillPerSecond}
}

// Allow reports whether the request fits in the client's shared bucket.
// It returns the Redis error rather than guessing, so the caller decides
// what an outage means (see FallbackLimiter).
func (l *RedisLimiter) Allow(clientKey string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	res, err := rateLimitScript.Run(ctx, l.client,
		[]string{"ratelimit:" + clientKey}, l.capacity, l.refill).Int()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

// FallbackLimiter enforces limits through Redis so they are shared across
// gateway instances. While Redis is unreachable it falls back to a
// per-instance in-memory limiter: limits stay enforced (up to N× with N
// instances) instead of failing open (unbounded upstream spend) or failing
// closed (Redis outage becomes a gateway outage).
type FallbackLimiter struct {
	primary    *RedisLimiter
	fallback   *Limiter
	retryAfter time.Duration
	logger     *slog.Logger

	// downUntil is a unix-nano deadline; until then Redis is skipped so a
	// hung Redis doesn't add its timeout to every request. 0 = healthy.
	downUntil atomic.Int64
}

func NewFallbackLimiter(primary *RedisLimiter, fallback *Limiter, retryAfter time.Duration, logger *slog.Logger) *FallbackLimiter {
	return &FallbackLimiter{primary: primary, fallback: fallback, retryAfter: retryAfter, logger: logger}
}

func (f *FallbackLimiter) Allow(clientKey string) bool {
	now := time.Now().UnixNano()
	if now < f.downUntil.Load() {
		return f.fallback.Allow(clientKey)
	}

	allowed, err := f.primary.Allow(clientKey)
	if err != nil {
		// Swap returns the old value: only the first failure of an outage logs.
		if f.downUntil.Swap(now+f.retryAfter.Nanoseconds()) == 0 {
			f.logger.Warn("redis rate limiter unavailable, using in-memory fallback",
				"err", err, "retry_after", f.retryAfter.String())
		}
		return f.fallback.Allow(clientKey)
	}

	if f.downUntil.Load() != 0 && f.downUntil.Swap(0) != 0 {
		f.logger.Info("redis rate limiter recovered")
	}
	return allowed
}
