// Package cache stores upstream LLM responses in Redis so identical,
// deterministic requests are answered without another upstream call.
package cache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// MaxEntryBytes caps what gets stored; larger responses are passed through
// uncached rather than buffered without limit.
const MaxEntryBytes = 1 << 20 // 1 MiB

const (
	getTimeout = 100 * time.Millisecond
	setTimeout = 200 * time.Millisecond
)

// Entry is a stored successful (200) upstream response.
type Entry struct {
	ContentType string
	Body        []byte
}

// Cache is safe for concurrent use. Every Redis failure is treated as a
// miss: caching is an optimisation, so an outage must never fail requests.
type Cache struct {
	rdb        *redis.Client
	ttl        time.Duration
	retryAfter time.Duration
	logger     *slog.Logger

	// downUntil is a unix-nano deadline; until then Redis is skipped so a
	// hung Redis doesn't add its timeout to every request. 0 = healthy.
	downUntil atomic.Int64
}

func New(rdb *redis.Client, ttl, retryAfter time.Duration, logger *slog.Logger) *Cache {
	return &Cache{rdb: rdb, ttl: ttl, retryAfter: retryAfter, logger: logger}
}

// Key returns the cache key for a request, and false if the request must
// not be cached. The key covers the client key (each client has its own
// cache), the path, and the whole normalised body, so requests differing
// in any parameter (tools, max_tokens, stream...) never share an entry.
func Key(clientKey, method, path string, body []byte) (string, bool) {
	if method != "POST" {
		return "", false
	}

	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber() // keep numbers exactly as sent, no float rounding
	var req map[string]any
	if err := dec.Decode(&req); err != nil {
		return "", false
	}

	if !cacheable(path, req) {
		return "", false
	}

	// Re-encoding sorts map keys, so field order and whitespace don't
	// change the hash.
	normalized, err := json.Marshal(req)
	if err != nil {
		return "", false
	}

	h := sha256.New()
	for _, part := range [][]byte{[]byte(clientKey), []byte(path), normalized} {
		h.Write(part)
		h.Write([]byte{0}) // separator: ("ab","c") must not hash like ("a","bc")
	}
	return "cache:" + hex.EncodeToString(h.Sum(nil)), true
}

// cacheable decides whether a response to req is reusable.
func cacheable(path string, req map[string]any) bool {
	switch path {
	case "/v1/embeddings":
		return true // embeddings are deterministic
	case "/v1/chat/completions":
		// Only explicit temperature 0. Unset is NOT safe: OpenAI defaults
		// to 1, so callers expect a different answer each time.
		if !numberIs(req["temperature"], 0) {
			return false
		}
		// n > 1 asks for several different completions.
		if n, ok := req["n"]; ok && !numberIs(n, 1) {
			return false
		}
		return true
	default:
		return false
	}
}

func numberIs(v any, want float64) bool {
	n, ok := v.(json.Number)
	if !ok {
		return false
	}
	f, err := strconv.ParseFloat(n.String(), 64)
	return err == nil && f == want
}

// Get returns the stored entry for key, if any.
func (c *Cache) Get(ctx context.Context, key string) (*Entry, bool) {
	if c.skipping() {
		return nil, false
	}
	ctx, cancel := context.WithTimeout(ctx, getTimeout)
	defer cancel()

	vals, err := c.rdb.HMGet(ctx, key, "content_type", "body").Result()
	if err != nil {
		c.failed(err)
		return nil, false
	}
	c.recovered()

	ct, ok1 := vals[0].(string)
	body, ok2 := vals[1].(string)
	if !ok1 || !ok2 {
		return nil, false // missing or expired
	}
	return &Entry{ContentType: ct, Body: []byte(body)}, true
}

// Set stores e under key with the configured TTL.
func (c *Cache) Set(ctx context.Context, key string, e *Entry) {
	if c.skipping() || len(e.Body) > MaxEntryBytes {
		return
	}
	// Detached from the request: the response is already sent, and a
	// client hanging up now shouldn't cancel the write.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), setTimeout)
	defer cancel()

	_, err := c.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.HSet(ctx, key, "content_type", e.ContentType, "body", e.Body)
		p.Expire(ctx, key, c.ttl)
		return nil
	})
	if err != nil {
		c.failed(err)
		return
	}
	c.recovered()
}

func (c *Cache) skipping() bool {
	return time.Now().UnixNano() < c.downUntil.Load()
}

func (c *Cache) failed(err error) {
	until := time.Now().Add(c.retryAfter).UnixNano()
	// Swap returns the old value: only the first failure of an outage logs.
	if c.downUntil.Swap(until) == 0 {
		c.logger.Warn("redis cache unavailable, serving uncached",
			"err", err, "retry_after", c.retryAfter.String())
	}
}

func (c *Cache) recovered() {
	if c.downUntil.Load() != 0 && c.downUntil.Swap(0) != 0 {
		c.logger.Info("redis cache recovered")
	}
}
