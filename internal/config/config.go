package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Upstream struct {
	Name   string
	URL    *url.URL
	APIKey string
}

const (
	defaultAddr                  = ":8080"
	defaultUpstreamURL           = "https://api.openai.com"
	defaultShutdownTimeout       = 10 * time.Second
	defaultResponseHeaderTimeout = 120 * time.Second
	defaultMaxIdleConnsPerHost   = 100
	defaultRateLimitBurst        = 20              // requests a client can burst before waiting
	defaultRateLimitPerSecond    = 5               // sustained requests/sec per client thereafter
	defaultRedisRetryAfter       = 5 * time.Second // skip Redis this long after a failure
	defaultCacheTTL              = 24 * time.Hour  // how long cached responses live
	defaultRetryMaxAttempts      = 3               // total upstream tries per request; 1 = no retries
	defaultRetryBaseDelay        = 250 * time.Millisecond
	defaultRetryMaxDelay         = 4 * time.Second
	defaultBreakerThreshold      = 5                // consecutive upstream failures that trip the breaker
	defaultBreakerCooldown       = 30 * time.Second // how long a tripped breaker fails fast

)

type Config struct {
	Upstreams             []Upstream
	Addr                  string
	UpstreamURL           *url.URL
	UpstreamAPIKey        string
	ClientAPIKeys         map[string]bool
	ShutdownTimeout       time.Duration
	MaxIdleConnsPerHost   int
	ResponseHeaderTimeout time.Duration
	RateLimitBurst        float64
	RateLimitPerSecond    float64
	RedisAddr             string // empty = use in-memory limiter
	RedisRetryAfter       time.Duration
	CacheTTL              time.Duration // 0 = caching off; needs RedisAddr
	RetryMaxAttempts      int
	RetryBaseDelay        time.Duration
	RetryMaxDelay         time.Duration
	BreakerThreshold      int
	BreakerCooldown       time.Duration
}

// Load reads configuration from the environment. Pass os.Getenv in
// production; tests pass a fake.
func Load(getenv func(string) string) (Config, error) {
	var err error
	cfg := Config{
		Addr:           envOr(getenv, "LLMGATE_ADDR", defaultAddr),
		UpstreamAPIKey: getenv("LLMGATE_UPSTREAM_API_KEY"),
		RedisAddr:      getenv("LLMGATE_REDIS_ADDR"),
	}

	if cfg.Upstreams, err = upstreamsEnv(getenv); err != nil {
		return Config{}, err
	}
	// Preserve the legacy fields for callers using the single-upstream config.
	cfg.UpstreamURL = cfg.Upstreams[0].URL
	cfg.UpstreamAPIKey = cfg.Upstreams[0].APIKey

	if cfg.ClientAPIKeys, err = keySetEnv(getenv, "LLMGATE_CLIENT_API_KEYS"); err != nil {
		return Config{}, err
	}

	if cfg.ShutdownTimeout, err = durationEnv(getenv, "LLMGATE_SHUTDOWN_TIMEOUT", defaultShutdownTimeout); err != nil {
		return Config{}, err
	}

	if cfg.ResponseHeaderTimeout, err = durationEnv(getenv, "LLMGATE_RESPONSE_HEADER_TIMEOUT", defaultResponseHeaderTimeout); err != nil {
		return Config{}, err
	}

	if cfg.MaxIdleConnsPerHost, err = intEnv(getenv, "LLMGATE_MAX_IDLE_CONNS_PER_HOST", defaultMaxIdleConnsPerHost); err != nil {
		return Config{}, err
	}
	if cfg.RateLimitBurst, err = floatEnv(getenv, "LLMGATE_RATE_LIMIT_BURST", defaultRateLimitBurst); err != nil {
		return Config{}, err
	}

	if cfg.RateLimitPerSecond, err = floatEnv(getenv, "LLMGATE_RATE_LIMIT_PER_SECOND", defaultRateLimitPerSecond); err != nil {
		return Config{}, err
	}

	if cfg.RedisRetryAfter, err = durationEnv(getenv, "LLMGATE_REDIS_RETRY_AFTER", defaultRedisRetryAfter); err != nil {
		return Config{}, err
	}

	// "0" turns caching off, which durationEnv would reject as non-positive.
	if raw := getenv("LLMGATE_CACHE_TTL"); raw != "0" && raw != "0s" {
		if cfg.CacheTTL, err = durationEnv(getenv, "LLMGATE_CACHE_TTL", defaultCacheTTL); err != nil {
			return Config{}, err
		}
	}

	if cfg.RetryMaxAttempts, err = intEnv(getenv, "LLMGATE_RETRY_MAX_ATTEMPTS", defaultRetryMaxAttempts); err != nil {
		return Config{}, err
	}
	if cfg.RetryBaseDelay, err = durationEnv(getenv, "LLMGATE_RETRY_BASE_DELAY", defaultRetryBaseDelay); err != nil {
		return Config{}, err
	}
	if cfg.RetryMaxDelay, err = durationEnv(getenv, "LLMGATE_RETRY_MAX_DELAY", defaultRetryMaxDelay); err != nil {
		return Config{}, err
	}
	if cfg.BreakerThreshold, err = intEnv(getenv, "LLMGATE_BREAKER_THRESHOLD", defaultBreakerThreshold); err != nil {
		return Config{}, err
	}
	if cfg.BreakerCooldown, err = durationEnv(getenv, "LLMGATE_BREAKER_COOLDOWN", defaultBreakerCooldown); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

// LLMGATE_UPSTREAMS overrides the legacy URL/key pair when provided.
func upstreamsEnv(getenv func(string) string) ([]Upstream, error) {
	raw := getenv("LLMGATE_UPSTREAMS")
	if raw == "" {
		key := getenv("LLMGATE_UPSTREAM_API_KEY")
		if strings.TrimSpace(key) == "" {
			return nil, errors.New("LLMGATE_UPSTREAM_API_KEY is required")
		}
		u, err := upstreamURL(envOr(getenv, "LLMGATE_UPSTREAM_URL", defaultUpstreamURL))
		if err != nil {
			return nil, errors.New("LLMGATE_UPSTREAM_URL must be an http(s) base URL without credentials, query, or fragment")
		}
		return []Upstream{{Name: "default", URL: u, APIKey: key}}, nil
	}

	var entries []struct {
		Name   string `json:"name"`
		URL    string `json:"url"`
		APIKey string `json:"api_key"`
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&entries); err != nil {
		return nil, fmt.Errorf("LLMGATE_UPSTREAMS: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, errors.New("LLMGATE_UPSTREAMS must contain a single JSON array")
	}
	if len(entries) == 0 {
		return nil, errors.New("LLMGATE_UPSTREAMS must contain at least one upstream")
	}

	upstreams := make([]Upstream, 0, len(entries))
	names := make(map[string]bool, len(entries))
	for i, entry := range entries {
		name := strings.TrimSpace(entry.Name)
		if name == "" {
			name = fmt.Sprintf("upstream-%d", i+1)
		}
		if names[name] {
			return nil, fmt.Errorf("LLMGATE_UPSTREAMS entry %d has a duplicate name", i+1)
		}
		names[name] = true
		u, err := upstreamURL(entry.URL)
		if err != nil {
			return nil, fmt.Errorf("LLMGATE_UPSTREAMS entry %d has an invalid base URL", i+1)
		}
		if strings.TrimSpace(entry.APIKey) == "" {
			return nil, fmt.Errorf("LLMGATE_UPSTREAMS entry %d requires api_key", i+1)
		}
		upstreams = append(upstreams, Upstream{Name: name, URL: u, APIKey: entry.APIKey})
	}
	return upstreams, nil
}

func upstreamURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") ||
		u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return nil, errors.New("invalid upstream URL")
	}
	return u, nil
}

func envOr(getenv func(string) string, key, fallback string) string {
	if v := getenv(key); v != "" {
		return v
	}
	return fallback
}

func durationEnv(getenv func(string) string, key string, fallback time.Duration) (time.Duration, error) {
	raw := getenv(key)
	if raw == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("%s must be positive, got %s", key, d)
	}
	return d, nil
}

func intEnv(getenv func(string) string, key string, fallback int) (int, error) {
	raw := getenv(key)
	if raw == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	if n <= 0 {
		return 0, fmt.Errorf("%s must be positive, got %d", key, n)
	}
	return n, nil
}

// keySetEnv parses a comma-separated list of keys into a set.
func keySetEnv(getenv func(string) string, key string) (map[string]bool, error) {
	set := make(map[string]bool)
	for _, k := range strings.Split(getenv(key), ",") {
		if k = strings.TrimSpace(k); k != "" {
			set[k] = true
		}
	}
	if len(set) == 0 {
		return nil, fmt.Errorf("%s is required (comma-separated client keys)", key)
	}
	return set, nil
}

func floatEnv(getenv func(string) string, key string, fallback float64) (float64, error) {
	raw := getenv(key)
	if raw == "" {
		return fallback, nil
	}
	f, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	if f <= 0 || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, fmt.Errorf("%s must be positive, got %v", key, f)
	}
	return f, nil
}
