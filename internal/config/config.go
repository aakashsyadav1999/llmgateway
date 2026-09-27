package config

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	defaultAddr                  = ":8080"
	defaultUpstreamURL           = "https://api.openai.com"
	defaultShutdownTimeout       = 10 * time.Second
	defaultResponseHeaderTimeout = 120 * time.Second
	defaultMaxIdleConnsPerHost   = 100
)

type Config struct {
	Addr                  string
	UpstreamURL           *url.URL
	UpstreamAPIKey        string
	ClientAPIKeys         map[string]bool
	ShutdownTimeout       time.Duration
	MaxIdleConnsPerHost   int
	ResponseHeaderTimeout time.Duration
}

// Load reads configuration from the environment. Pass os.Getenv in
// production; tests pass a fake.
func Load(getenv func(string) string) (Config, error) {
	var err error
	cfg := Config{
		Addr:           envOr(getenv, "LLMGATE_ADDR", defaultAddr),
		UpstreamAPIKey: getenv("LLMGATE_UPSTREAM_API_KEY"),
	}

	if cfg.UpstreamAPIKey == "" {
		return Config{}, errors.New("LLMGATE_UPSTREAM_API_KEY is required")
	}

	if cfg.ClientAPIKeys, err = keySetEnv(getenv, "LLMGATE_CLIENT_API_KEYS"); err != nil {
		return Config{}, err
	}

	rawURL := envOr(getenv, "LLMGATE_UPSTREAM_URL", defaultUpstreamURL)
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return Config{}, fmt.Errorf("LLMGATE_UPSTREAM_URL %q is not a valid http(s) URL", rawURL)
	}
	cfg.UpstreamURL = u

	if cfg.ShutdownTimeout, err = durationEnv(getenv, "LLMGATE_SHUTDOWN_TIMEOUT", defaultShutdownTimeout); err != nil {
		return Config{}, err
	}

	if cfg.ResponseHeaderTimeout, err = durationEnv(getenv, "LLMGATE_RESPONSE_HEADER_TIMEOUT", defaultResponseHeaderTimeout); err != nil {
		return Config{}, err
	}

	if cfg.MaxIdleConnsPerHost, err = intEnv(getenv, "LLMGATE_MAX_IDLE_CONNS_PER_HOST", defaultMaxIdleConnsPerHost); err != nil {
		return Config{}, err
	}

	return cfg, nil
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
