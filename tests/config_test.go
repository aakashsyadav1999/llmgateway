package tests

import (
	"strings"
	"testing"
	"time"

	"github.com/aakashsyadav1999/llmgate/internal/config"
)

func TestLoad_UpstreamPool(t *testing.T) {
	cfg, err := config.Load(fakeEnv(map[string]string{
		"LLMGATE_CLIENT_API_KEYS": "client-key",
		"LLMGATE_UPSTREAMS": `[
			{"name":"primary","url":"https://one.example/api","api_key":"key-one"},
			{"url":"https://two.example","api_key":"key-two"}
		]`,
		// The pool overrides these legacy settings.
		"LLMGATE_UPSTREAM_URL": "invalid",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Upstreams) != 2 {
		t.Fatalf("got %d upstreams, want 2", len(cfg.Upstreams))
	}
	if cfg.Upstreams[0].Name != "primary" || cfg.Upstreams[0].URL.Path != "/api" || cfg.Upstreams[0].APIKey != "key-one" {
		t.Fatal("first upstream fields were not loaded")
	}
	if cfg.Upstreams[1].Name != "upstream-2" || cfg.Upstreams[1].URL.Host != "two.example" || cfg.Upstreams[1].APIKey != "key-two" {
		t.Fatal("second upstream fields were not loaded")
	}
	if cfg.UpstreamURL != cfg.Upstreams[0].URL || cfg.UpstreamAPIKey != cfg.Upstreams[0].APIKey {
		t.Fatal("legacy fields should point to the first configured upstream")
	}
}

func TestLoad_InvalidUpstreamPools(t *testing.T) {
	for name, raw := range map[string]string{
		"invalid JSON": "[",
		"object":       `{}`,
		"empty":        `[]`,
		"null":         `null`,
		"trailing":     `[{"url":"http://one.example","api_key":"key"}] []`,
		"missing URL":  `[{"api_key":"key"}]`,
		"missing key":  `[{"url":"http://one.example"}]`,
		"blank key":    `[{"url":"http://one.example","api_key":" "}]`,
		"bad scheme":   `[{"url":"ftp://one.example","api_key":"key"}]`,
		"userinfo":     `[{"url":"http://user:secret-for-errors@one.example","api_key":"key"}]`,
		"query":        `[{"url":"http://one.example?token=secret-for-errors","api_key":"key"}]`,
		"fragment":     `[{"url":"http://one.example#part","api_key":"key"}]`,
		"unknown":      `[{"url":"http://one.example","api_key":"key","typo":true}]`,
		"duplicate":    `[{"name":"same","url":"http://one.example","api_key":"key"},{"name":"same","url":"http://two.example","api_key":"key"}]`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := config.Load(fakeEnv(map[string]string{
				"LLMGATE_CLIENT_API_KEYS": "client-key",
				"LLMGATE_UPSTREAMS":       raw,
			}))
			if err == nil {
				t.Fatal("expected invalid pool to be rejected")
			}
			if strings.Contains(err.Error(), "secret-for-errors") {
				t.Fatal("configuration error exposed URL credentials")
			}
		})
	}
}

func fakeEnv(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoad_MissingAPIKey(t *testing.T) {
	_, err := config.Load(fakeEnv(map[string]string{}))
	if err == nil {
		t.Fatal("expected error when API key is missing")
	}
}

func TestLoad_Defaults(t *testing.T) {
	cfg, err := config.Load(fakeEnv(map[string]string{
		"LLMGATE_UPSTREAM_API_KEY": "sk-test",
		"LLMGATE_CLIENT_API_KEYS":  "sk-gateway-dev-key",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Addr != ":8080" {
		t.Errorf("Addr = %q, want :8080", cfg.Addr)
	}
	if cfg.UpstreamURL.Host != "api.openai.com" {
		t.Errorf("Host = %q, want api.openai.com", cfg.UpstreamURL.Host)
	}
	if len(cfg.Upstreams) != 1 || cfg.Upstreams[0].Name != "default" || cfg.Upstreams[0].APIKey != "sk-test" {
		t.Fatal("legacy configuration should populate a single-upstream pool")
	}
	if cfg.RedisAddr != "" {
		t.Errorf("RedisAddr = %q, want empty (in-memory limiter)", cfg.RedisAddr)
	}
	if cfg.RedisRetryAfter != 5*time.Second {
		t.Errorf("RedisRetryAfter = %v, want 5s", cfg.RedisRetryAfter)
	}
	if cfg.RetryMaxAttempts != 3 || cfg.BreakerThreshold != 5 || cfg.BreakerCooldown != 30*time.Second {
		t.Errorf("resilience defaults = %d attempts, threshold %d, cooldown %v; want 3, 5, 30s",
			cfg.RetryMaxAttempts, cfg.BreakerThreshold, cfg.BreakerCooldown)
	}
}

func TestLoad_RedisSettings(t *testing.T) {
	cfg, err := config.Load(fakeEnv(map[string]string{
		"LLMGATE_UPSTREAM_API_KEY":  "sk-test",
		"LLMGATE_CLIENT_API_KEYS":   "sk-gateway-dev-key",
		"LLMGATE_REDIS_ADDR":        "localhost:6379",
		"LLMGATE_REDIS_RETRY_AFTER": "2s",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.RedisAddr != "localhost:6379" {
		t.Errorf("RedisAddr = %q, want localhost:6379", cfg.RedisAddr)
	}
	if cfg.RedisRetryAfter != 2*time.Second {
		t.Errorf("RedisRetryAfter = %v, want 2s", cfg.RedisRetryAfter)
	}
}

func TestLoad_BadDuration(t *testing.T) {
	_, err := config.Load(fakeEnv(map[string]string{
		"LLMGATE_UPSTREAM_API_KEY": "sk-test",
		"LLMGATE_CLIENT_API_KEYS":  "sk-gateway-dev-key",
		"LLMGATE_SHUTDOWN_TIMEOUT": "banana",
	}))
	if err == nil {
		t.Fatal("expected error for invalid duration")
	}
}
