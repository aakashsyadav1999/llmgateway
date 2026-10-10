package tests

import (
	"testing"
	"time"

	"github.com/aakashsyadav1999/llmgate/internal/config"
)

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
