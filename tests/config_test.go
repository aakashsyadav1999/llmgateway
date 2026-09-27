package tests

import (
	"testing"

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
