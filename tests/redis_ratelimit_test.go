package tests

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Exercise the actual Lua script in Redis. A private Unix socket avoids port
// conflicts and prevents these tests from touching an existing Redis database.
func TestRedisRateLimitScript(t *testing.T) {
	run := startRateLimitRedis(t)
	cli := func(t *testing.T, args ...string) string {
		t.Helper()
		result, err := run(args...)
		if err != nil {
			t.Fatalf("Redis %s: %v", args[0], err)
		}
		return result
	}
	script, err := os.ReadFile("../internal/middleware/ratelimit.lua")
	if err != nil {
		t.Fatal(err)
	}
	allow := func(t *testing.T, key, capacity, refill string) string {
		return cli(t, "EVAL", string(script), "1", key, capacity, refill)
	}
	seed := func(t *testing.T, key, tokens, offsetMS string) {
		t.Helper()
		// Compute seed timestamps on the same clock used by the limiter.
		cli(t, "EVAL", `local clock = redis.call("TIME")
local now = tonumber(clock[1]) + tonumber(clock[2]) / 1000000
return redis.call("HSET", KEYS[1], "tokens", ARGV[1], "last_refill", string.format("%.17g", now + tonumber(ARGV[2]) / 1000))`,
			"1", key, tokens, offsetMS)
	}

	t.Run("burst and separate clients", func(t *testing.T) {
		for i, want := range []string{"1", "1", "0"} {
			if got := allow(t, "burst-a", "2", "0.001"); got != want {
				t.Fatalf("request %d = %s, want %s", i+1, got, want)
			}
		}
		if got := allow(t, "burst-b", "2", "0.001"); got != "1" {
			t.Fatalf("separate client = %s, want 1", got)
		}
	})

	t.Run("fractional tokens survive rejection", func(t *testing.T) {
		// A future timestamp freezes refill while we check exact fractions.
		seed(t, "fraction", "0.5", "60000")
		before := cli(t, "HGET", "fraction", "last_refill")
		if got := allow(t, "fraction", "2", "1"); got != "0" {
			t.Fatalf("half a token = %s, want 0", got)
		}
		if got := cli(t, "HGET", "fraction", "tokens"); got != "0.5" {
			t.Fatalf("tokens after rejection = %s, want 0.5", got)
		}
		if got := cli(t, "HGET", "fraction", "last_refill"); got != before {
			t.Fatalf("timestamp moved backwards: got %s, want %s", got, before)
		}
		ttl, err := strconv.Atoi(cli(t, "PTTL", "fraction"))
		if err != nil || ttl <= 3000 || ttl > 63000 {
			t.Fatalf("TTL = %d (err %v), want rollback added to refill time", ttl, err)
		}
		cli(t, "HSET", "fraction", "tokens", "1.5")
		if got := allow(t, "fraction", "2", "1"); got != "1" {
			t.Fatalf("one and a half tokens = %s, want 1", got)
		}
		if got := cli(t, "HGET", "fraction", "tokens"); got != "0.5" {
			t.Fatalf("tokens after consumption = %s, want 0.5", got)
		}
	})

	t.Run("refill capped at capacity", func(t *testing.T) {
		seed(t, "refill", "0", "-10000000")
		if got := allow(t, "refill", "2", "0.001"); got != "1" {
			t.Fatalf("refilled bucket = %s, want 1", got)
		}
		if got := cli(t, "HGET", "refill", "tokens"); got != "1" {
			t.Fatalf("tokens after refill and consumption = %s, want 1", got)
		}
	})

	t.Run("denial stores refill progress", func(t *testing.T) {
		seed(t, "progress", "0", "-10000")
		before := cli(t, "HGET", "progress", "last_refill")
		if got := allow(t, "progress", "2", "0.001"); got != "0" {
			t.Fatalf("partially refilled bucket = %s, want 0", got)
		}
		tokens, err := strconv.ParseFloat(cli(t, "HGET", "progress", "tokens"), 64)
		if err != nil || tokens < 0.01 || tokens >= 1 {
			t.Fatalf("stored tokens = %v (err %v), want partial refill", tokens, err)
		}
		if got := cli(t, "HGET", "progress", "last_refill"); got == before {
			t.Fatal("rejection did not update the refill timestamp")
		}
	})

	t.Run("idle bucket expires safely", func(t *testing.T) {
		// One second to refill, plus the script's one-second expiry margin.
		if got := allow(t, "expiry", "1", "1"); got != "1" {
			t.Fatalf("initial request = %s, want 1", got)
		}
		ttl, err := strconv.Atoi(cli(t, "PTTL", "expiry"))
		if err != nil || ttl <= 1000 || ttl > 2000 {
			t.Fatalf("TTL = %d (err %v), want 1001..2000 ms", ttl, err)
		}
		time.Sleep(2100 * time.Millisecond)
		if got := cli(t, "EXISTS", "expiry"); got != "0" {
			t.Fatalf("idle bucket still exists: %s", got)
		}
		if got := allow(t, "expiry", "1", "1"); got != "1" {
			t.Fatalf("request after expiry = %s, want 1", got)
		}
	})

	t.Run("concurrent callers share capacity", func(t *testing.T) {
		const requests = 40
		type response struct {
			value string
			err   error
		}
		results := make(chan response, requests)
		var wg sync.WaitGroup
		for i := 0; i < requests; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				value, err := run("EVAL", string(script), "1", "shared", "10", "0.001")
				results <- response{value: value, err: err}
			}()
		}
		wg.Wait()
		close(results)
		allowed := 0
		for result := range results {
			if result.err != nil {
				t.Fatalf("concurrent Redis call: %v", result.err)
			}
			switch result.value {
			case "1":
				allowed++
			case "0":
			default:
				t.Fatalf("unexpected script result: %s", result.value)
			}
		}
		if allowed != 10 {
			t.Fatalf("allowed %d concurrent requests, want exactly 10", allowed)
		}
	})

	t.Run("invalid arguments do not create a bucket", func(t *testing.T) {
		for _, args := range [][2]string{
			{"0", "1"}, {"-1", "1"}, {"NaN", "1"}, {"+Inf", "1"},
			{"1", "0"}, {"1", "-1"}, {"1", "NaN"}, {"1", "+Inf"},
			{"1", "0.00000000000000000001"},
		} {
			if got := allow(t, "invalid", args[0], args[1]); !strings.HasPrefix(got, "ERR ") {
				t.Fatalf("arguments %v: got %s, want an error", args, got)
			}
		}
		if got := cli(t, "EXISTS", "invalid"); got != "0" {
			t.Fatalf("invalid arguments created a bucket: %s", got)
		}
	})

	t.Run("invalid state is not reset to a full bucket", func(t *testing.T) {
		cli(t, "HSET", "corrupt", "tokens", "invalid", "last_refill", "0")
		if got := allow(t, "corrupt", "2", "1"); got != "ERR invalid bucket state" {
			t.Fatalf("corrupt state = %s, want an error", got)
		}
		if got := cli(t, "HGET", "corrupt", "tokens"); got != "invalid" {
			t.Fatalf("corrupt state was overwritten: %s", got)
		}
		cli(t, "HSET", "incomplete", "tokens", "1")
		if got := allow(t, "incomplete", "2", "1"); got != "ERR invalid bucket state" {
			t.Fatalf("incomplete state = %s, want an error", got)
		}
	})
}

func startRateLimitRedis(t *testing.T) func(...string) (string, error) {
	t.Helper()
	server, err := exec.LookPath("redis-server")
	if err != nil {
		t.Skip("redis-server is required for Lua integration tests")
	}
	client, err := exec.LookPath("redis-cli")
	if err != nil {
		t.Skip("redis-cli is required for Lua integration tests")
	}
	dir := t.TempDir()
	socket := filepath.Join(dir, "redis.sock")
	logPath := filepath.Join(dir, "redis.log")
	ctx, stop := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, server, "--port", "0", "--unixsocket", socket,
		"--save", "", "--appendonly", "no", "--dir", dir, "--logfile", logPath)
	if err := cmd.Start(); err != nil {
		stop()
		t.Fatalf("start Redis: %v", err)
	}
	t.Cleanup(func() {
		stop()
		_ = cmd.Wait()
	})
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err := os.Stat(socket); err == nil {
			break
		}
		if time.Now().After(deadline) {
			log, _ := os.ReadFile(logPath)
			t.Fatalf("Redis did not start: %s", log)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return func(args ...string) (string, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		output, err := exec.CommandContext(ctx, client, append([]string{"--raw", "-s", socket}, args...)...).CombinedOutput()
		if err != nil {
			return "", fmt.Errorf("%w: %s", err, output)
		}
		return strings.TrimSpace(string(output)), nil
	}
}
