package usage

import (
	"strings"
	"testing"
)

func TestJSONUsage(t *testing.T) {
	for _, tc := range []struct {
		body   string
		found  bool
		tokens Tokens
	}{
		{`{"usage":{"prompt_tokens":10,"completion_tokens":4,"prompt_tokens_details":{"cached_tokens":3}}}`, true, Tokens{10, 4, 3}},
		{`{"usage":{"input_tokens":6,"output_tokens":2,"input_tokens_details":{"cached_tokens":1}}}`, true, Tokens{6, 2, 1}},
		{`{"usage":{"prompt_tokens":5,"total_tokens":5}}`, true, Tokens{5, 0, 0}},
		{`{"usage":null}`, false, Tokens{}},
		{`{"usage":{"prompt_tokens":-1}}`, false, Tokens{}},
		{`{"usage":{"prompt_tokens":1,"prompt_tokens_details":{"cached_tokens":2}}}`, false, Tokens{}},
		{`{"usage":{"total_tokens":3}}`, false, Tokens{}},
	} {
		o := new(Observer)
		o.Write([]byte(tc.body))
		o.Finish()
		if o.Found != tc.found || o.Tokens != tc.tokens {
			t.Fatalf("%s: %+v", tc.body, o)
		}
	}
}

func TestSSEChunksAndCumulativeUsage(t *testing.T) {
	body := "data: {\"model\":\"demo\",\"usage\":null}\r\n\r\n" +
		"data: {\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":1}}\n\n" +
		"data: {\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":4}}\n\n" + "data: [DONE]\n\n"
	for step := 1; step <= len(body); step++ {
		o := &Observer{Stream: true}
		for i := 0; i < len(body); i += step {
			o.Write([]byte(body[i:min(i+step, len(body))]))
		}
		o.Finish()
		if !o.Found || o.Tokens != (Tokens{10, 4, 0}) || o.Model != "demo" {
			t.Fatalf("step %d: %+v", step, o)
		}
	}
}

func TestObserverBoundsAndRecovery(t *testing.T) {
	o := &Observer{Stream: true}
	o.Write([]byte("data: " + strings.Repeat("x", limit+100) + "\n\n"))
	o.Write([]byte("data: {\"usage\":{\"prompt_tokens\":2}}\n\n"))
	o.Finish()
	if !o.Found || o.Tokens.Input != 2 {
		t.Fatal("did not recover after oversized event")
	}
	j := new(Observer)
	j.Write([]byte(strings.Repeat("x", limit+1)))
	j.Finish()
	if j.Found || len(j.buf) != 0 {
		t.Fatal("oversized JSON retained")
	}
}

func TestCostCachedInput(t *testing.T) {
	discount := 0.5
	p := Price{Input: 2, Output: 5, CachedInput: &discount}
	if cost := p.Cost(Tokens{1000000, 200000, 400000}); cost != 2.4 {
		t.Fatalf("cost %g", cost)
	}
}
