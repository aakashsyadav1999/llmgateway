package proxy

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"sync"
	"time"

	"github.com/aakashsyadav1999/llmgate/internal/resilience"
)

// Upstream describes one OpenAI-compatible endpoint and its credentials.
// Separate entries may use the same URL with different API keys.
type Upstream struct {
	Name    string
	URL     *url.URL
	APIKey  string
	Breaker *resilience.Breaker
}

// Configuration is immutable once the handler begins serving requests.
type upstreamPool struct {
	upstreams []Upstream
	scope     string
	mu        sync.Mutex
	next      int
}

func newUpstreamPool(upstreams []Upstream) *upstreamPool {
	if len(upstreams) == 0 {
		panic("proxy: at least one upstream is required")
	}
	pool := &upstreamPool{upstreams: append([]Upstream(nil), upstreams...)}
	identity := make([][3]string, len(upstreams))
	for i, upstream := range upstreams {
		if upstream.URL == nil {
			panic("proxy: upstream URL is required")
		}
		identity[i] = [3]string{upstream.Name, upstream.URL.String(), upstream.APIKey}
	}
	encoded, _ := json.Marshal(identity)
	digest := sha256.Sum256(encoded)
	pool.scope = hex.EncodeToString(digest[:])
	return pool
}

// acquire advances round-robin selection and reserves any half-open probe
// in the same operation. Every successful acquire must record an outcome.
func (p *upstreamPool) acquire() *Upstream {
	p.mu.Lock()
	defer p.mu.Unlock()
	for offset := 0; offset < len(p.upstreams); offset++ {
		index := (p.next + offset) % len(p.upstreams)
		upstream := &p.upstreams[index]
		if upstream.Breaker != nil && !upstream.Breaker.Allow() {
			continue
		}
		p.next = (index + 1) % len(p.upstreams)
		return upstream
	}
	return nil
}

func (p *upstreamPool) retryIn() time.Duration {
	var earliest time.Duration
	found := false
	for _, upstream := range p.upstreams {
		if upstream.Breaker == nil {
			continue
		}
		delay := upstream.Breaker.RetryIn()
		if !found || delay < earliest {
			earliest = delay
			found = true
		}
	}
	return earliest
}
