package resilience

import (
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"
)

// RetryPolicy retries transient upstream failures with exponential backoff
// and full jitter, following the rules the official OpenAI SDKs use.
type RetryPolicy struct {
	MaxAttempts int // total tries including the first; 1 = no retries
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

// RetryableStatus reports whether an upstream status is worth retrying:
// timeouts, rate limits and server errors, never client errors.
func RetryableStatus(code int) bool {
	switch code {
	case http.StatusRequestTimeout, http.StatusTooManyRequests,
		http.StatusInternalServerError, http.StatusBadGateway,
		http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	}
	return false
}

// Wait returns how long to sleep before retry number attempt (1 = first
// retry), and false if retrying isn't worth it.
//
// Without a hint it picks uniformly from [0, BaseDelay*2^(attempt-1)],
// capped at MaxDelay. The randomness ("full jitter") stops many gateways
// that failed together from all retrying at the same instant and knocking
// a recovering upstream straight back down.
//
// An upstream Retry-After header wins; if it asks for longer than
// MaxDelay, don't retry and let the client see it instead.
func (p RetryPolicy) Wait(attempt int, retryAfter string) (time.Duration, bool) {
	if retryAfter != "" {
		if secs, err := strconv.Atoi(retryAfter); err == nil && secs >= 0 {
			d := time.Duration(secs) * time.Second
			return d, d <= p.MaxDelay
		}
	}

	ceiling := p.MaxDelay
	if shift := attempt - 1; shift < 30 { // avoid overflow on huge attempt counts
		ceiling = min(p.BaseDelay<<shift, p.MaxDelay)
	}
	if ceiling <= 0 {
		return 0, true
	}
	return rand.N(ceiling + 1), true
}
