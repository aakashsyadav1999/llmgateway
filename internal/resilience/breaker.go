// Package resilience holds the retry policy and circuit breaker that keep
// a flaky or dead upstream from turning into slow or failed requests.
package resilience

import (
	"sync"
	"time"
)

type State int

const (
	Closed   State = iota // healthy: requests flow
	Open                  // tripped: requests fail fast until the cooldown ends
	HalfOpen              // cooldown over: one probe request decides
)

func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case Open:
		return "open"
	default:
		return "half-open"
	}
}

// Outcome is what one upstream attempt says about upstream health.
type Outcome int

const (
	Success Outcome = iota // upstream answered (any status below 500)
	Failure                // transport error or 5xx
	Neutral                // says nothing about upstream, e.g. client hung up
)

// Breaker trips after threshold consecutive failures, rejects requests for
// cooldown, then lets a single probe through: success closes it, failure
// re-opens it. Every Allow that returns true must be followed by Record.
type Breaker struct {
	threshold int
	cooldown  time.Duration
	onChange  func(from, to State)

	mu       sync.Mutex
	state    State
	failures int
	openedAt time.Time
	probing  bool // a half-open probe is in flight
}

// NewBreaker returns a closed breaker. onChange (may be nil) is called on
// every state change, with the breaker's lock held, so it must not call
// back into the breaker.
func NewBreaker(threshold int, cooldown time.Duration, onChange func(from, to State)) *Breaker {
	return &Breaker{threshold: threshold, cooldown: cooldown, onChange: onChange}
}

// Allow reports whether a request may go to the upstream now.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case Open:
		if time.Since(b.openedAt) < b.cooldown {
			return false
		}
		b.setState(HalfOpen)
		b.probing = true
		return true
	case HalfOpen:
		if b.probing {
			return false // one probe at a time; the rest still fail fast
		}
		b.probing = true
		return true
	default:
		return true
	}
}

// Record reports the outcome of a request that Allow let through.
func (b *Breaker) Record(o Outcome) {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case HalfOpen:
		b.probing = false
		switch o {
		case Success:
			b.failures = 0
			b.setState(Closed)
		case Failure:
			b.trip()
		}
		// Neutral: no verdict; the next request becomes the probe.
	case Closed:
		switch o {
		case Success:
			b.failures = 0
		case Failure:
			b.failures++
			if b.failures >= b.threshold {
				b.trip()
			}
		}
	}
	// Open: a straggler that started before the trip; ignore it.
}

// RetryIn is how long until an open breaker lets a probe through, for the
// Retry-After header. 0 when not open.
func (b *Breaker) RetryIn() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != Open {
		return 0
	}
	return max(0, b.cooldown-time.Since(b.openedAt))
}

func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

func (b *Breaker) trip() {
	b.openedAt = time.Now()
	b.setState(Open)
}

func (b *Breaker) setState(s State) {
	if s == b.state {
		return
	}
	from := b.state
	b.state = s
	if b.onChange != nil {
		b.onChange(from, s)
	}
}
