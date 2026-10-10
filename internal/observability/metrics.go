package observability

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/aakashsyadav1999/llmgate/internal/usage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	Registry  *prometheus.Registry
	requests  *prometheus.CounterVec
	duration  *prometheus.HistogramVec
	cache     *prometheus.CounterVec
	rejected  *prometheus.CounterVec
	tokens    *prometheus.CounterVec
	cost      *prometheus.CounterVec
	responses *prometheus.CounterVec
	clients   map[string]string
	prices    map[string]map[string]usage.Price
}

func New(keys map[string]bool, prices map[string]map[string]usage.Price) *Metrics {
	m := &Metrics{Registry: prometheus.NewRegistry(), clients: map[string]string{}, prices: prices}
	for key, allowed := range keys {
		if allowed {
			sum := sha256.Sum256([]byte(key))
			m.clients[key] = fmt.Sprintf("%x", sum[:16])
		}
	}
	counter := func(name, help string, labels ...string) *prometheus.CounterVec {
		c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmgate_" + name, Help: help}, labels)
		m.Registry.MustRegister(c)
		return c
	}
	m.requests = counter("requests_total", "Completed gateway requests.", "method", "route", "status")
	m.duration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "llmgate_request_duration_seconds", Help: "Gateway request duration including stream delivery.", Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120}}, []string{"method", "route"})
	m.Registry.MustRegister(m.duration, prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	m.cache = counter("cache_requests_total", "Cache decisions.", "result")
	m.rejected = counter("rate_limit_rejections_total", "Requests rejected by the gateway limiter.", "client")
	m.tokens = counter("tokens_total", "Observed upstream tokens; cached_input is a subset of input.", "client", "upstream", "model", "kind")
	m.cost = counter("estimated_cost_usd_total", "Estimated upstream cost at configured prices.", "client", "upstream", "model")
	m.responses = counter("usage_responses_total", "Upstream success usage accounting outcomes.", "client", "upstream", "result")
	return m
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}
func (m *Metrics) client(key string) string {
	if id := m.clients[key]; id != "" {
		return id
	}
	return "unknown"
}
func (m *Metrics) Cache(result string) {
	if m != nil {
		m.cache.WithLabelValues(result).Inc()
	}
}
func (m *Metrics) Reject(key string) {
	if m != nil {
		m.rejected.WithLabelValues(m.client(key)).Inc()
	}
}
func (m *Metrics) Account(key, upstream, model string, o *usage.Observer) {
	if m == nil {
		return
	}
	client := m.client(key)
	if !o.Found {
		m.responses.WithLabelValues(client, upstream, "missing").Inc()
		return
	}
	p, priced := m.prices[upstream][model]
	if !priced {
		p, priced = m.prices["*"][model]
	}
	label, result := model, "priced"
	if !priced {
		label = "unpriced"
		result = "unpriced"
	}
	m.responses.WithLabelValues(client, upstream, result).Inc()
	for kind, n := range map[string]int64{"input": o.Tokens.Input, "output": o.Tokens.Output, "cached_input": o.Tokens.CachedInput} {
		m.tokens.WithLabelValues(client, upstream, label, kind).Add(float64(n))
	}
	if priced {
		m.cost.WithLabelValues(client, upstream, label).Add(p.Cost(o.Tokens))
	}
}

type writer struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (w *writer) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *writer) WriteHeader(code int) {
	if code < 200 {
		w.ResponseWriter.WriteHeader(code)
		return
	}
	if !w.wrote {
		w.status = code
		w.wrote = true
		w.ResponseWriter.WriteHeader(code)
	}
}
func (w *writer) Write(p []byte) (int, error) {
	if !w.wrote {
		w.WriteHeader(200)
	}
	return w.ResponseWriter.Write(p)
}
func (w *writer) FlushError() error {
	if !w.wrote {
		w.WriteHeader(200)
	}
	return http.NewResponseController(w.ResponseWriter).Flush()
}

func (m *Metrics) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}
		method := r.Method
		switch method {
		case "GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS":
		default:
			method = "other"
		}
		route := r.URL.Path
		switch route {
		case "/healthz", "/v1/chat/completions", "/v1/completions", "/v1/embeddings", "/v1/models":
		default:
			route = "other"
		}
		rec := &writer{ResponseWriter: w, status: 200}
		start := time.Now()
		next.ServeHTTP(rec, r)
		m.requests.WithLabelValues(method, route, strconv.Itoa(rec.status)).Inc()
		m.duration.WithLabelValues(method, route).Observe(time.Since(start).Seconds())
	})
}
