package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aakashsyadav1999/llmgate/internal/cache"
	"github.com/aakashsyadav1999/llmgate/internal/middleware"
)

const maxBodyBytes = 1 << 20 // 1 MiB

// Handler forwards requests to an upstream LLM API, piping streamed
// (SSE) responses through to the client as they arrive.

type Handler struct {
	upstream *url.URL
	apiKey   string
	client   *http.Client
	logger   *slog.Logger
	cache    *cache.Cache
}

func NewHandler(
	upstream *url.URL,
	apiKey string,
	maxIdleConnsPerHost int,
	responseHeaderTimeout time.Duration,
	logger *slog.Logger,
) *Handler {
	tr := http.DefaultTransport.(*http.Transport).Clone()

	tr.MaxIdleConnsPerHost = maxIdleConnsPerHost
	tr.ResponseHeaderTimeout = responseHeaderTimeout

	return &Handler{
		upstream: upstream,
		apiKey:   apiKey,
		client:   &http.Client{Transport: tr},
		logger:   logger,
	}
}

// WithCache enables response caching; see package cache for what is cached.
func (h *Handler) WithCache(c *cache.Cache) *Handler {
	h.cache = c
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	start := time.Now()
	// Forward the request to the upstream LLM API
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			h.logger.Error("request body too large", "error", err)
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "cannot read request body", http.StatusBadRequest)
		return
	}

	// Peek at the fields we care about. A parse failure is not fatal:
	// the upstream will produce the proper error for a malformed body.
	var meta struct {
		Model  string `json:"model"`
		Stream bool   `json:"stream"`
	}
	if len(body) > 0 {
		_ = json.Unmarshal(body, &meta)
	}

	cacheKey, cacheStatus := h.lookupCache(w, r, body, meta.Model)
	if cacheStatus == "hit" {
		return // served from cache
	}

	target := h.upstream.JoinPath(r.URL.Path)
	target.RawQuery = r.URL.RawQuery

	req, err := http.NewRequestWithContext(r.Context(), r.Method, target.String(), bytes.NewReader(body))
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	// Forward the client's headers, but not the API key.
	for _, k := range []string{"Content-Type", "Accept"} {
		if v := r.Header.Get(k); v != "" {
			req.Header.Set(k, v)
		}
	}
	req.Header.Set("Authorization", "Bearer "+h.apiKey)

	resp, err := h.client.Do(req)
	if err != nil {
		if r.Context().Err() != nil {
			h.logger.Info("client went away before upstream", "path", r.URL.Path)
			return
		}
		h.logger.Error("upstream request failed", "err", err, "path", r.URL.Path)
		http.Error(w, "bad gateway", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	for _, k := range []string{"Content-Type", "Cache-Control", "Retry-After"} {
		if v := resp.Header.Get(k); v != "" {
			w.Header().Set(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	// Decide from what upstream actually sent, not from what the client
	// asked for: a failed stream request comes back as plain JSON.
	streamed := strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream")

	// Record a cacheable success while relaying it, so nothing is buffered
	// before the client sees it.
	var src io.Reader = resp.Body
	var rec *recorder
	if cacheKey != "" && resp.StatusCode == http.StatusOK {
		rec = &recorder{}
		src = io.TeeReader(src, rec)
	}

	if streamed {
		err = pipeFlushing(w, src)
	} else {
		_, err = io.Copy(w, src)
	}
	if err != nil {
		if errors.Is(err, context.Canceled) {
			h.logger.Info("client disconnected mid-response", "path", r.URL.Path)
		} else {
			h.logger.Error("copying response failed", "err", err, "path", r.URL.Path)
		}
	}

	// Only store responses that arrived whole: an error or client hang-up
	// mid-copy, or a stream without its end marker, would be truncated.
	if rec != nil && err == nil && rec.complete(streamed) {
		h.cache.Set(r.Context(), cacheKey, &cache.Entry{
			ContentType: resp.Header.Get("Content-Type"),
			Body:        rec.buf.Bytes(),
		})
	}

	h.logger.Info("request done",
		"method", r.Method,
		"path", r.URL.Path,
		"model", meta.Model,
		"stream", streamed,
		"status", resp.StatusCode,
		"cache", cacheStatus,
		"duration", time.Since(start),
	)
}

// lookupCache serves the request from cache when possible. It returns the
// key to store the upstream response under ("" = don't store) and the
// cache status: off, bypass, miss, or hit (response already written).
func (h *Handler) lookupCache(w http.ResponseWriter, r *http.Request, body []byte, model string) (string, string) {
	if h.cache == nil {
		return "", "off"
	}
	start := time.Now()

	key, ok := cache.Key(middleware.ClientKeyFromContext(r.Context()), r.Method, r.URL.Path, body)
	cc := r.Header.Get("Cache-Control")

	status := "bypass"
	switch {
	case !ok || strings.Contains(cc, "no-store"):
		key = ""
	case strings.Contains(cc, "no-cache"):
		// Skip the read but still store the fresh response.
	default:
		if e, hit := h.cache.Get(r.Context(), key); hit {
			h.serveCached(w, r, e, model, start)
			return key, "hit"
		}
		status = "miss"
	}
	w.Header().Set("X-Cache", strings.ToUpper(status))
	return key, status
}

// serveCached replays a stored response. A stored SSE stream is written in
// one go; clients parse it event by event just as if it had trickled in.
func (h *Handler) serveCached(w http.ResponseWriter, r *http.Request, e *cache.Entry, model string, start time.Time) {
	w.Header().Set("Content-Type", e.ContentType)
	w.Header().Set("X-Cache", "HIT")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(e.Body); err != nil {
		h.logger.Info("client disconnected mid-response", "path", r.URL.Path)
	}

	h.logger.Info("request done",
		"method", r.Method,
		"path", r.URL.Path,
		"model", model,
		"stream", strings.HasPrefix(e.ContentType, "text/event-stream"),
		"status", http.StatusOK,
		"cache", "hit",
		"duration", time.Since(start),
	)
}

// recorder captures a response body for caching, giving up (but never
// failing the copy) once it exceeds cache.MaxEntryBytes.
type recorder struct {
	buf      bytes.Buffer
	overflow bool
}

func (rc *recorder) Write(p []byte) (int, error) {
	if !rc.overflow {
		if rc.buf.Len()+len(p) > cache.MaxEntryBytes {
			rc.overflow = true
			rc.buf.Reset()
		} else {
			rc.buf.Write(p)
		}
	}
	return len(p), nil
}

// complete reports whether the recording is a whole response. Streams must
// end with OpenAI's "data: [DONE]" marker; anything else was cut short.
func (rc *recorder) complete(streamed bool) bool {
	if rc.overflow {
		return false
	}
	if streamed {
		return bytes.HasSuffix(bytes.TrimSpace(rc.buf.Bytes()), []byte("data: [DONE]"))
	}
	return true
}

// pipeFlushing copies src to w and flushes after every read, so each
// chunk from upstream reaches the client immediately.
func pipeFlushing(w http.ResponseWriter, src io.Reader) error {
	rc := http.NewResponseController(w)
	// Send headers now so the client sees the stream open before the first token.
	if err := rc.Flush(); err != nil {
		return err
	}

	buf := make([]byte, 4096)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return werr
			}
			if ferr := rc.Flush(); ferr != nil {
				return ferr
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}
