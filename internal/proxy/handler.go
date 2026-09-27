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
)

const maxBodyBytes = 1 << 20 // 1 MiB

// Handler forwards requests to an upstream LLM API, piping streamed
// (SSE) responses through to the client as they arrive.

type Handler struct {
	upstream *url.URL
	apiKey   string
	client   *http.Client
	logger   *slog.Logger
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

	for _, k := range []string{"Content-Type", "Cache-Control"} {
		if v := resp.Header.Get(k); v != "" {
			w.Header().Set(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	// Decide from what upstream actually sent, not from what the client
	// asked for: a failed stream request comes back as plain JSON.
	streamed := strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream")
	if streamed {
		err = pipeFlushing(w, resp.Body)
	} else {
		_, err = io.Copy(w, resp.Body)
	}
	if err != nil {
		if errors.Is(err, context.Canceled) {
			h.logger.Info("client disconnected mid-response", "path", r.URL.Path)
		} else {
			h.logger.Error("copying response failed", "err", err, "path", r.URL.Path)
		}
	}

	h.logger.Info("request done",
		"method", r.Method,
		"path", r.URL.Path,
		"model", meta.Model,
		"stream", streamed,
		"status", resp.StatusCode,
		"duration", time.Since(start),
	)
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
