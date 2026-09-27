package proxy

import (
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
)

func New(upstream *url.URL, apiKey string, logger *slog.Logger) http.Handler {
	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(upstream)
			pr.Out.Header.Set("Authorization", "Bearer "+apiKey)
		},

		FlushInterval: -1,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			logger.Error("upstream error", "err", err, "method", r.Method, "path", r.URL.Path)
			http.Error(w, "bad gateway", http.StatusBadGateway)
		},
	}
}
