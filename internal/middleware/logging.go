package middleware

import (
	"log/slog"
	"net/http"
	"time"
)

type statusRecorder struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if code < 200 {
		r.ResponseWriter.WriteHeader(code)
		return
	}
	if !r.wrote {
		r.status = code
		r.wrote = true
		r.ResponseWriter.WriteHeader(code)
	}
}

func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }
func (r *statusRecorder) Write(p []byte) (int, error) {
	if !r.wrote {
		r.WriteHeader(200)
	}
	return r.ResponseWriter.Write(p)
}
func (r *statusRecorder) FlushError() error {
	if !r.wrote {
		r.WriteHeader(200)
	}
	return http.NewResponseController(r.ResponseWriter).Flush()
}

func Logging(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			start := time.Now()

			next.ServeHTTP(rec, r)

			logger.Info("http request",
				"request_id", RequestIDFromContext(r.Context()),
				"method", r.Method,
				"path", r.URL.Path,
				"status", rec.status,
				"duration", time.Since(start),
			)
		})
	}
}
