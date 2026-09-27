package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aakashsyadav1999/llmgate/internal/config"
	"github.com/aakashsyadav1999/llmgate/internal/middleware"
	"github.com/aakashsyadav1999/llmgate/internal/proxy"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	if err := run(logger); err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}

// run holds the real program so deferred cleanup always executes;
// main is the only place that calls os.Exit.
func run(logger *slog.Logger) error {
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           newHandler(cfg, logger),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       120 * time.Second,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("server starting", "addr", cfg.Addr, "upstream", cfg.UpstreamURL.String())
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return fmt.Errorf("server: %w", err)
	case <-ctx.Done():
	}

	stop() // restore default signal handling: a second Ctrl-C kills immediately
	logger.Info("shutting down", "timeout", cfg.ShutdownTimeout.String())

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		// Long-running streams didn't finish in time; drop them.
		logger.Warn("graceful shutdown incomplete, closing remaining connections", "err", err)
		if cerr := srv.Close(); cerr != nil {
			return fmt.Errorf("close: %w", cerr)
		}
	}

	logger.Info("stopped")
	return nil
}

// newHandler builds the routing and middleware chain. Health checks sit
// outside Auth so Docker/Caddy can probe without a client key.
func newHandler(cfg config.Config, logger *slog.Logger) http.Handler {
	api := http.NewServeMux()
	api.Handle("/v1/", proxy.NewHandler(
		cfg.UpstreamURL,
		cfg.UpstreamAPIKey,
		cfg.MaxIdleConnsPerHost,
		cfg.ResponseHeaderTimeout,
		logger,
	))

	limiter := middleware.NewLimiter(cfg.RateLimitBurst, cfg.RateLimitPerSecond)

	var protected http.Handler = api
	protected = middleware.RateLimit(limiter)(protected)
	protected = middleware.Auth(cfg.ClientAPIKeys)(protected)

	root := http.NewServeMux()
	root.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	root.Handle("/", protected)

	var h http.Handler = root
	h = middleware.Logging(logger)(h)
	h = middleware.RequestID(h)
	return h
}
