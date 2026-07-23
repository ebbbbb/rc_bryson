package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"reliable-notifier/internal/delivery"
)

func newMux(api *delivery.API, ready func(context.Context) error) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, request *http.Request) {
		if ready != nil {
			ctx, cancel := context.WithTimeout(request.Context(), time.Second)
			defer cancel()
			if err := ready(ctx); err != nil {
				http.Error(w, "not ready", http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusNoContent)
	})
	if api != nil {
		api.Register(mux)
	}
	return mux
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	databaseURL := envOr("DATABASE_URL", "postgres://notifier:notifier-test-only@localhost:15432/notifier?sslmode=disable")
	pool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		logger.Error("database pool configuration failed", "error", "invalid_database_configuration")
		os.Exit(1)
	}
	defer pool.Close()
	startupContext, cancelStartup := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelStartup()
	if err := pool.Ping(startupContext); err != nil {
		logger.Error("database connection failed", "error", "database_unavailable")
		os.Exit(1)
	}

	backlogLimit, err := strconv.ParseInt(envOr("BACKLOG_LIMIT", "100000"), 10, 64)
	if err != nil || backlogLimit <= 0 {
		logger.Error("invalid backlog limit", "error", "invalid_configuration")
		os.Exit(1)
	}
	store, err := delivery.NewStore(pool, backlogLimit)
	if err != nil {
		logger.Error("delivery store configuration failed", "error", "invalid_configuration")
		os.Exit(1)
	}
	if envOr("BOOTSTRAP_LOCAL_CONFIG", "false") == "true" {
		if err := store.Bootstrap(startupContext, localBootstrapConfig()); err != nil {
			logger.Error("local configuration bootstrap failed", "error", "bootstrap_failed")
			os.Exit(1)
		}
	}
	api, err := delivery.NewAPI(store, logger)
	if err != nil {
		logger.Error("delivery API configuration failed", "error", "invalid_configuration")
		os.Exit(1)
	}

	server := &http.Server{
		Addr:              envOr("HTTP_ADDR", ":8080"),
		Handler:           newMux(api, pool.Ping),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if envOr("PUBLISHER_ENABLED", "true") == "true" {
		go runPublisher(
			ctx,
			store,
			envOr("RABBITMQ_URL", "amqp://notifier:notifier-test-only@localhost:15672/"),
			logger,
		)
	}
	if envOr("WORKER_ENABLED", "true") == "true" {
		go runWorker(
			ctx,
			store,
			envOr("RABBITMQ_URL", "amqp://notifier:notifier-test-only@localhost:15672/"),
			logger,
		)
	}
	go runRetryScheduler(ctx, store, logger)
	go runLeaseReconciler(ctx, store, logger)

	errs := make(chan error, 1)
	go func() {
		errs <- server.ListenAndServe()
	}()

	select {
	case err := <-errs:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("HTTP server failed", "error", "listen_failed")
			os.Exit(1)
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("HTTP server shutdown failed", "error", "shutdown_failed")
			os.Exit(1)
		}
	}
}

func localBootstrapConfig() delivery.BootstrapConfig {
	return delivery.BootstrapConfig{
		CallerID:     envOr("BOOTSTRAP_CALLER_ID", "caller-a"),
		CallerAPIKey: envOr("BOOTSTRAP_CALLER_API_KEY", "caller-a-test-key"),
		Destination: delivery.DestinationVersion{
			DestinationID:     envOr("BOOTSTRAP_DESTINATION_ID", "supplier-a"),
			Version:           1,
			URL:               envOr("BOOTSTRAP_DESTINATION_URL", "https://fake-supplier.test:8443/notify"),
			NetworkPolicy:     "test-only-supplier",
			AllowedMethods:    []string{http.MethodPost},
			AllowedHeaders:    []string{"content-type", "x-event-type"},
			SecretRef:         "env:SUPPLIER_AUTH_VALUE",
			CredentialHeader:  "Authorization",
			IdempotencyHeader: "Idempotency-Key",
			SuccessStatuses:   []int32{},
			RetryStatuses:     []int32{408, 425, 429},
			ConnectTimeout:    3 * time.Second,
			RequestTimeout:    15 * time.Second,
			RatePerSecond:     1,
			MaxConcurrency:    1,
		},
	}
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
