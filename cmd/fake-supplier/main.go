package main

import (
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"
)

func newMux() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

func main() {
	addr := envOr("HTTPS_ADDR", ":8443")
	certFile := envOr("TLS_CERT_FILE", "/certs/fake-supplier.crt")
	keyFile := envOr("TLS_KEY_FILE", "/certs/fake-supplier.key")
	server := &http.Server{
		Addr:              addr,
		Handler:           newMux(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	if err := server.ListenAndServeTLS(certFile, keyFile); !errors.Is(err, http.ErrServerClosed) {
		slog.Error("fake HTTPS supplier failed", "error", err)
		os.Exit(1)
	}
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
