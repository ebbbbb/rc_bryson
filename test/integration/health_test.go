//go:build integration

package integration

import (
	"net/http"
	"os"
	"testing"
	"time"
)

func TestComposeServiceHealth(t *testing.T) {
	client := &http.Client{Timeout: 5 * time.Second}
	assertStatus(t, client, envOr("APP_HEALTH_URL", "http://localhost:18080/healthz"), http.StatusNoContent)
}

func assertStatus(t *testing.T, client *http.Client, url string, expected int) {
	t.Helper()
	response, err := client.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer response.Body.Close()
	if response.StatusCode != expected {
		t.Fatalf("GET %s status = %d, want %d", url, response.StatusCode, expected)
	}
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
