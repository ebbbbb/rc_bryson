package outbound

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"reliable-notifier/internal/delivery"
)

type fixedResolver struct {
	addresses []net.IP
	err       error
}

func (resolver fixedResolver) LookupIP(context.Context, string, string) ([]net.IP, error) {
	return resolver.addresses, resolver.err
}

type fixedSecrets struct {
	value string
}

func (provider fixedSecrets) Resolve(context.Context, string) (string, error) {
	return provider.value, nil
}

func TestSenderUsesValidatedIPAndRegisteredHostname(t *testing.T) {
	certificate, err := tls.LoadX509KeyPair(
		fixturePath(t, "fake-supplier.crt"),
		fixturePath(t, "fake-supplier.key"),
	)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{certificate},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	requests := make(chan *http.Request, 1)
	bodies := make(chan []byte, 1)
	server := &http.Server{Handler: http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		body, readErr := io.ReadAll(request.Body)
		if readErr != nil {
			t.Errorf("read supplier request: %v", readErr)
		}
		requests <- request.Clone(request.Context())
		bodies <- body
		response.WriteHeader(http.StatusNoContent)
	})}
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		_ = server.Close()
	})

	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	sender, err := NewSender(
		fixedResolver{addresses: []net.IP{net.ParseIP("127.0.0.1")}},
		fixedSecrets{value: "Bearer runtime-secret"},
		true,
		fixturePath(t, "test-ca.crt"),
	)
	if err != nil {
		t.Fatal(err)
	}
	task, destination := testTaskAndDestination("https://fake-supplier.test:" + port + "/notify")
	result, err := sender.Send(t.Context(), task, destination)
	if err != nil {
		t.Fatal(err)
	}
	if result.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", result.StatusCode, http.StatusNoContent)
	}

	request := <-requests
	body := <-bodies
	if request.Method != http.MethodPost || request.Host != "fake-supplier.test:"+port {
		t.Fatalf("supplier request = %s %s, want registered HTTPS target", request.Method, request.Host)
	}
	if request.Header.Get("Authorization") != "Bearer runtime-secret" {
		t.Fatal("runtime credential was not injected")
	}
	if request.Header.Get("Idempotency-Key") != "stable-supplier-key" {
		t.Fatal("stable supplier idempotency value was not injected")
	}
	if request.Header.Get("X-Event-Type") != "registered" || string(body) != `{"exact":true}` {
		t.Fatalf("supplier payload headers/body mismatch: %v %q", request.Header, body)
	}
}

func TestSenderForbiddenPreconditionsMakeZeroConnections(t *testing.T) {
	tests := []struct {
		name       string
		rawURL     string
		policy     string
		testPolicy bool
		addresses  []net.IP
		headers    map[string]string
		wantError  string
	}{
		{
			name:       "HTTP URL",
			rawURL:     "http://fake-supplier.test/notify",
			policy:     testNetworkPolicy,
			testPolicy: true,
			addresses:  []net.IP{net.ParseIP("127.0.0.1")},
			wantError:  "absolute HTTPS",
		},
		{
			name:      "test policy disabled",
			rawURL:    "https://fake-supplier.test/notify",
			policy:    testNetworkPolicy,
			addresses: []net.IP{net.ParseIP("127.0.0.1")},
			wantError: "test-only destination policy",
		},
		{
			name:      "production private address",
			rawURL:    "https://supplier.example/notify",
			policy:    "public-internet",
			addresses: []net.IP{net.ParseIP("10.0.0.1")},
			wantError: "forbidden address",
		},
		{
			name:      "mixed DNS answers",
			rawURL:    "https://supplier.example/notify",
			policy:    "public-internet",
			addresses: []net.IP{net.ParseIP("203.0.113.8"), net.ParseIP("127.0.0.1")},
			wantError: "forbidden address",
		},
		{
			name:       "protected Header",
			rawURL:     "https://fake-supplier.test/notify",
			policy:     testNetworkPolicy,
			testPolicy: true,
			addresses:  []net.IP{net.ParseIP("127.0.0.1")},
			headers:    map[string]string{"authorization": "caller-secret"},
			wantError:  "protected Header",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sender, err := NewSender(
				fixedResolver{addresses: test.addresses},
				fixedSecrets{value: "secret"},
				test.testPolicy,
				fixturePath(t, "test-ca.crt"),
			)
			if err != nil {
				t.Fatal(err)
			}
			var dials atomic.Int32
			sender.dial = func(context.Context, string, string) (net.Conn, error) {
				dials.Add(1)
				return nil, net.ErrClosed
			}
			task, destination := testTaskAndDestination(test.rawURL)
			destination.NetworkPolicy = test.policy
			if test.headers != nil {
				task.CallerHeaders = test.headers
			}
			_, err = sender.Send(t.Context(), task, destination)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("error = %v, want containing %q", err, test.wantError)
			}
			if got := dials.Load(); got != 0 {
				t.Fatalf("dial attempts = %d, want zero", got)
			}
		})
	}
}

func TestSenderDoesNotFollowRedirect(t *testing.T) {
	var connections atomic.Int32
	var requests atomic.Int32
	listener, server := newTLSServer(t, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		response.Header().Set("Location", "https://fake-supplier.test/second")
		response.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer listener.Close()
	defer server.Close()

	_, port, _ := net.SplitHostPort(listener.Addr().String())
	sender, err := NewSender(
		fixedResolver{addresses: []net.IP{net.ParseIP("127.0.0.1")}},
		fixedSecrets{value: "secret"},
		true,
		fixturePath(t, "test-ca.crt"),
	)
	if err != nil {
		t.Fatal(err)
	}
	dialer := &net.Dialer{}
	sender.dial = func(ctx context.Context, network, address string) (net.Conn, error) {
		connections.Add(1)
		return dialer.DialContext(ctx, network, address)
	}
	task, destination := testTaskAndDestination("https://fake-supplier.test:" + port + "/redirect")
	result, err := sender.Send(t.Context(), task, destination)
	if err != nil {
		t.Fatal(err)
	}
	if result.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("status = %d, want redirect response", result.StatusCode)
	}
	if connections.Load() != 1 || requests.Load() != 1 {
		t.Fatalf("connections = %d, requests = %d, want one and one", connections.Load(), requests.Load())
	}
}

func newTLSServer(t *testing.T, handler http.Handler) (net.Listener, *http.Server) {
	t.Helper()
	certificate, err := tls.LoadX509KeyPair(
		fixturePath(t, "fake-supplier.crt"),
		fixturePath(t, "fake-supplier.key"),
	)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{certificate},
	})
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: time.Second,
	}
	go func() {
		_ = server.Serve(listener)
	}()
	return listener, server
}

func testTaskAndDestination(rawURL string) (delivery.Delivery, delivery.DestinationVersion) {
	return delivery.Delivery{
			ID:                       "delivery-1",
			Method:                   http.MethodPost,
			CallerHeaders:            map[string]string{"x-event-type": "registered"},
			Body:                     []byte(`{"exact":true}`),
			SupplierIdempotencyValue: "stable-supplier-key",
		}, delivery.DestinationVersion{
			URL:               rawURL,
			NetworkPolicy:     testNetworkPolicy,
			AllowedMethods:    []string{http.MethodPost},
			AllowedHeaders:    []string{"x-event-type"},
			SecretRef:         "env:SUPPLIER_SECRET",
			CredentialHeader:  "Authorization",
			IdempotencyHeader: "Idempotency-Key",
			ConnectTimeout:    time.Second,
			RequestTimeout:    2 * time.Second,
		}
}

func fixturePath(t *testing.T, name string) string {
	t.Helper()
	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(root, "..", "..", "testdata", "certs", name)
}
