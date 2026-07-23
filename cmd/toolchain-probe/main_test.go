package main

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
)

func TestProbeHTTPSForbiddenFixturesMakeZeroDialAttempts(t *testing.T) {
	tests := []struct {
		name        string
		url         string
		validatedIP string
		wantError   string
	}{
		{
			name:        "HTTP scheme",
			url:         "http://fake-supplier.test:18443/healthz",
			validatedIP: "127.0.0.1",
			wantError:   "test-only URL must name fake-supplier.test explicitly",
		},
		{
			name:        "unregistered host",
			url:         "https://localhost:18443/healthz",
			validatedIP: "127.0.0.1",
			wantError:   "test-only URL must name fake-supplier.test explicitly",
		},
		{
			name:        "invalid validated IP",
			url:         "https://fake-supplier.test:18443/healthz",
			validatedIP: "not-an-ip",
			wantError:   "validated test IP is invalid",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var dials atomic.Int32
			countingDial := func(context.Context, string, string) (net.Conn, error) {
				dials.Add(1)
				return nil, net.ErrClosed
			}

			err := probeHTTPSWithDialer(test.url, test.validatedIP, "missing-ca", countingDial)

			if err == nil {
				t.Fatal("expected fixture rejection")
			}
			if got := err.Error(); got != test.wantError {
				t.Fatalf("error = %q, want %q", got, test.wantError)
			}
			if got := dials.Load(); got != 0 {
				t.Fatalf("dial attempts = %d, want 0", got)
			}
		})
	}
}
