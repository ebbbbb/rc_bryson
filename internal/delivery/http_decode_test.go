package delivery

import (
	"strings"
	"testing"
)

func TestDecodeCallerHeadersRejectsDuplicateJSONNames(t *testing.T) {
	for _, raw := range []string{
		`{"X-Event-Type":"first","X-Event-Type":"second"}`,
		`{"X-Event-Type":"first","x-event-type":"second"}`,
	} {
		t.Run(raw, func(t *testing.T) {
			_, err := decodeCallerHeaders([]byte(raw))
			if err == nil || !strings.Contains(err.Error(), "duplicate") {
				t.Fatalf("decodeCallerHeaders(%s) = %v, want duplicate rejection", raw, err)
			}
		})
	}
}

func TestDecodeCallerHeadersPreservesRawValues(t *testing.T) {
	headers, err := decodeCallerHeaders(
		[]byte(`{"Content-Type":" application/json ","X-Event-Type":"registered"}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	if headers["Content-Type"] != " application/json " ||
		headers["X-Event-Type"] != "registered" {
		t.Fatalf("decoded Headers = %#v", headers)
	}
}
