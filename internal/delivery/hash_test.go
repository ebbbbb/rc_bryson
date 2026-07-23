package delivery

import (
	"bytes"
	"errors"
	"testing"
)

func TestRequestHashCanonicalizationAndBoundaries(t *testing.T) {
	first, err := RequestHash(HashInput{
		DestinationID:      "supplier-a",
		DestinationVersion: 1,
		Method:             "post",
		CallerHeaders: map[string]string{
			" X-Event-Type ": "  registered  ",
			"Content-Type":   "application/json",
		},
		Body: []byte("{ \"raw\": true }\n"),
	})
	if err != nil {
		t.Fatal(err)
	}
	equivalent, err := RequestHash(HashInput{
		DestinationID:      "supplier-a",
		DestinationVersion: 1,
		Method:             "POST",
		CallerHeaders: map[string]string{
			"content-type": "application/json",
			"x-event-type": "registered",
		},
		Body: []byte("{ \"raw\": true }\n"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first[:], equivalent[:]) {
		t.Fatal("equivalent canonical requests produced different hashes")
	}

	changedVersion, err := RequestHash(HashInput{
		DestinationID:      "supplier-a",
		DestinationVersion: 2,
		Method:             "POST",
		CallerHeaders: map[string]string{
			"content-type": "application/json",
			"x-event-type": "registered",
		},
		Body: []byte("{ \"raw\": true }\n"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(first[:], changedVersion[:]) {
		t.Fatal("destination version was not included in request hash")
	}

	changedRawBody, err := RequestHash(HashInput{
		DestinationID:      "supplier-a",
		DestinationVersion: 1,
		Method:             "POST",
		CallerHeaders: map[string]string{
			"content-type": "application/json",
			"x-event-type": "registered",
		},
		Body: []byte("{\"raw\":true}\n"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(first[:], changedRawBody[:]) {
		t.Fatal("raw body bytes were not included exactly")
	}

	left, err := RequestHash(HashInput{
		DestinationID:      "ab",
		DestinationVersion: 1,
		Method:             "C",
	})
	if err != nil {
		t.Fatal(err)
	}
	right, err := RequestHash(HashInput{
		DestinationID:      "a",
		DestinationVersion: 1,
		Method:             "BC",
	})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(left[:], right[:]) {
		t.Fatal("length-delimited fields collided")
	}
}

func TestRequestHashRejectsCaseInsensitiveDuplicateHeaders(t *testing.T) {
	_, err := RequestHash(HashInput{
		DestinationID:      "supplier-a",
		DestinationVersion: 1,
		Method:             "POST",
		CallerHeaders: map[string]string{
			"X-Event-Type": "one",
			"x-event-type": "two",
		},
	})
	if err == nil {
		t.Fatal("expected duplicate caller Header rejection")
	}
}

func TestCanonicalCallerHeadersRejectsInvalidWireValues(t *testing.T) {
	for _, headers := range []map[string]string{
		{"bad header": "value"},
		{"x-event": "safe\r\nAuthorization: leaked"},
		{"": "value"},
	} {
		if _, err := CanonicalCallerHeaders(headers); !errors.Is(err, ErrInvalidHeader) {
			t.Fatalf("headers %v error = %v, want ErrInvalidHeader", headers, err)
		}
	}
}
