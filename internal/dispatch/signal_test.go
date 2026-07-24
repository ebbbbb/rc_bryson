package dispatch

import "testing"

func TestSignalValidationRejectsPoisonIdentifiers(t *testing.T) {
	tests := []Signal{
		{
			DeliveryID: "not-a-uuid",
			Generation: 0,
			TraceID:    "00000000-0000-4000-8000-000000000002",
		},
		{
			DeliveryID: "00000000-0000-4000-8000-000000000001",
			Generation: -1,
			TraceID:    "00000000-0000-4000-8000-000000000002",
		},
		{
			DeliveryID: "00000000-0000-4000-8000-000000000001",
			Generation: 0,
			TraceID:    "not-a-uuid",
		},
	}
	for _, signal := range tests {
		if err := signal.Validate(); err == nil {
			t.Fatalf("Validate(%+v) succeeded, want permanent format rejection", signal)
		}
	}
}

func TestSignalValidationAcceptsIdentifierOnlyMessage(t *testing.T) {
	signal := Signal{
		DeliveryID: "00000000-0000-4000-8000-000000000001",
		Generation: 0,
		TraceID:    "00000000-0000-4000-8000-000000000002",
	}
	if err := signal.Validate(); err != nil {
		t.Fatal(err)
	}
}
