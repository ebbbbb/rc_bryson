package main

import "testing"

func TestTestNetworkPolicyRequiresExplicitTestRuntime(t *testing.T) {
	tests := []struct {
		name        string
		environment string
		bootstrap   string
		allow       string
		want        bool
	}{
		{name: "production cannot opt in", environment: "production", bootstrap: "true", allow: "true"},
		{name: "empty environment cannot opt in", bootstrap: "true", allow: "true"},
		{name: "test requires bootstrap", environment: "test", allow: "true"},
		{name: "test requires explicit allowance", environment: "test", bootstrap: "true"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := allowTestNetworkPolicy(test.environment, test.bootstrap, test.allow)
			if got != test.want {
				t.Fatalf("allowTestNetworkPolicy() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestProductionBuildCannotEnableTestNetworkPolicy(t *testing.T) {
	if testNetworkPolicyBuildEnabled {
		t.Skip("testnetwork build variant has separate positive coverage")
	}
	if allowTestNetworkPolicy("test", "true", "true") {
		t.Fatal("production build enabled the test-only network policy")
	}
}
