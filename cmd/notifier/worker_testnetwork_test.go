//go:build testnetwork

package main

import "testing"

func TestTestBuildRequiresAllRuntimeGuards(t *testing.T) {
	if !allowTestNetworkPolicy("test", "true", "true") {
		t.Fatal("test build did not enable explicitly guarded test policy")
	}
}
