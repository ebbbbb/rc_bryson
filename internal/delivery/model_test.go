package delivery

import "testing"

func TestSupportedOutboundMethods(t *testing.T) {
	tests := []struct {
		method string
		want   bool
	}{
		{method: "POST", want: true},
		{method: "put", want: true},
		{method: "PATCH", want: true},
		{method: "DELETE", want: true},
		{method: "GET", want: false},
		{method: "HEAD", want: false},
		{method: "", want: false},
	}

	for _, test := range tests {
		t.Run(test.method, func(t *testing.T) {
			if got := IsSupportedOutboundMethod(test.method); got != test.want {
				t.Fatalf("IsSupportedOutboundMethod(%q) = %t, want %t", test.method, got, test.want)
			}
		})
	}
}
