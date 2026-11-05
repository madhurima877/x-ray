package url

import (
	"context"
	"testing"

	"xray-go/internal/model"
)

func TestCheck(t *testing.T) {
	tests := []struct{ content, want string }{
		{"visit http://phishing.test/login", model.Block},
		{"https://a.malware.test", model.Block},
		{"go to www.phishing.test now", model.Block},
		{"https://notphishing.test", model.Allow},
		{"http://192.168.0.1/pay", model.Hold},
		{"click https://bit.ly/abc", model.Hold},
		{"https://xn--pple-43d.com", model.Hold},
		{"https://prize.xyz", model.Hold},
		{"https://bank.com@evil.com", model.Hold},
		{"https://bit.ly/x http://phishing.test", model.Block},
		{"see https://example.com", model.Allow},
		{"hello", model.Allow},
	}
	for _, tt := range tests {
		if got, reason, _ := Check(context.Background(), tt.content); got != tt.want {
			t.Errorf("%q: got %s (%s), want %s", tt.content, got, reason, tt.want)
		}
	}
}
