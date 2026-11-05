package keyword

import (
	"context"
	"testing"

	"xray-go/internal/model"
)

func TestCheck(t *testing.T) {
	tests := []struct{ content, want string }{
		{"You won the LOTTERY", model.Hold},
		{"l o t-t e r y", model.Hold},
		{"Please verify your account today", model.Block},
		{"축하합니다 당첨되셨습니다", model.Hold},
		{"see you at lunch", model.Allow},
	}
	for _, tt := range tests {
		if got, reason, _ := Check(context.Background(), tt.content); got != tt.want {
			t.Errorf("%q: got %s (%s), want %s", tt.content, got, reason, tt.want)
		}
	}
}
