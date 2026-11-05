package service

import (
	"context"
	"testing"
	"time"

	"xray-go/internal/model"
)

func TestInspectTimeout(t *testing.T) {
	slow := func(ctx context.Context, _ string) (string, string, map[string]any) {
		time.Sleep(200 * time.Millisecond)
		return model.Allow, "", nil
	}
	res := Inspect(context.Background(), "AI", 50*time.Millisecond, slow, model.InspectRequest{InspectionID: "INS-1"})
	if res.Result != model.Timeout {
		t.Fatalf("got %s, want TIMEOUT", res.Result)
	}
}

func TestInspectRetriesOnce(t *testing.T) {
	calls := 0
	flaky := func(ctx context.Context, _ string) (string, string, map[string]any) {
		calls++
		if calls == 1 {
			return model.Error, "boom", nil
		}
		return model.Hold, "ok", nil
	}
	res := Inspect(context.Background(), "AI", time.Second, flaky, model.InspectRequest{})
	if res.Result != model.Hold || calls != 2 {
		t.Fatalf("got %s after %d calls, want HOLD after 2", res.Result, calls)
	}
}

func TestInspectGivesUpAfterOneRetry(t *testing.T) {
	calls := 0
	broken := func(ctx context.Context, _ string) (string, string, map[string]any) {
		calls++
		return model.Error, "down", nil
	}
	res := Inspect(context.Background(), "AI", time.Second, broken, model.InspectRequest{})
	if res.Result != model.Error || calls != 2 || res.Reason != "engine unavailable: down" {
		t.Fatalf("got %s %q after %d calls", res.Result, res.Reason, calls)
	}
}
