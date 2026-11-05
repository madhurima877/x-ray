package verdict

import (
	"testing"

	"xray-go/internal/model"
)

func r(engine, result, reason string) model.EngineResult {
	return model.EngineResult{InspectionID: "INS-1", MsgKey: "MSG-1", Engine: engine, Result: result, Reason: reason}
}

func TestDecide(t *testing.T) {
	tests := []struct {
		name    string
		results []model.EngineResult
		want    string
	}{
		{"all allow", []model.EngineResult{r("URL", "ALLOW", ""), r("KEYWORD", "ALLOW", ""), r("AI", "ALLOW", "")}, model.Allow},
		{"block wins over hold", []model.EngineResult{r("URL", "BLOCK", "Malicious domain"), r("KEYWORD", "HOLD", "kw")}, model.Block},
		{"hold", []model.EngineResult{r("URL", "ALLOW", ""), r("KEYWORD", "HOLD", "kw")}, model.Hold},
		{"timeout is fail-open", []model.EngineResult{r("URL", "ALLOW", ""), r("AI", "TIMEOUT", "")}, model.Allow},
	}
	for _, tt := range tests {
		m := map[string]model.EngineResult{}
		for _, x := range tt.results {
			m[x.Engine] = x
		}
		v := Decide(m)
		if v.Verdict != tt.want {
			t.Errorf("%s: got %s, want %s", tt.name, v.Verdict, tt.want)
		}
		if v.MsgKey != "MSG-1" || len(v.Details) != len(tt.results) {
			t.Errorf("%s: bad msgKey/details: %+v", tt.name, v)
		}
	}
	all := map[string]model.EngineResult{"URL": r("URL", "BLOCK", "Malicious domain"), "KEYWORD": r("KEYWORD", "ALLOW", ""), "AI": r("AI", "ALLOW", "")}
	if v := Decide(all); v.Reason != "Malicious domain" {
		t.Errorf("reason = %q", v.Reason)
	}

	// Missing or failed engines are named in the reason.
	v := Decide(map[string]model.EngineResult{"URL": r("URL", "BLOCK", "Malicious domain"), "AI": r("AI", "TIMEOUT", "")})
	if want := "Malicious domain (not checked: KEYWORD no response, AI timeout)"; v.Reason != want {
		t.Errorf("reason = %q, want %q", v.Reason, want)
	}
	if _, ok := v.Details["urlResult"]; !ok {
		t.Errorf("missing urlResult key: %v", v.Details)
	}
}
