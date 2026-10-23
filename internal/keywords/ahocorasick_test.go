package keywords

import (
	"reflect"
	"testing"
)

func TestFind(t *testing.T) {
	m := Build([]string{"free money", "he", "she", "hers", "OTP"})
	tests := []struct {
		text string
		want []string
	}{
		{"Get FREE   money now!", []string{"free money"}},
		{"get fr33 m0ney", []string{"free money"}},
		{"f.r.e.e money", []string{"free money"}},
		{"she said hers", []string{"she", "hers"}},
		{"freedom money", nil},         // whole words only
		{"ushers", nil},                // no partial-word hit
		{"send o​tp", []string{"otp"}}, // zero-width space removed
		{"", nil},
	}
	for _, tt := range tests {
		if got := m.Find(tt.text); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("Find(%q) = %v, want %v", tt.text, got, tt.want)
		}
	}
}

func TestNilMatcher(t *testing.T) {
	var m *Matcher
	if got := m.Find("anything"); got != nil {
		t.Fatalf("nil matcher found %v", got)
	}
}
