// Package keyword is the keyword engine: looks for phishing and spam words.
package keyword

import (
	"context"
	"strings"

	"xray-go/internal/model"
)

// Phrases that are almost always phishing → BLOCK.
var blockKeywords = []string{
	"verify your account", "account suspended", "confirm your password",
	"send your otp", "계좌 비밀번호", "인증번호를 알려",
}

// Words common in spam/scams → HOLD for review.
var holdKeywords = []string{
	"lottery", "you have won", "free money", "loan approved", "urgent", "gift card",
	"당첨", "대출", "무료", "긴급", "택배 조회",
}

// Check returns BLOCK for phishing phrases, HOLD for spam words, else ALLOW.
func Check(ctx context.Context, content string) (string, string, map[string]any) {
	text := normalize(content)

	if found := findAll(text, blockKeywords); len(found) > 0 {
		return model.Block, "Phishing phrase", map[string]any{"keywords": found}
	}
	if found := findAll(text, holdKeywords); len(found) > 0 {
		return model.Hold, "Suspicious keyword", map[string]any{"keywords": found}
	}
	return model.Allow, "No suspicious keyword", nil
}

// normalize lowercases and removes spaces and separators,
// so "L o t-t e r y" still matches "lottery".
func normalize(s string) string {
	s = strings.ToLower(s)
	return strings.NewReplacer(" ", "", "\t", "", "\n", "", "-", "", "_", "", ".", "", "*", "").Replace(s)
}

func findAll(text string, keywords []string) []string {
	var found []string
	for _, k := range keywords {
		if strings.Contains(text, normalize(k)) {
			found = append(found, k)
		}
	}
	return found
}
