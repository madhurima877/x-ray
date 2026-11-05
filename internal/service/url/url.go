// Package url is the URL engine: checks every link in a message.
package url

import (
	"context"
	"net"
	neturl "net/url"
	"regexp"
	"strings"

	"xray-go/internal/model"
)

// Known bad domains. Subdomains are blocked too (a.phishing.test).
var blockedDomains = []string{"malware.test", "phishing.test"}

// Link shorteners hide where the link really goes.
var shorteners = []string{"bit.ly", "tinyurl.com", "t.co", "goo.gl", "is.gd", "ow.ly", "han.gl", "me2.do"}

// Top-level domains often used for spam and phishing.
var suspiciousTLDs = []string{"zip", "mov", "xyz", "top", "click", "tk", "ml", "gq", "cf"}

// Finds links starting with http://, https:// or www.
var urlPattern = regexp.MustCompile(`(?i)\b(?:https?://|www\.)[^\s<>"']+`)

// Check finds every link and returns the worst result.
func Check(ctx context.Context, content string) (string, string, map[string]any) {
	links := urlPattern.FindAllString(content, -1)
	result, reason := model.Allow, "No malicious URL"
	details := map[string]any{"urls": links}

	for _, link := range links {
		r, why := checkOneURL(link)
		if stronger(r, result) {
			result, reason = r, why
			details["url"] = link
		}
		if result == model.Block {
			break
		}
	}
	return result, reason, details
}

// checkOneURL returns BLOCK, HOLD or ALLOW for a single link.
func checkOneURL(link string) (string, string) {
	if !strings.Contains(link, "://") {
		link = "http://" + link
	}
	u, err := neturl.Parse(link)
	if err != nil {
		return model.Hold, "Malformed URL"
	}
	host := strings.ToLower(u.Hostname())

	switch {
	case matchesDomain(host, blockedDomains):
		return model.Block, "Malicious domain"
	case u.User != nil:
		return model.Hold, "URL hides the real host (user@host)"
	case net.ParseIP(host) != nil:
		return model.Hold, "URL uses a raw IP address"
	case strings.Contains(host, "xn--"):
		return model.Hold, "Look-alike (punycode) domain"
	case matchesDomain(host, shorteners):
		return model.Hold, "Shortened URL hides destination"
	case hasTLD(host, suspiciousTLDs):
		return model.Hold, "Suspicious top-level domain"
	}
	return model.Allow, "No malicious URL"
}

// matchesDomain is true if host is one of the domains or a subdomain of one.
func matchesDomain(host string, domains []string) bool {
	for _, d := range domains {
		if host == d || strings.HasSuffix(host, "."+d) {
			return true
		}
	}
	return false
}

func hasTLD(host string, tlds []string) bool {
	tld := host[strings.LastIndex(host, ".")+1:]
	for _, t := range tlds {
		if tld == t {
			return true
		}
	}
	return false
}

// stronger is true if result a is worse than b (BLOCK > HOLD > ALLOW).
func stronger(a, b string) bool {
	rank := map[string]int{model.Allow: 0, model.Hold: 1, model.Block: 2}
	return rank[a] > rank[b]
}
