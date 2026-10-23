// Package urls finds URLs in message text (step 4) and expands short links
// by following redirects (step 8).
package urls

import (
	"net"
	"net/url"
	"regexp"
	"strings"
)

// MaxURLs caps how many URLs one message can make X-Ray check.
const MaxURLs = 10

// Matches http(s)://…, www.…, or a bare domain followed by a path (bit.ly/abc).
var urlRe = regexp.MustCompile(`(?i)(?:https?://[^\s<>"']+|www\.[^\s<>"']+|\b[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)*\.[a-z]{2,}/[^\s<>"']*)`)

// Extract returns the normalised, deduplicated URLs in text, at most MaxURLs.
func Extract(text string) []string {
	var out []string
	seen := map[string]bool{}
	for _, m := range urlRe.FindAllString(text, -1) {
		n, ok := Normalise(m)
		if !ok || seen[n] {
			continue
		}
		seen[n] = true
		out = append(out, n)
		if len(out) == MaxURLs {
			break
		}
	}
	return out
}

// Normalise turns a raw match into a canonical URL: scheme added if missing,
// lower-case scheme and host, default port, credentials and fragment removed,
// trailing punctuation from the sentence trimmed.
func Normalise(raw string) (string, bool) {
	raw = strings.TrimRight(raw, ".,;:!?)]}'\"")
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" {
		return "", false
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", false
	}
	host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	port := u.Port()
	if (scheme == "http" && port == "80") || (scheme == "https" && port == "443") {
		port = ""
	}
	if port != "" {
		u.Host = net.JoinHostPort(host, port)
	} else if strings.Contains(host, ":") {
		u.Host = "[" + host + "]"
	} else {
		u.Host = host
	}
	u.Scheme = scheme
	u.User = nil
	u.Fragment, u.RawFragment = "", ""
	if u.Path == "" {
		u.Path = "/"
	}
	return u.String(), true
}

// Host is the list-lookup key for a URL: lower-case host without "www.".
func Host(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return strings.TrimPrefix(strings.ToLower(u.Hostname()), "www.")
}
