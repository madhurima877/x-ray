package urls

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"syscall"
	"time"
)

var errPrivateAddr = errors.New("refusing to connect to a private address")

// Expander follows a URL's redirect chain without downloading pages.
type Expander struct {
	client  *http.Client
	maxHops int
}

// NewExpander builds an Expander. Unless allowPrivate is set it refuses to
// dial loopback, private and link-local addresses, so a message cannot make
// X-Ray probe the internal network.
func NewExpander(timeout time.Duration, maxHops int, allowPrivate bool) *Expander {
	dialer := &net.Dialer{Timeout: timeout}
	if !allowPrivate {
		dialer.Control = denyPrivate
	}
	tr := &http.Transport{
		DialContext:           dialer.DialContext,
		MaxIdleConns:          1000,
		MaxIdleConnsPerHost:   4,
		IdleConnTimeout:       30 * time.Second,
		TLSHandshakeTimeout:   timeout,
		ResponseHeaderTimeout: timeout,
	}
	return &Expander{
		client: &http.Client{
			Transport: tr,
			// We follow redirects ourselves so each hop can be checked.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		maxHops: maxHops,
	}
}

func denyPrivate(_, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	ip := net.ParseIP(host)
	if ip == nil || ip.IsLoopback() || ip.IsPrivate() || ip.IsUnspecified() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() {
		return errPrivateAddr
	}
	return nil
}

// Expand returns every URL in the redirect chain, starting with raw. On error
// the chain walked so far is returned too.
func (e *Expander) Expand(ctx context.Context, raw string) ([]string, error) {
	chain := []string{raw}
	cur := raw
	for i := 0; i < e.maxHops; i++ {
		resp, err := e.do(ctx, http.MethodHead, cur)
		if err == nil && resp.StatusCode == http.StatusMethodNotAllowed {
			resp, err = e.do(ctx, http.MethodGet, cur)
		}
		if err != nil {
			return chain, err
		}
		if resp.StatusCode < 300 || resp.StatusCode >= 400 {
			return chain, nil
		}
		loc := resp.Header.Get("Location")
		if loc == "" {
			return chain, nil
		}
		next, err := resp.Request.URL.Parse(loc)
		if err != nil {
			return chain, err
		}
		n, ok := Normalise(next.String())
		if !ok {
			return chain, fmt.Errorf("unsupported redirect target %q", loc)
		}
		chain = append(chain, n)
		cur = n
	}
	return chain, fmt.Errorf("more than %d redirects", e.maxHops)
}

func (e *Expander) do(ctx context.Context, method, u string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "xray-url-inspector/1.0")
	resp, err := e.client.Do(req)
	if err != nil {
		return nil, err
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
	resp.Body.Close()
	return resp, nil
}
