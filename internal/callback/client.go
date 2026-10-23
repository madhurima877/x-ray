// Package callback POSTs the final verdict to the relay (step 11).
package callback

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"xray/internal/model"
)

type Client struct {
	http    *http.Client
	retries int
	backoff time.Duration
}

func New(retries int) *Client {
	return &Client{
		http: &http.Client{Transport: &http.Transport{
			MaxIdleConns:        500,
			MaxIdleConnsPerHost: 200,
			IdleConnTimeout:     90 * time.Second,
		}},
		retries: retries,
		backoff: 100 * time.Millisecond,
	}
}

// Send posts v to url, retrying network errors, 429 and 5xx a few times
// with a short linear backoff. Other 4xx answers are not retried.
func (c *Client) Send(ctx context.Context, url string, v model.Verdict) error {
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}
	var lastErr error
	for attempt := 0; attempt <= c.retries; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(c.backoff * time.Duration(attempt)):
			case <-ctx.Done():
				return fmt.Errorf("callback: %w (last error: %v)", ctx.Err(), lastErr)
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", v.MessageID)
		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		resp.Body.Close()
		if resp.StatusCode < 300 {
			return nil
		}
		lastErr = fmt.Errorf("callback: status %d", resp.StatusCode)
		if resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests {
			return lastErr
		}
	}
	return lastErr
}
