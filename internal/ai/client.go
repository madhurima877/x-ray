// Package ai calls the Python TinyBERT service that scores message text (step 9).
package ai

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Result struct {
	Score        float64 `json:"score"` // 0 = clean … 1 = spam/phishing
	ModelVersion string  `json:"model_version"`
}

type Client struct {
	url     string
	timeout time.Duration
	http    *http.Client
}

func New(url string, timeout time.Duration) *Client {
	return &Client{
		url:     url,
		timeout: timeout,
		http: &http.Client{Transport: &http.Transport{
			MaxIdleConns:        500,
			MaxIdleConnsPerHost: 500,
			IdleConnTimeout:     90 * time.Second,
		}},
	}
}

// Score sends the text and waits at most the configured timeout.
func (c *Client) Score(ctx context.Context, text string) (Result, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	body, _ := json.Marshal(map[string]string{"text": text})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return Result{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return Result{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		return Result{}, fmt.Errorf("ai service: status %d", resp.StatusCode)
	}
	var r Result
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&r); err != nil {
		return Result{}, fmt.Errorf("ai service: %w", err)
	}
	if r.Score < 0 || r.Score > 1 {
		return Result{}, fmt.Errorf("ai service: score %v out of range", r.Score)
	}
	return r, nil
}
