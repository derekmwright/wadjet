package alerts

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"time"
)

// WebhookSink POSTs an AlertFire JSON body to a URL with configurable headers
// and jittered exponential-backoff retries.
type WebhookSink struct {
	URL     string
	Headers map[string]string
	Client  *http.Client

	// retries is the number of retries after the initial attempt (3 in prod).
	retries int
	// backoff is the base backoff; doubled each attempt with +/-50% jitter.
	backoff time.Duration
}

// NewWebhookSink constructs a WebhookSink with production defaults: 3 retries,
// 200ms base backoff (→ 200, 800, 3200 ms), and the supplied HTTP timeout.
func NewWebhookSink(url string, headers map[string]string, timeout time.Duration) *WebhookSink {
	return &WebhookSink{
		URL:     url,
		Headers: headers,
		Client:  &http.Client{Timeout: timeout},
		retries: 3,
		backoff: 200 * time.Millisecond,
	}
}

func (*WebhookSink) Name() string { return "webhook" }

// Deliver POSTs the fire as JSON, retrying on network errors and non-2xx
// responses. Returns the last error after all retries are exhausted.
func (s *WebhookSink) Deliver(ctx context.Context, fire AlertFire) error {
	body, err := json.Marshal(fire)
	if err != nil {
		return fmt.Errorf("marshal AlertFire: %w", err)
	}

	var lastErr error
	for attempt := 0; attempt <= s.retries; attempt++ {
		if attempt > 0 {
			d := jitteredBackoff(s.backoff, attempt-1)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(d):
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.URL, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		for k, v := range s.Headers {
			req.Header.Set(k, v)
		}
		resp, err := s.Client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		resp.Body.Close()
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		lastErr = fmt.Errorf("webhook HTTP %d", resp.StatusCode)
	}
	return fmt.Errorf("webhook delivery failed after %d attempts: %w", s.retries+1, lastErr)
}

// jitteredBackoff returns base * 2^n +/- 50% jitter.
func jitteredBackoff(base time.Duration, n int) time.Duration {
	d := base << uint(n)
	jitter := time.Duration(rand.Int63n(int64(d)))
	return d/2 + jitter
}
