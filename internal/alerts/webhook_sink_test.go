package alerts

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestWebhookSinkSuccess(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		body, _ := io.ReadAll(r.Body)
		var f AlertFire
		if err := json.Unmarshal(body, &f); err != nil {
			t.Errorf("body not valid AlertFire: %v", err)
		}
		if r.Header.Get("X-Auth") != "secret" {
			t.Errorf("missing custom header")
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := NewWebhookSink("", srv.URL, map[string]string{"X-Auth": "secret"}, 10*time.Second)
	err := s.Deliver(context.Background(), AlertFire{AlertName: "a", EvaluatedAt: time.Now(), RowCount: 1})
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Errorf("want 1 call, got %d", calls)
	}
}

func TestWebhookSinkRetriesOn5xx(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	s := &WebhookSink{
		URL:     srv.URL,
		Client:  &http.Client{Timeout: 2 * time.Second},
		retries: 3,
		backoff: 1 * time.Millisecond, // speed up test
	}
	err := s.Deliver(context.Background(), AlertFire{AlertName: "a"})
	if err == nil {
		t.Fatal("want error after retries, got nil")
	}
	if atomic.LoadInt32(&calls) != 4 { // initial + 3 retries
		t.Errorf("want 4 calls, got %d", calls)
	}
}

func TestWebhookSinkConnectionError(t *testing.T) {
	s := &WebhookSink{
		URL:     "http://127.0.0.1:1", // unreachable
		Client:  &http.Client{Timeout: 200 * time.Millisecond},
		retries: 1,
		backoff: 1 * time.Millisecond,
	}
	err := s.Deliver(context.Background(), AlertFire{AlertName: "a"})
	if err == nil {
		t.Fatal("want connection error, got nil")
	}
}
