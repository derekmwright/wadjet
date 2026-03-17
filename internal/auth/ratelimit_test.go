package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRateLimiterBasic(t *testing.T) {
	rl := NewRateLimiter(RateLimitConfig{
		RequestsPerSecond: 10,
		Burst:             5,
		CleanupInterval:   time.Hour, // won't trigger during test
	})

	// First 5 requests should pass (burst)
	for i := 0; i < 5; i++ {
		if !rl.Allow("user1") {
			t.Fatalf("request %d should be allowed within burst", i+1)
		}
	}

	// Sixth request should fail (burst exhausted)
	if rl.Allow("user1") {
		t.Fatal("request beyond burst should be denied")
	}
}

func TestRateLimiterDifferentIdentities(t *testing.T) {
	rl := NewRateLimiter(RateLimitConfig{
		RequestsPerSecond: 10,
		Burst:             2,
		CleanupInterval:   time.Hour,
	})

	// Each identity has its own bucket
	if !rl.Allow("user1") {
		t.Fatal("user1 first request should pass")
	}
	if !rl.Allow("user2") {
		t.Fatal("user2 first request should pass")
	}
	if !rl.Allow("user1") {
		t.Fatal("user1 second request should pass")
	}
	if !rl.Allow("user2") {
		t.Fatal("user2 second request should pass")
	}
	// Both should be exhausted now
	if rl.Allow("user1") {
		t.Fatal("user1 should be exhausted")
	}
	if rl.Allow("user2") {
		t.Fatal("user2 should be exhausted")
	}
}

func TestRateLimiterDefaultBurst(t *testing.T) {
	// When Burst <= 0, it should default to max(RPS*2, 10)
	rl := NewRateLimiter(RateLimitConfig{
		RequestsPerSecond: 3,
		Burst:             0, // should default to 10
		CleanupInterval:   time.Hour,
	})

	// Should allow at least 10 requests (default burst)
	allowed := 0
	for i := 0; i < 15; i++ {
		if rl.Allow("user1") {
			allowed++
		}
	}
	if allowed < 10 {
		t.Fatalf("expected at least 10 allowed (default burst), got %d", allowed)
	}
}

func TestRateLimiterHighRPS(t *testing.T) {
	rl := NewRateLimiter(RateLimitConfig{
		RequestsPerSecond: 100,
		Burst:             0, // should default to 200
		CleanupInterval:   time.Hour,
	})

	// Should allow burst=200 requests
	allowed := 0
	for i := 0; i < 300; i++ {
		if rl.Allow("user1") {
			allowed++
		}
	}
	if allowed < 200 {
		t.Fatalf("expected at least 200 allowed, got %d", allowed)
	}
}

func TestRateLimitMiddleware(t *testing.T) {
	rl := NewRateLimiter(RateLimitConfig{
		RequestsPerSecond: 100,
		Burst:             2,
		CleanupInterval:   time.Hour,
	})

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	wrapped := RateLimitMiddleware(rl, nil)(handler)

	// Health check should bypass rate limiting
	for i := 0; i < 10; i++ {
		r := httptest.NewRequest("GET", "/v1/health", nil)
		r.RemoteAddr = "1.2.3.4:1234"
		w := httptest.NewRecorder()
		wrapped.ServeHTTP(w, r)
		if w.Code != 200 {
			t.Fatalf("health check should bypass rate limit, got %d", w.Code)
		}
	}

	// Metrics endpoint should bypass rate limiting
	r := httptest.NewRequest("GET", "/metrics", nil)
	r.RemoteAddr = "1.2.3.4:1234"
	w := httptest.NewRecorder()
	wrapped.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatal("metrics should bypass rate limit")
	}

	// Normal requests should be rate limited (by RemoteAddr since no identity)
	for i := 0; i < 2; i++ {
		r := httptest.NewRequest("GET", "/v1/query", nil)
		r.RemoteAddr = "10.0.0.1:9999"
		w := httptest.NewRecorder()
		wrapped.ServeHTTP(w, r)
		if w.Code != 200 {
			t.Fatalf("request %d should pass, got %d", i+1, w.Code)
		}
	}

	// Third request should be rate limited
	r2 := httptest.NewRequest("GET", "/v1/query", nil)
	r2.RemoteAddr = "10.0.0.1:9999"
	w2 := httptest.NewRecorder()
	wrapped.ServeHTTP(w2, r2)
	if w2.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", w2.Code)
	}
	if w2.Header().Get("Retry-After") != "1" {
		t.Error("expected Retry-After header")
	}
}

func TestRateLimitMiddlewareWithIdentity(t *testing.T) {
	rl := NewRateLimiter(RateLimitConfig{
		RequestsPerSecond: 100,
		Burst:             1,
		CleanupInterval:   time.Hour,
	})

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	wrapped := RateLimitMiddleware(rl, nil)(handler)

	// Request with identity in context should use identity name for rate limiting
	id := &Identity{Name: "alice", Role: "admin"}
	r := httptest.NewRequest("GET", "/v1/query", nil)
	r = r.WithContext(context.WithValue(r.Context(), contextKey{}, id))
	w := httptest.NewRecorder()
	wrapped.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("first request should pass, got %d", w.Code)
	}

	// Second request should be rate limited for "alice"
	r2 := httptest.NewRequest("GET", "/v1/query", nil)
	r2 = r2.WithContext(context.WithValue(r2.Context(), contextKey{}, id))
	w2 := httptest.NewRecorder()
	wrapped.ServeHTTP(w2, r2)
	if w2.Code != http.StatusTooManyRequests {
		t.Fatalf("second request should be rate limited, got %d", w2.Code)
	}
}

func TestTokenBucketReplenish(t *testing.T) {
	b := &tokenBucket{
		tokens:   0,
		lastTime: time.Now().Add(-time.Second),
		rate:     10,
		burst:    10,
	}
	// After 1 second at rate 10, should have ~10 tokens
	if !b.allow() {
		t.Fatal("should be allowed after replenish")
	}
}

func TestTokenBucketCap(t *testing.T) {
	b := &tokenBucket{
		tokens:   0,
		lastTime: time.Now().Add(-10 * time.Second),
		rate:     10,
		burst:    5,
	}
	// Even though 100 tokens would be added, cap at burst=5
	allowed := 0
	for i := 0; i < 10; i++ {
		if b.allow() {
			allowed++
		}
	}
	if allowed > 5 {
		t.Fatalf("expected max 5 (burst), got %d", allowed)
	}
}
