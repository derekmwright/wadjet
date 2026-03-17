package auth

import (
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// RateLimitConfig configures per-identity rate limiting.
type RateLimitConfig struct {
	RequestsPerSecond float64       // max requests per second per identity
	Burst             int           // max burst size (token bucket capacity)
	CleanupInterval   time.Duration // how often to evict idle entries (default 5m)
}

type tokenBucket struct {
	tokens   float64
	lastTime time.Time
	rate     float64
	burst    int
}

func (b *tokenBucket) allow() bool {
	now := time.Now()
	elapsed := now.Sub(b.lastTime).Seconds()
	b.lastTime = now

	b.tokens += elapsed * b.rate
	if b.tokens > float64(b.burst) {
		b.tokens = float64(b.burst)
	}

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// RateLimiter tracks per-identity request rates.
type RateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*tokenBucket
	rate    float64
	burst   int
}

// NewRateLimiter creates a new per-identity rate limiter.
func NewRateLimiter(cfg RateLimitConfig) *RateLimiter {
	if cfg.Burst <= 0 {
		cfg.Burst = int(cfg.RequestsPerSecond) * 2
		if cfg.Burst < 10 {
			cfg.Burst = 10
		}
	}
	cleanupInterval := cfg.CleanupInterval
	if cleanupInterval <= 0 {
		cleanupInterval = 5 * time.Minute
	}

	rl := &RateLimiter{
		buckets: make(map[string]*tokenBucket),
		rate:    cfg.RequestsPerSecond,
		burst:   cfg.Burst,
	}

	go rl.cleanupLoop(cleanupInterval)
	return rl
}

// Allow checks if the identity is within its rate limit.
func (rl *RateLimiter) Allow(identity string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	b, ok := rl.buckets[identity]
	if !ok {
		b = &tokenBucket{
			tokens:   float64(rl.burst),
			lastTime: time.Now(),
			rate:     rl.rate,
			burst:    rl.burst,
		}
		rl.buckets[identity] = b
	}
	return b.allow()
}

func (rl *RateLimiter) cleanupLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		rl.mu.Lock()
		cutoff := time.Now().Add(-interval)
		for k, b := range rl.buckets {
			if b.lastTime.Before(cutoff) {
				delete(rl.buckets, k)
			}
		}
		rl.mu.Unlock()
	}
}

// RateLimitMiddleware returns HTTP middleware that enforces per-identity rate limits.
// Unauthenticated requests use the remote address as identity.
func RateLimitMiddleware(rl *RateLimiter, logger *slog.Logger) func(http.Handler) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Health/metrics endpoints skip rate limiting
			if r.URL.Path == "/v1/health" || r.URL.Path == "/metrics" {
				next.ServeHTTP(w, r)
				return
			}

			key := r.RemoteAddr
			if id := IdentityFromContext(r.Context()); id != nil {
				key = id.Name
			}

			if !rl.Allow(key) {
				logger.Warn("rate limit exceeded", "identity", key, "path", r.URL.Path)
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusTooManyRequests)
				w.Write([]byte(`{"error":"rate limit exceeded"}`))
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
