package expr

import (
	"math"
	"testing"
)

func TestRegisteredDomain(t *testing.T) {
	fn := DefaultRegistry.Lookup("registered_domain")
	if fn == nil {
		t.Fatal("registered_domain not registered")
	}

	tests := []struct {
		input string
		want  string
	}{
		{"mail.google.com", "google.com"},
		{"sub.example.co.uk", "example.co.uk"},
		{"a.b.c.evil.com", "evil.com"},
		{"example.com", "example.com"},
		{"deep.sub.domain.amazon.co.jp", "amazon.co.jp"},
		{"MAIL.GOOGLE.COM", "google.com"},        // case insensitive
		{"mail.google.com.", "google.com"},        // trailing dot
		{"crypto-mine.evil.org", "evil.org"},      // hyphens
		{"localhost", ""},                          // single label → nil
	}
	for _, tt := range tests {
		got := fn([]any{tt.input})
		if tt.want == "" {
			if got != nil {
				t.Errorf("registered_domain(%q) = %v, want nil", tt.input, got)
			}
		} else if got != tt.want {
			t.Errorf("registered_domain(%q) = %v, want %q", tt.input, got, tt.want)
		}
	}

	// NULL propagation
	if got := fn([]any{nil}); got != nil {
		t.Errorf("registered_domain(nil) = %v, want nil", got)
	}
}

func TestTLD(t *testing.T) {
	fn := DefaultRegistry.Lookup("tld")
	if fn == nil {
		t.Fatal("tld not registered")
	}

	tests := []struct {
		input string
		want  string
	}{
		{"mail.google.com", "com"},
		{"sub.example.co.uk", "co.uk"},
		{"example.org", "org"},
		{"deep.sub.amazon.co.jp", "co.jp"},
		{"example.com.", "com"},                 // trailing dot
		{"EXAMPLE.COM", "com"},                  // case insensitive
	}
	for _, tt := range tests {
		got := fn([]any{tt.input})
		if got != tt.want {
			t.Errorf("tld(%q) = %v, want %q", tt.input, got, tt.want)
		}
	}

	if got := fn([]any{nil}); got != nil {
		t.Errorf("tld(nil) = %v, want nil", got)
	}
}

func TestSubdomain(t *testing.T) {
	fn := DefaultRegistry.Lookup("subdomain")
	if fn == nil {
		t.Fatal("subdomain not registered")
	}

	tests := []struct {
		input string
		want  string
	}{
		{"mail.google.com", "mail"},
		{"a.b.c.evil.com", "a.b.c"},
		{"sub.example.co.uk", "sub"},
		{"a.b.c.example.co.uk", "a.b.c"},
		{"example.com", ""},                     // no subdomain
		{"google.co.uk", ""},                    // no subdomain
		{"DEEP.SUB.EXAMPLE.COM", "deep.sub"},    // case insensitive
	}
	for _, tt := range tests {
		got := fn([]any{tt.input})
		if got != tt.want {
			t.Errorf("subdomain(%q) = %v, want %q", tt.input, got, tt.want)
		}
	}

	if got := fn([]any{nil}); got != nil {
		t.Errorf("subdomain(nil) = %v, want nil", got)
	}
}

func TestDomainDepth(t *testing.T) {
	fn := DefaultRegistry.Lookup("domain_depth")
	if fn == nil {
		t.Fatal("domain_depth not registered")
	}

	tests := []struct {
		input string
		want  float64
	}{
		{"com", 1},
		{"example.com", 2},
		{"mail.google.com", 3},
		{"a.b.c.evil.com", 5},
		{"a.b.c.d.e.f.g.com", 8},    // DGA-like depth
	}
	for _, tt := range tests {
		got := fn([]any{tt.input})
		if got != tt.want {
			t.Errorf("domain_depth(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}

	if got := fn([]any{nil}); got != nil {
		t.Errorf("domain_depth(nil) = %v, want nil", got)
	}
}

func TestURLEncodeDecode(t *testing.T) {
	encode := DefaultRegistry.Lookup("url_encode")
	decode := DefaultRegistry.Lookup("url_decode")
	if encode == nil || decode == nil {
		t.Fatal("url_encode or url_decode not registered")
	}

	tests := []struct {
		plain   string
		encoded string
	}{
		{"hello world", "hello+world"},
		{"foo=bar&baz=qux", "foo%3Dbar%26baz%3Dqux"},
		{"simple", "simple"},
		{"path/to/file", "path%2Fto%2Ffile"},
	}
	for _, tt := range tests {
		got := encode([]any{tt.plain})
		if got != tt.encoded {
			t.Errorf("url_encode(%q) = %v, want %q", tt.plain, got, tt.encoded)
		}
		roundtrip := decode([]any{got})
		if roundtrip != tt.plain {
			t.Errorf("url_decode(url_encode(%q)) = %v, want %q", tt.plain, roundtrip, tt.plain)
		}
	}

	// NULL propagation
	if got := encode([]any{nil}); got != nil {
		t.Errorf("url_encode(nil) = %v, want nil", got)
	}
	if got := decode([]any{nil}); got != nil {
		t.Errorf("url_decode(nil) = %v, want nil", got)
	}

	// Invalid percent encoding
	if got := decode([]any{"%ZZ"}); got != nil {
		t.Errorf("url_decode(%%ZZ) = %v, want nil", got)
	}
}

func TestEntropy(t *testing.T) {
	fn := DefaultRegistry.Lookup("entropy")
	if fn == nil {
		t.Fatal("entropy not registered")
	}

	tests := []struct {
		input string
		min   float64
		max   float64
	}{
		{"aaaa", 0.0, 0.01},           // zero entropy — single repeated char
		{"ab", 0.99, 1.01},            // 1 bit — two equally likely chars
		{"hello world", 2.5, 3.5},     // natural language — moderate entropy
		{"a3f8b2c9e1d7", 3.0, 4.0},    // hex string — higher entropy
		{"ABCDEFGHabcdefgh12345678!@#$%^&*()", 4.0, 5.5}, // high entropy
	}
	for _, tt := range tests {
		got := fn([]any{tt.input}).(float64)
		if got < tt.min || got > tt.max {
			t.Errorf("entropy(%q) = %v, want [%v, %v]", tt.input, got, tt.min, tt.max)
		}
	}

	// Empty string
	got := fn([]any{""}).(float64)
	if got != 0.0 {
		t.Errorf("entropy(\"\") = %v, want 0.0", got)
	}

	// NULL propagation
	if got := fn([]any{nil}); got != nil {
		t.Errorf("entropy(nil) = %v, want nil", got)
	}

	// Verify: 26 unique lowercase letters → log2(26) ≈ 4.7
	alpha := fn([]any{"abcdefghijklmnopqrstuvwxyz"}).(float64)
	if math.Abs(alpha-math.Log2(26)) > 0.01 {
		t.Errorf("entropy(a-z) = %v, want %v", alpha, math.Log2(26))
	}
}
