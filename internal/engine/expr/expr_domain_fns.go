// This file holds expr domain fns; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"strings"

	"golang.org/x/net/publicsuffix"
)

// --- Domain parsing functions ---
// These use the Public Suffix List (golang.org/x/net/publicsuffix) to correctly
// handle multi-part TLDs like .co.uk, .com.au, .gov.uk etc.

// cleanDomain strips any trailing dot and lowercases for consistent handling.
func cleanDomain(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	s = strings.TrimSuffix(s, ".")
	return s
}

// fnRegisteredDomain extracts the registered domain (eTLD+1) from a hostname.
// registered_domain('mail.google.com') → 'google.com'
// registered_domain('sub.example.co.uk') → 'example.co.uk'
func fnRegisteredDomain(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	domain := cleanDomain(toString(args[0]))
	if domain == "" {
		return nil
	}
	rd, err := publicsuffix.EffectiveTLDPlusOne(domain)
	if err != nil {
		return nil
	}
	return rd
}

// fnTLD extracts the effective top-level domain (public suffix) from a hostname.
// tld('mail.google.com') → 'com'
// tld('sub.example.co.uk') → 'co.uk'
func fnTLD(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	domain := cleanDomain(toString(args[0]))
	if domain == "" {
		return nil
	}
	suffix, _ := publicsuffix.PublicSuffix(domain)
	return suffix
}

// fnSubdomain extracts the subdomain portion (everything before the registered domain).
// subdomain('mail.google.com') → 'mail'
// subdomain('a.b.c.example.co.uk') → 'a.b.c'
// subdomain('example.com') → ” (no subdomain)
func fnSubdomain(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	domain := cleanDomain(toString(args[0]))
	if domain == "" {
		return nil
	}
	rd, err := publicsuffix.EffectiveTLDPlusOne(domain)
	if err != nil {
		return nil
	}
	if domain == rd {
		return ""
	}
	// domain = "a.b.c.example.co.uk", rd = "example.co.uk"
	// subdomain = "a.b.c"
	return strings.TrimSuffix(domain, "."+rd)
}

// fnDomainDepth returns the number of labels (dot-separated parts) in a domain.
// domain_depth('mail.google.com') → 3
// domain_depth('a.b.c.evil.com') → 5
// Useful for detecting DGA domains which tend to have unusual depth.
func fnDomainDepth(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	domain := cleanDomain(toString(args[0]))
	if domain == "" {
		return nil
	}
	return float64(strings.Count(domain, ".") + 1)
}
