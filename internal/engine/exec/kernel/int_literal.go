package kernel

import "math"

// parseIntText implements PostgreSQL integer input (#634, #536; ADR-0012 item 1).
// Accept 0x/0o/0b prefixes and decimal leading zeros; never infer octal from 017.
// Underscores require a following digit and cannot start a decimal, but may
// immediately follow a radix prefix. Trim only pgIntWhitespace (C isspace),
// not NBSP or other Unicode spaces.
// Accumulate negatively so MinInt64 remains representable; overflow returns
// NumConstRange/22003, never a wrapped value, and invalid grammar is syntax failure.
// See docs/internals/kernel-postgres-integer-input.md for the design.
func parseIntText(s string) (int64, NumConstStatus) {
	t := trimIntSpace(s)
	if t == "" {
		return 0, NumConstSyntax
	}
	neg := false
	switch t[0] {
	case '+':
		t = t[1:]
	case '-':
		neg, t = true, t[1:]
	}
	base := int64(10)
	prefixed := false
	if len(t) > 2 && t[0] == '0' {
		switch t[1] {
		case 'x', 'X':
			base, t, prefixed = 16, t[2:], true
		case 'o', 'O':
			base, t, prefixed = 8, t[2:], true
		case 'b', 'B':
			base, t, prefixed = 2, t[2:], true
		}
	}

	var acc int64
	digits := 0
	for i := 0; i < len(t); i++ {
		c := t[i]
		if c == '_' {
			// An underscore may not be FIRST in a decimal (PostgreSQL's own
			// check lives in that branch alone), and must be followed by a
			// digit wherever it appears.
			if !prefixed && i == 0 {
				return 0, NumConstSyntax
			}
			if i+1 >= len(t) || digitValue(t[i+1]) >= base {
				return 0, NumConstSyntax
			}
			continue
		}
		d := digitValue(c)
		if d >= base {
			return 0, NumConstSyntax
		}
		// acc*base - d, checked. Both halves must be tested BEFORE they
		// happen: a wrapped intermediate is a different number wearing the
		// right type, which is the whole class this parser exists to refuse.
		if acc < math.MinInt64/base {
			return 0, NumConstRange
		}
		acc *= base
		if acc < math.MinInt64+d {
			return 0, NumConstRange
		}
		acc -= d
		digits++
	}
	if digits == 0 {
		return 0, NumConstSyntax
	}
	if neg {
		return acc, NumConstOK
	}
	if acc == math.MinInt64 {
		return 0, NumConstRange
	}
	return -acc, NumConstOK
}

// digitValue maps one character to its digit value, or to 16 (above every
// base this parser reads) for anything that is not a hex digit.
func digitValue(c byte) int64 {
	switch {
	case c >= '0' && c <= '9':
		return int64(c - '0')
	case c >= 'a' && c <= 'f':
		return int64(c-'a') + 10
	case c >= 'A' && c <= 'F':
		return int64(c-'A') + 10
	}
	return 16
}

// trimIntSpace strips pgIntWhitespace from both ends. It is strings.Trim
// spelled out so the accept-set is stated once, beside the grammar it belongs
// to, rather than depending on a caller passing the right cutset.
func trimIntSpace(s string) string {
	i, j := 0, len(s)
	for i < j && isIntSpace(s[i]) {
		i++
	}
	for j > i && isIntSpace(s[j-1]) {
		j--
	}
	return s[i:j]
}

func isIntSpace(c byte) bool {
	switch c {
	case ' ', '\t', '\n', '\v', '\f', '\r':
		return true
	}
	return false
}
