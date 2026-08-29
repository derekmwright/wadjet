package kernel

import "math"

// parseIntText reads PostgreSQL's INTEGER type input (pg_strtoint32_safe /
// pg_strtoint64_safe as of 16), which is a strict superset of Go's base-10
// grammar in three places, all of them observable (#634, split out of #536):
//
//	'0x1A'  -> 26      '0o17'  -> 15      '0b101' -> 5
//	'1_000' -> 1000    '0x1_A' -> 26      '0x_1A' -> 26
//	'007'   -> 7       (decimal seven, NOT octal fifteen)
//
// Neither Go base matches. `strconv.ParseInt(s, 10, 64)` — what this replaced
// — refuses the radix and underscore forms, which is a PG-SUPERSET REGRESSION:
// it raises 22P02 for input PostgreSQL answers, and ADR-0012 item 1 says the
// binder never refuses what PostgreSQL accepts. `ParseInt(s, 0, 64)` is wrong
// the other way: it reads '017' as octal 15 where PostgreSQL reads decimal 7.
//
// The underscore rule is PostgreSQL's exactly, including its one asymmetry,
// verified live on postgres:17-alpine: an underscore must be FOLLOWED by a
// digit ('1000_' and '1__000' are 22P02) and may not be FIRST in a decimal
// ('_1000' is 22P02) — but it MAY be first after a radix prefix ('0x_1A' is
// 26), because the prefix already stands in front of it. PostgreSQL's own
// source has the "underscore may not be first" check in the decimal branch
// alone, and this reproduces that rather than tidying it.
//
// Whitespace is pgIntWhitespace — C isspace() in the default locale, NOT
// strings.TrimSpace, which also strips NBSP. PostgreSQL rejects an
// NBSP-prefixed integer (verified live), so trimming it would accept input
// PostgreSQL refuses.
//
// Overflow reports NumConstRange (22003, "value \"X\" is out of range for type
// bigint"), never a wrapped value. The accumulation runs NEGATIVE for
// PostgreSQL's own reason: two's complement is asymmetric, so
// -9223372036854775808 has no positive counterpart and accumulating
// positively would refuse the one value int64's minimum bound sits on.
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
