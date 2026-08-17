package scan

import "bytes"

// LIKE pushdown (extends the Level-3 scan filter to pattern predicates).
//
// ClickBench Q22/Q23/Q24 profile as pure column decode: `URL LIKE
// '%google%'` materialized every URL before the exec filter ran a single
// match. Evaluating the pattern here rides the same structure as the
// comparison preds — ONCE per dictionary entry on dict pages, per value
// on plain pages — so non-matching strings never materialize.
//
// Semantics parity: the matcher is byte-wise and case-sensitive, exactly
// expr.matchLike ('%' = any run, '_' = any single byte, no ESCAPE); the
// planner only pushes literal patterns over string/bytes columns. NULL
// never matches LIKE or NOT LIKE (SQL three-valued logic — the page
// walkers already treat null rows as non-matching for every pred).

// RowPred.Op values for pattern predicates.
const (
	OpLike    = "like"
	OpNotLike = "not_like"
)

// compileLike builds a matcher for a LIKE pattern, specialized for the
// wildcard shapes that dominate real predicates.
func compileLike(pattern string, negate bool) func([]byte) bool {
	var base func([]byte) bool
	inner := pattern
	hasUnderscore := bytes.IndexByte([]byte(pattern), '_') >= 0
	switch {
	case !hasUnderscore && !bytes.ContainsAny([]byte(pattern), "%"):
		want := []byte(pattern)
		base = func(s []byte) bool { return bytes.Equal(s, want) }
	case !hasUnderscore && len(inner) >= 2 && inner[0] == '%' && inner[len(inner)-1] == '%' &&
		!bytes.ContainsAny([]byte(inner[1:len(inner)-1]), "%"):
		needle := []byte(inner[1 : len(inner)-1])
		base = func(s []byte) bool { return bytes.Contains(s, needle) }
	case !hasUnderscore && len(inner) >= 1 && inner[len(inner)-1] == '%' &&
		!bytes.ContainsAny([]byte(inner[:len(inner)-1]), "%"):
		prefix := []byte(inner[:len(inner)-1])
		base = func(s []byte) bool { return bytes.HasPrefix(s, prefix) }
	case !hasUnderscore && len(inner) >= 1 && inner[0] == '%' &&
		!bytes.ContainsAny([]byte(inner[1:]), "%"):
		suffix := []byte(inner[1:])
		base = func(s []byte) bool { return bytes.HasSuffix(s, suffix) }
	default:
		p := []byte(pattern)
		base = func(s []byte) bool { return likeMatchBytes(s, p, 0, 0) }
	}
	if negate {
		return func(s []byte) bool { return !base(s) }
	}
	return base
}

// likeMatchBytes mirrors expr.matchLikeRecur byte-for-byte so pushed and
// residual evaluation agree on every input.
func likeMatchBytes(s, pattern []byte, si, pi int) bool {
	for pi < len(pattern) {
		switch pattern[pi] {
		case '%':
			pi++
			for pi < len(pattern) && pattern[pi] == '%' {
				pi++
			}
			if pi == len(pattern) {
				return true
			}
			for i := si; i <= len(s); i++ {
				if likeMatchBytes(s, pattern, i, pi) {
					return true
				}
			}
			return false
		case '_':
			if si >= len(s) {
				return false
			}
			si++
			pi++
		default:
			if si >= len(s) || s[si] != pattern[pi] {
				return false
			}
			si++
			pi++
		}
	}
	return si == len(s)
}

// isLikeOp reports whether a RowPred is a pattern predicate.
func isLikeOp(op string) bool { return op == OpLike || op == OpNotLike }
