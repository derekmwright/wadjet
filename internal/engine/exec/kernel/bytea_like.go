// SPDX-License-Identifier: MIT

package kernel

import "github.com/derekmwright/wadjet/internal/sqlerr"

// bytea LIKE (#1593): PostgreSQL's `bytea ~~ bytea`, the matcher the bytea
// operands of LIKE and NOT LIKE read. It is NOT the text matcher
// (expr.matchLike / kernel.matchLike / scan.likeMatchBytes): those take no
// escape at all, while bytea's pattern takes the BACKSLASH as its escape, a
// byte at a time — `\%`, `\_` and `\\` are the literals, and a pattern that
// REACHES a lone trailing backslash is 22025. The two never share a function,
// so a change to one cannot move the other.
//
// It is a port of PostgreSQL's MatchText (like_match.c, single-byte
// instantiation), including the LIKE_ABORT short-circuit, because the error is
// raised where the matcher REACHES the trailing escape and not where the
// pattern is merely malformed: `'' LIKE '\'` is false and `'A' LIKE '\'` is
// 22025 (measured on 17.11), and `'A' LIKE '%\'` is 22025 whether or not the
// value is exhausted first.

const (
	byteaLikeTrue = iota
	byteaLikeFalse
	byteaLikeAbort
)

// ByteaLike reports whether s matches the pattern p, taking the backslash as
// the escape. The error is 22025 and carries PostgreSQL's wording.
func ByteaLike(s, p []byte) (bool, error) {
	r, err := byteaMatchText(s, p)
	return r == byteaLikeTrue, err
}

func byteaLikeEndsInEscape() error {
	return sqlerr.New("22025", "LIKE pattern must not end with escape character")
}

func byteaMatchText(t, p []byte) (int, error) {
	// A lone % matches everything, whatever the value is.
	if len(p) == 1 && p[0] == '%' {
		return byteaLikeTrue, nil
	}
	for len(t) > 0 && len(p) > 0 {
		switch p[0] {
		case '\\':
			// The next pattern byte is a literal, and there must be one.
			p = p[1:]
			if len(p) == 0 {
				return byteaLikeFalse, byteaLikeEndsInEscape()
			}
			if p[0] != t[0] {
				return byteaLikeFalse, nil
			}
		case '%':
			p = p[1:]
			// Consecutive % collapse, and every _ after the % consumes a byte
			// outright.
			for len(p) > 0 {
				if p[0] == '%' {
					p = p[1:]
				} else if p[0] == '_' {
					if len(t) == 0 {
						return byteaLikeAbort, nil
					}
					t = t[1:]
					p = p[1:]
				} else {
					break
				}
			}
			if len(p) == 0 {
				return byteaLikeTrue, nil
			}
			// Find a value position where the next literal pattern byte
			// matches, and try the rest of the pattern there.
			var first byte
			if p[0] == '\\' {
				if len(p) < 2 {
					return byteaLikeFalse, byteaLikeEndsInEscape()
				}
				first = p[1]
			} else {
				first = p[0]
			}
			for len(t) > 0 {
				if t[0] == first {
					m, err := byteaMatchText(t, p)
					if err != nil {
						return byteaLikeFalse, err
					}
					if m != byteaLikeFalse {
						return m, nil
					}
				}
				t = t[1:]
			}
			// The value ran out with the pattern unconsumed: no shorter
			// suffix can match either.
			return byteaLikeAbort, nil
		case '_':
			t = t[1:]
			p = p[1:]
			continue
		default:
			if p[0] != t[0] {
				return byteaLikeFalse, nil
			}
		}
		p = p[1:]
		t = t[1:]
	}
	if len(t) > 0 {
		return byteaLikeFalse, nil
	}
	for len(p) > 0 && p[0] == '%' {
		p = p[1:]
	}
	if len(p) == 0 {
		return byteaLikeTrue, nil
	}
	return byteaLikeAbort, nil
}

// ByteaLikeEscape is PostgreSQL's like_escape(bytea, bytea): it rewrites a
// pattern that names its own escape into the backslash form ByteaLike reads.
// An empty escape turns escaping off, which doubles every backslash so each is
// an ordinary byte; an escape of more than one byte is 22025; a backslash
// escape leaves the pattern as it is; any other escape becomes a backslash,
// and a backslash that does not follow an escape is doubled so it stays
// literal.
func ByteaLikeEscape(pat, esc []byte) ([]byte, error) {
	if len(esc) == 0 {
		out := make([]byte, 0, len(pat)+4)
		for _, c := range pat {
			if c == '\\' {
				out = append(out, '\\')
			}
			out = append(out, c)
		}
		return out, nil
	}
	if len(esc) > 1 {
		return nil, sqlerr.New("22025", "invalid escape string")
	}
	e := esc[0]
	if e == '\\' {
		return pat, nil
	}
	out := make([]byte, 0, len(pat)+4)
	afterEscape := false
	for _, c := range pat {
		switch {
		case c == e && !afterEscape:
			out = append(out, '\\')
			afterEscape = true
		case c == '\\':
			out = append(out, '\\')
			if !afterEscape {
				out = append(out, '\\')
			}
			afterEscape = false
		default:
			out = append(out, c)
			afterEscape = false
		}
	}
	return out, nil
}
