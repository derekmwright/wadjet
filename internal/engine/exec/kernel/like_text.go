// SPDX-License-Identifier: MIT

package kernel

import (
	"strings"
	"unicode/utf8"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// The TEXT LIKE pattern language (#1209): PostgreSQL's `text ~~ text`. This is
// the ONE implementation every text LIKE path calls — the scan's pushed filter
// (scan.compileLike), the exec LIKE filter's kernel (ResolveLikeFilterKernel),
// the expression evaluator (expr.Like) and LIKE ... ESCAPE (expr.fnLikeEscape)
// — so the language has one definition, the way ADR-0023 item 5 keeps one key
// producer. bytea's matcher (bytea_like.go) is a different operator and stays
// separate.
//
// The pattern takes the BACKSLASH as its escape: `\%`, `\_` and `\\` are the
// literals, and a backslash before any other character makes that character
// literal. A pattern whose matcher REACHES a lone trailing backslash is 22025;
// one the matcher never reaches that far answers without error ('abc' LIKE
// 'abc\' is false). `LIKE p ESCAPE e` is the same matcher over LikeEscape(p, e).
//
// `_` matches one BYTE, as every text matcher here did before #1209, where
// PostgreSQL's UTF-8 matcher takes one CHARACTER (`'é' LIKE '_'` is t there);
// that difference is kept so that no LIKE without a backslash or an ESCAPE
// changes answer, and is recorded in the note of #1209 as follow-up work.
//
// The matcher is a port of MatchText (like_match.c) including the LIKE_ABORT
// short-circuit, which decides whether the trailing escape is REACHED.

const (
	likeTrue = iota
	likeFalse
	likeAbort
)

// errLikeEndsInEscape is PostgreSQL's 22025 for a pattern the matcher reads
// past its last character on a trailing escape.
func errLikeEndsInEscape() error {
	return sqlerr.New("22025", "LIKE pattern must not end with escape character")
}

// LikeMatch reports whether s matches pattern p read with the backslash escape.
// The error is 22025, raised only when the matcher reaches a trailing escape.
func LikeMatch(s, p string) (bool, error) {
	r, err := likeMatchText(s, p)
	return r == likeTrue, err
}

func likeMatchText(t, p string) (int, error) {
	// A lone % matches everything, whatever the value is.
	if len(p) == 1 && p[0] == '%' {
		return likeTrue, nil
	}
	for len(t) > 0 && len(p) > 0 {
		switch p[0] {
		case '\\':
			// The next pattern byte is a literal, and there must be one.
			p = p[1:]
			if len(p) == 0 {
				return likeFalse, errLikeEndsInEscape()
			}
			if p[0] != t[0] {
				return likeFalse, nil
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
						return likeAbort, nil
					}
					t = t[1:]
					p = p[1:]
				} else {
					break
				}
			}
			if len(p) == 0 {
				return likeTrue, nil
			}
			// Find a value position where the next literal pattern byte
			// matches, and try the rest of the pattern there.
			var first byte
			if p[0] == '\\' {
				if len(p) < 2 {
					return likeFalse, errLikeEndsInEscape()
				}
				first = p[1]
			} else {
				first = p[0]
			}
			for len(t) > 0 {
				if t[0] == first {
					m, err := likeMatchText(t, p)
					if err != nil {
						return likeFalse, err
					}
					if m != likeFalse {
						return m, nil
					}
				}
				t = t[1:]
			}
			// The value ran out with the pattern unconsumed: no shorter
			// suffix can match either.
			return likeAbort, nil
		case '_':
			t = t[1:]
			p = p[1:]
			continue
		default:
			if p[0] != t[0] {
				return likeFalse, nil
			}
		}
		p = p[1:]
		t = t[1:]
	}
	if len(t) > 0 {
		return likeFalse, nil
	}
	for len(p) > 0 && p[0] == '%' {
		p = p[1:]
	}
	if len(p) == 0 {
		return likeTrue, nil
	}
	return likeAbort, nil
}

// LikeEscape is PostgreSQL's like_escape(text, text) (do_like_escape): it
// rewrites a pattern that names its own escape into the backslash form
// LikeMatch reads. An empty escape turns escaping off, which doubles every
// backslash so each is an ordinary character; an escape of more than one
// CHARACTER is 22025 (`invalid escape string`); any other escape becomes a
// backslash, and a backslash that does not follow an escape is doubled so it
// stays literal. The escape is compared a character at a time, so a multi-byte
// escape character names itself.
func LikeEscape(pat, esc string) (string, error) {
	if esc == "" {
		if strings.IndexByte(pat, '\\') < 0 {
			return pat, nil
		}
		return strings.ReplaceAll(pat, `\`, `\\`), nil
	}
	if utf8.RuneCountInString(esc) != 1 {
		return "", sqlerr.New("22025", "invalid escape string")
	}
	var b strings.Builder
	b.Grow(len(pat) + 4)
	afterEscape := false
	for i := 0; i < len(pat); {
		_, w := utf8.DecodeRuneInString(pat[i:])
		c := pat[i : i+w]
		switch {
		case c == esc && !afterEscape:
			b.WriteByte('\\')
			afterEscape = true
		case c == `\`:
			b.WriteByte('\\')
			if !afterEscape {
				b.WriteByte('\\')
			}
			afterEscape = false
		default:
			b.WriteString(c)
			afterEscape = false
		}
		i += w
	}
	return b.String(), nil
}

// likeShape is the form a compiled pattern matches by.
type likeShape uint8

const (
	likeShapeGeneral  likeShape = iota // MatchText over the pattern
	likeShapeExact                     // no wildcard: s == lit
	likeShapePrefix                    // lit%...
	likeShapeSuffix                    // %...lit
	likeShapeContains                  // %...lit%...
	likeShapeAll                       // only %: every value
)

// LikePattern is a pattern compiled once for a column or a page: the
// no-wildcard, prefix, suffix and contains forms are classified on the
// UNESCAPED literal, so `a\%%` is the prefix `a%` and never the prefix `a\`.
type LikePattern struct {
	pat   string
	lit   string
	shape likeShape
	// mayRaise: the pattern ends in an unescaped lone backslash, so matching
	// it can raise 22025 (always the general form).
	mayRaise bool
}

// CompileLike classifies a pattern read with the backslash escape.
func CompileLike(pat string) LikePattern {
	cp := LikePattern{pat: pat, shape: likeShapeGeneral}
	// Tokenize: literal bytes (escapes resolved), %, and _.
	var lit strings.Builder
	lead, trail := 0, 0 // runs of % before and after the literal
	sawLit, sawTrail := false, false
	general := false
	for i := 0; i < len(pat); i++ {
		c := pat[i]
		switch c {
		case '\\':
			if i+1 >= len(pat) {
				cp.mayRaise = true
				return cp
			}
			i++
			c = pat[i]
			if sawTrail {
				general = true
			}
			sawLit = true
			lit.WriteByte(c)
		case '%':
			if sawLit {
				trail++
				sawTrail = true
			} else {
				lead++
			}
		case '_':
			general = true
		default:
			if sawTrail {
				general = true
			}
			sawLit = true
			lit.WriteByte(c)
		}
	}
	if general {
		return cp
	}
	cp.lit = lit.String()
	switch {
	case !sawLit && lead > 0:
		cp.shape = likeShapeAll
	case lead == 0 && trail == 0:
		cp.shape = likeShapeExact
	case lead == 0:
		cp.shape = likeShapePrefix
	case trail == 0:
		cp.shape = likeShapeSuffix
	default:
		cp.shape = likeShapeContains
	}
	return cp
}

// MayRaise reports whether matching can raise 22025: the pattern ends in a
// lone backslash. Every other pattern's Match never errors.
func (cp LikePattern) MayRaise() bool { return cp.mayRaise }

// Match reports whether s matches. The error is 22025 and only a MayRaise
// pattern returns one.
func (cp LikePattern) Match(s string) (bool, error) {
	switch cp.shape {
	case likeShapeExact:
		return s == cp.lit, nil
	case likeShapePrefix:
		return strings.HasPrefix(s, cp.lit), nil
	case likeShapeSuffix:
		return strings.HasSuffix(s, cp.lit), nil
	case likeShapeContains:
		return containsStr(s, cp.lit), nil
	case likeShapeAll:
		return true, nil
	}
	return LikeMatch(s, cp.pat)
}

// Matcher returns the error-free match function of a pattern that cannot
// raise, specialized by shape so the hot loop pays no dispatch. ok is false
// for a MayRaise pattern, whose caller needs Match and its error.
func (cp LikePattern) Matcher() (func(string) bool, bool) {
	if cp.mayRaise {
		return nil, false
	}
	lit := cp.lit
	switch cp.shape {
	case likeShapeExact:
		return func(s string) bool { return s == lit }, true
	case likeShapePrefix:
		return func(s string) bool { return len(s) >= len(lit) && s[:len(lit)] == lit }, true
	case likeShapeSuffix:
		return func(s string) bool { return len(s) >= len(lit) && s[len(s)-len(lit):] == lit }, true
	case likeShapeContains:
		return func(s string) bool { return len(s) >= len(lit) && containsStr(s, lit) }, true
	case likeShapeAll:
		return func(string) bool { return true }, true
	}
	pat := cp.pat
	return func(s string) bool {
		r, _ := likeMatchText(s, pat)
		return r == likeTrue
	}, true
}
