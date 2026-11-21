// SPDX-License-Identifier: MIT

package expr

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// The bodies behind the SQL-standard spellings the grammar rewrites into
// calls (#1169, #1168). Each one is reached by a name the registry declares
// and ADR-0038's rewrite table enumerates:
//
//	OVERLAY(s PLACING r FROM n [FOR m])   → overlay(s, r, n[, m])
//	x LIKE p ESCAPE e                     → like_escape(x, p, e)
//	LOCALTIMESTAMP                        → localtimestamp()
//	x SIMILAR TO p [ESCAPE e]             → similar_to(x, p[, e])
//
// PostgreSQL 17.11 is the oracle for every value and every refusal here,
// measured live rather than remembered.

// fnOverlay is OVERLAY(string PLACING newsubstring FROM start [FOR count]).
//
// PostgreSQL defines it as
//
//	substring(s from 1 for start-1) || newsub || substring(s from start+count)
//
// with count defaulting to length(newsub), and the definition is what is
// computed here rather than a clamped rewrite of it: the two disagree at the
// boundaries this engine is asked about. `overlay('abc' placing 'X' from 0)`
// is 22011 (negative substring length) on the server, because the first
// substring's length is start-1 = -1; `overlay('abc' placing 'X' from 2 for
// -1)` is `aXabc`, because start+count walks BACKWARDS to position 1. Both
// measured on 17.11.
func fnOverlay(args []any) any {
	if len(args) < 3 {
		return nil
	}
	for _, a := range args {
		if a == nil {
			return nil
		}
	}
	src := []rune(toString(args[0]))
	newsub := []rune(toString(args[1]))
	start := int(ToFloat64(args[2]))
	count := len(newsub)
	if len(args) >= 4 {
		count = int(ToFloat64(args[3]))
	}
	// substring(s from 1 for start-1): a negative length is the server's own
	// refusal, which is how FROM 0 reports.
	if start-1 < 0 {
		raiseNegativeSubstringLength()
	}
	head := start - 1
	if head > len(src) {
		head = len(src)
	}
	tail := start + count - 1 // substring(s from start+count), 0-based
	if tail < 0 {
		tail = 0
	}
	if tail > len(src) {
		tail = len(src)
	}
	var b strings.Builder
	b.WriteString(string(src[:head]))
	b.WriteString(string(newsub))
	b.WriteString(string(src[tail:]))
	return b.String()
}

// fnLikeEscape is `x LIKE pattern ESCAPE escape`, the spelling the grammar
// rewrites here (#1169).
//
// It exists as a CALL rather than as a field on plansql.LikeExpr because that
// node is rebuilt by seven rewriters (canonicalization, correlation, the
// lateral walks, filter and project pushdown, the worker's filter compile,
// the DAG's refusal walk) and a rebuild that forgot to copy the escape would
// silently answer the UNESCAPED pattern — the same reason BETWEEN SYMMETRIC
// and ILIKE are expanded at parse time rather than flagged.
//
// It is PostgreSQL's `x ~~ like_escape(pattern, escape)`: kernel.LikeEscape
// rewrites the pattern into the backslash form and kernel.LikeMatch, the one
// text matcher (#1209), reads it. PostgreSQL's rules, measured: an escape
// string of more than one character is 22025 (`invalid escape string`), and is
// raised whatever x is, because like_escape runs before ~~ sees x; the EMPTY
// string disables escaping entirely; a NULL escape or a NULL pattern makes the
// whole predicate NULL, and so does a NULL x; a pattern that ends in its escape
// is 22025 only where the matcher reaches it (`'abc' LIKE 'abc!' ESCAPE '!'`
// is false, #1204).
func fnLikeEscape(args []any) any {
	if len(args) < 3 {
		return nil
	}
	if args[1] == nil || args[2] == nil {
		return nil
	}
	pat, err := kernel.LikeEscape(toString(args[1]), toString(args[2]))
	if err != nil {
		panic(fatalEval{err})
	}
	if args[0] == nil {
		return nil
	}
	m, err := kernel.LikeMatch(toString(args[0]), pat)
	if err != nil {
		panic(fatalEval{err})
	}
	return m
}

// fnLikeEscapeText is PostgreSQL's like_escape(text, text)
// (plansql.LikeEscapeTextFunc): kernel.LikeEscape, the rewrite `LIKE p ESCAPE
// e` already reads its pattern through. Measured on PostgreSQL 17.11: a NULL
// pattern or escape is NULL, an empty escape doubles every backslash, an
// escape of more than one character is 22025. `x ILIKE p ESCAPE e` lowers
// this function's result, never its input (#1209).
func fnLikeEscapeText(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	pat, err := kernel.LikeEscape(toString(args[0]), toString(args[1]))
	if err != nil {
		panic(fatalEval{err})
	}
	return pat
}

// fnLocalTimestamp is LOCALTIMESTAMP: the current instant as a TIMESTAMP
// WITHOUT TIME ZONE.
//
// It is a different declaration from CURRENT_TIMESTAMP, which is `timestamp
// with time zone` on the server, and that difference is the whole point of
// the spelling — a client reading the wire sees timestamp, not timestamptz.
// This engine has one timestamp type and renders every instant in UTC, so the
// VALUE is current_timestamp's; the declaration is what changes.
func fnLocalTimestamp(args []any) any { return unboundClock("LOCALTIMESTAMP") }

// fnSimilarTo is `x SIMILAR TO pattern [ESCAPE escape]` (#1168).
//
// SIMILAR TO is neither LIKE nor a regular expression: it is the SQL
// standard's own pattern language, in which `%` and `_` mean what they mean in
// LIKE, `|`, `*`, `+`, `?`, `{m,n}`, `()` and `[…]` mean what they mean in a
// regular expression, and EVERY OTHER character — `.`, `^`, `$` above all —
// is a literal. The match is against the WHOLE string.
//
// Before this, the spelling was rewritten to `regexp_like(x, pattern)`, which
// is a different language read against a SUBSTRING of the input: `'abc'
// SIMILAR TO 'a%'` answered false where the server answers true, `'abc'
// SIMILAR TO 'a.c'` answered true where the server answers false, and `'abc'
// SIMILAR TO 'ab'` answered true where the server answers false — six
// inverted cells, one of them a WHERE clause whose COUNT(*) was 0 for a row
// the server counts.
func fnSimilarTo(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	esc := `\`
	if len(args) >= 3 {
		if args[2] == nil {
			return nil
		}
		esc = toString(args[2])
		if len([]rune(esc)) > 1 {
			panic(fatalEval{sqlerr.New("22025", "invalid escape string")})
		}
	}
	pattern := toString(args[1])
	// The rewrite is an ARE, compiled like every SQL pattern
	// (translateAndCompile): `_` and `%` match a newline, as on the server,
	// and an escaped letter is the ARE escape it spells there (`#y` a word
	// boundary, `#b` a backspace).
	re, err := translateAndCompile(SimilarToRegexp(pattern, esc), reFlags{})
	if err != nil {
		if sqlerr.StateOf(err) == "2201B" {
			// A pattern the translation cannot express as a regular
			// expression is REFUSED, not answered NULL: the server raises
			// 2201B for the same patterns (`SIMILAR TO '*'` is "quantifier
			// operand invalid", `SIMILAR TO '['` is "brackets [] not
			// balanced"), and a NULL here would be a plausible answer to a
			// question that has none.
			err = sqlerr.New("2201B",
				"invalid regular expression: the SIMILAR TO pattern %s cannot be matched",
				sqlerr.Quote(pattern))
		}
		panic(fatalEval{err})
	}
	return re.re.MatchString(toString(args[0]))
}

// SimilarToRegexp translates a SIMILAR TO pattern into the anchored regular
// expression that means the same thing — PostgreSQL's own similar_to_escape
// rewrite, and the reason the server can hand the pattern to its regex engine.
//
// The translation, measured form by form against 17.11:
//
//	%          → .*          (LIKE's wildcard, not the regex quantifier)
//	_          → .           (LIKE's single character)
//	| * + ? { } ) [ ]        pass through as regex metacharacters
//	(          → (?:         (a SIMILAR TO group does not capture)
//	.  ^  $  \                and every other metacharacter is QUOTED
//	<esc>c     → the literal c
//
// The result is an ARE wrapped in `^(?:…)$`, as similar_to_escape writes it,
// because SIMILAR TO matches the whole string, which is what makes `'abc'
// SIMILAR TO 'ab'` false.
//
// A bracket expression is copied VERBATIM to the regex: inside `[…]`, `%` and
// `_` are ordinary characters on the server too, and `[[:alpha:]]` is the
// POSIX class Go's regexp reads with the same spelling.
func SimilarToRegexp(pattern, escape string) string {
	var esc rune = -1
	if escape != "" {
		for _, r := range escape {
			esc = r
			break
		}
	}
	var b strings.Builder
	b.WriteString(`^(?:`)
	runes := []rune(pattern)
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		if esc >= 0 && r == esc {
			// The escape and the character after it travel to the regex as
			// `\c`, which is what PostgreSQL's own similar_to_escape emits —
			// and it is not the same as "that character, literally". Measured
			// on 17.11: `'abc' SIMILAR TO 'a#bc' ESCAPE '#'` is FALSE, because
			// `\b` is a backspace in the ARE the server hands the pattern to
			// (and `'a b' SIMILAR TO 'a#y b' ESCAPE '#'` is TRUE: `\y` is its
			// word boundary); `'a5c' SIMILAR TO 'a#5c'` is 2201B
			// (invalid backreference) and `'agc' SIMILAR TO 'a#gc'` is 2201B
			// (invalid escape). Emitting the literal instead would answer TRUE
			// for all three.
			//
			// A DANGLING escape is the one place the passthrough cannot be
			// verbatim: a trailing backslash does not compile. It contributes
			// NOTHING on the server, measured on 17.11 — `'a' SIMILAR TO 'a\'`
			// and `'abc' SIMILAR TO 'abc!' ESCAPE '!'` are both TRUE, and
			// `'' SIMILAR TO '!' ESCAPE '!'` is TRUE. (`'abc' SIMILAR TO 'a\'`
			// is FALSE there because `a` does not match `abc`, not because a
			// dangling escape matches nothing — the cell this arc's the earlier implementation
			// notes reasoned from, which does not discriminate.)
			if i+1 >= len(runes) {
				break
			}
			i++
			b.WriteString(`\` + string(runes[i]))
			continue
		}
		switch r {
		case '%':
			b.WriteString(`.*`)
		case '_':
			b.WriteString(`.`)
		case '(':
			// A SIMILAR TO group does not capture: similar_to_escape writes
			// `(?:`, so `'aa' SIMILAR TO '(a)#1' ESCAPE '#'` is 2201B there
			// (invalid backreference number: the RE has no group 1).
			b.WriteString(`(?:`)
		case '|', '*', '+', '?', '{', '}', ')':
			b.WriteRune(r)
		case '[':
			// A bracket expression travels verbatim, up to its closing ]. A
			// leading ^ and a ] in first position belong to the expression.
			j := i + 1
			if j < len(runes) && runes[j] == '^' {
				j++
			}
			if j < len(runes) && runes[j] == ']' {
				j++
			}
			for j < len(runes) && runes[j] != ']' {
				if runes[j] == '[' && j+1 < len(runes) && runes[j+1] == ':' {
					for j < len(runes) && !(runes[j] == ':' && j+1 < len(runes) && runes[j+1] == ']') {
						j++
					}
					j++ // the :
				}
				j++
			}
			if j >= len(runes) {
				// Unbalanced: the server refuses with "brackets [] not
				// balanced"; Go's regexp refuses to compile, and the caller
				// answers NULL for an uncompilable pattern. Emit it as
				// written so the failure is the regex engine's own.
				b.WriteString(string(runes[i:]))
				i = len(runes)
				continue
			}
			b.WriteString(string(runes[i : j+1]))
			i = j
		default:
			b.WriteString(quoteRegexpRune(r))
		}
	}
	b.WriteString(`)$`)
	return b.String()
}

// quoteRegexpRune renders one character as a regex that matches exactly it.
func quoteRegexpRune(r rune) string {
	switch r {
	case '.', '^', '$', '\\', '[', ']', '(', ')', '{', '}', '*', '+', '?', '|', '-':
		return `\` + string(r)
	}
	return string(r)
}
