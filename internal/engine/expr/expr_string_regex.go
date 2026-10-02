// SPDX-License-Identifier: MIT

// This file holds expr string regex; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"regexp"
	"regexp/syntax"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// --- String: regex and parsing ---

func fnSplitPart(args []any) any {
	if len(args) < 3 || args[0] == nil || args[1] == nil || args[2] == nil {
		return nil
	}
	parts := strings.Split(toString(args[0]), toString(args[1]))
	pos := int(ToFloat64(args[2]))
	// PostgreSQL 14 and later count a NEGATIVE position from the END:
	// SPLIT_PART('a,b,c', ',', -1) is `c`, -2 is `b`, and a magnitude past the
	// field count answers the empty string exactly as a too-large positive one
	// does. Zero names nothing and is 22023, not the empty string this used to
	// answer for every non-positive position alike (#855). All measured live
	// on 17.11.
	if pos == 0 {
		raiseFieldPositionZero()
	}
	idx := pos - 1 // SQL is 1-indexed
	if pos < 0 {
		idx = len(parts) + pos
	}
	if idx < 0 || idx >= len(parts) {
		return ""
	}
	return parts[idx]
}

func fnStrPos(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	s := toString(args[0])
	pos := strings.Index(s, toString(args[1]))
	if pos < 0 {
		return int32(0)
	}
	// The position is a CHARACTER position, as everywhere else in this family:
	// POSITION('à' IN 'éàü') is 2 on the server and was 3 here, the byte offset
	// (#856). The needle search itself stays bytewise — on valid UTF-8 a
	// substring match is the same match either way — and only the answer is
	// converted.
	return int32Count(utf8.RuneCountInString(s[:pos]) + 1) // 1-based
}

func fnRegexpLike(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	matched, err := regexp.MatchString(toString(args[1]), toString(args[0]))
	if err != nil {
		return nil
	}
	return matched
}

func fnRegexpExtract(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	re := compileRegexpCached(toString(args[1]))
	if re == nil {
		return nil
	}
	group := 0
	if len(args) >= 3 && args[2] != nil {
		group = int(ToFloat64(args[2]))
	}
	matches := re.FindStringSubmatch(toString(args[0]))
	if matches == nil || group >= len(matches) {
		return nil
	}
	return matches[group]
}

func compileRegexpCached(pattern string) *regexp.Regexp {
	if v, ok := regexpCache.Load(pattern); ok {
		re, _ := v.(*regexp.Regexp)
		return re
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		re = nil
	}
	regexpCache.Store(pattern, re)
	return re
}

// fnRegexpReplace is regexp_replace(source, pattern, replacement [, flags])
// as PostgreSQL answers it: the FIRST match replaced, every match only under
// the 'g' flag (#1481 — it replaced every match whatever the flags), the
// pattern read with PostgreSQL's default flags (a `.` matches a newline), the
// replacement's \1 … \9 and \& expanded. The flags are regexpReplaceFlags';
// a flag this engine does not implement is refused, never ignored.
func fnRegexpReplace(args []any) any {
	if len(args) < 3 || args[0] == nil || args[1] == nil || args[2] == nil {
		return nil
	}
	flags := ""
	if len(args) >= 4 {
		if args[3] == nil {
			return nil
		}
		f, ok := args[3].(string)
		if !ok {
			// regexp_replace(source, pattern, replacement, start [, N
			// [, flags]]): PostgreSQL answers; this engine has no start
			// position or occurrence count.
			panic(fatalEval{sqlerr.New("0A000",
				"regexp_replace with a start position is not supported")})
		}
		flags = f
	}
	p, err := cachedRegexpReplace(toString(args[1]), flags)
	if err != nil {
		panic(fatalEval{err})
	}
	return p.withTemplate(toString(args[2])).replace(toString(args[0]))
}

// cachedRegexpReplace is the compiled pattern for (pattern, flags), compiled
// once per process like regexpCache's patterns.
func cachedRegexpReplace(pattern, flags string) (*preparedRegexp, error) {
	key := flags + "\x00" + pattern
	if v, ok := regexpReplaceCache.Load(key); ok {
		return v.(*preparedRegexp), nil
	}
	goPattern, global, err := regexpReplaceFlags(pattern, flags)
	if err != nil {
		return nil, err
	}
	p := prepareRegexpReplace(goPattern, "")
	if !p.ok {
		_, cerr := regexp.Compile(goPattern)
		return nil, sqlerr.New("2201B", "invalid regular expression: %s", reErrorText(cerr))
	}
	if !hasLazyQuantifier(goPattern) {
		// An ARE whose quantifiers are all greedy matches the LONGEST text
		// at the leftmost position (PostgreSQL §9.7.3.5); RE2's default is
		// the first alternative that matches (`regexp_replace('abc',
		// 'a|ab', 'X')` is Xc there, Xbc under leftmost-first). A pattern
		// with a non-greedy quantifier keeps RE2's leftmost-first reading,
		// which is PostgreSQL's whenever the first quantifier is the
		// non-greedy one and nothing after it is greedy.
		p.re.Longest()
	}
	p.global = global
	p.emptyAt = emptyMatcher(goPattern)
	regexpReplaceCache.Store(key, p)
	return p, nil
}

var regexpReplaceCache sync.Map // flags + NUL + pattern → *preparedRegexp

// regexpReplaceFlags reads regexp_replace's flags string as PostgreSQL's
// parse_re_flags does, and answers the RE2 pattern that matches as the
// pattern and flags say plus whether every match is replaced:
//
//	g      every match (otherwise the first)
//	i, c   case-insensitive / case-sensitive, the last one written wins
//	q      the pattern is a literal string
//	s, t   PostgreSQL's defaults (non-newline-sensitive, tight syntax): no-ops
//
// The pattern is an ARE, read through aregexToRE2 — the one translation the
// `~` operators use (ADR-0044) — so `.` matches a newline, `\b` is a
// backspace, and a form RE2 has no equivalent for is refused rather than
// read as something else. The newline-sensitive flags (n, m, p, w), expanded
// syntax (x) and the basic / extended dialects (b, e) are refused 0A000 as
// the translator refuses their embedded-option spellings; any other letter
// is PostgreSQL's own 22023.
func regexpReplaceFlags(pattern, flags string) (goPattern string, global bool, err error) {
	caseless, literal, newline := false, false, byte(0)
	var unsupported byte
	for _, r := range flags {
		switch r {
		case 'g':
			global = true
		case 'i':
			caseless = true
		case 'c':
			caseless = false
		case 'q':
			literal = true
		case 's':
			newline = 0
		case 't':
		case 'n', 'm', 'p', 'w':
			newline = byte(r)
		case 'x', 'b', 'e':
			if unsupported == 0 {
				unsupported = byte(r)
			}
		default:
			return "", false, sqlerr.New("22023", "invalid regular expression option: %s", sqlerr.Quote(string(r)))
		}
	}
	if unsupported == 0 && newline != 0 {
		unsupported = newline
	}
	if unsupported != 0 {
		return "", false, sqlerr.New("0A000",
			"regexp_replace flag %s is not supported", sqlerr.Quote(string(unsupported)))
	}
	if literal {
		return "(?s)" + literalARE(pattern, caseless), global, nil
	}
	goPattern, err = aregexToRE2(pattern, caseless)
	return goPattern, global, err
}

// hasLazyQuantifier reports whether an RE2 pattern holds a non-greedy
// quantifier.
func hasLazyQuantifier(pattern string) bool {
	re, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return true
	}
	var walk func(*syntax.Regexp) bool
	walk = func(r *syntax.Regexp) bool {
		switch r.Op {
		case syntax.OpStar, syntax.OpPlus, syntax.OpQuest, syntax.OpRepeat:
			if r.Flags&syntax.NonGreedy != 0 {
				return true
			}
		}
		for _, sub := range r.Sub {
			if walk(sub) {
				return true
			}
		}
		return false
	}
	return walk(re)
}

// sqlBackrefsToGo converts SQL-style backreferences (\1 … \9, the
// POSIX/DuckDB/Postgres convention) in a replacement string to Go's ${N}
// form. \\ stays a literal backslash escape for a following digit.
func sqlBackrefsToGo(repl string) string {
	if !strings.ContainsRune(repl, '\\') {
		return repl
	}
	var b strings.Builder
	b.Grow(len(repl) + 4)
	for i := 0; i < len(repl); i++ {
		c := repl[i]
		if c == '\\' && i+1 < len(repl) {
			next := repl[i+1]
			if next >= '1' && next <= '9' {
				b.WriteString("${")
				b.WriteByte(next)
				b.WriteString("}")
				i++
				continue
			}
			if next == '\\' {
				b.WriteByte('\\')
				i++
				continue
			}
			if next == '&' {
				b.WriteString("${0}")
				i++
				continue
			}
		}
		// Go's Expand treats $ specially — escape literal dollars.
		if c == '$' {
			b.WriteString("$$")
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
}
