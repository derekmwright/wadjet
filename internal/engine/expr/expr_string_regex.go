// SPDX-License-Identifier: MIT

// This file holds expr string regex; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"regexp/syntax"
	"strings"
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

// fnRegexpLike is regexp_like(string, pattern [, flags]): whether the
// pattern, read as an ARE under the flags (translateAndCompile), matches. A
// NULL flags argument is NULL, as on the server; 'g' is 22023 there.
func fnRegexpLike(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	f, ok := regexFlagsArg(args, 2, "regexp_like")
	if !ok {
		return nil
	}
	return mustCompileSQLRegex(toString(args[1]), f).re.MatchString(toString(args[0]))
}

// regexFlagsArg reads the flags argument at position i of a regular-
// expression function (absent: the defaults). ok=false means it is NULL.
func regexFlagsArg(args []any, i int, fn string) (reFlags, bool) {
	if len(args) <= i {
		return reFlags{}, true
	}
	if args[i] == nil {
		return reFlags{}, false
	}
	f, err := parseREFlags(fn, toString(args[i]))
	if err != nil {
		panic(fatalEval{err})
	}
	return f, true
}

// fnRegexpExtract is this engine's regexp_extract(string, pattern [,
// group]): the leftmost match (or its group), NULL without one. The
// pattern is read as every other construct reads it — PostgreSQL's ARE
// through translateAndCompile, so `\b` is a backspace and the match the
// longest at the leftmost position — which makes it regexp_substr(string,
// pattern, 1, 1, ”, group) (catalog: the engine's own regex functions).
func fnRegexpExtract(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	re := mustCompileSQLRegex(toString(args[1]), ownFunctionFlags)
	group := 0
	if len(args) >= 3 && args[2] != nil {
		group = int(ToFloat64(args[2]))
	}
	matches := re.re.FindStringSubmatch(toString(args[0]))
	if matches == nil || group < 0 || group >= len(matches) {
		return nil
	}
	return matches[group]
}

// fnRegexpReplace is regexp_replace(source, pattern, replacement [, flags])
// as PostgreSQL answers it: the FIRST match replaced, every match only under
// the 'g' flag (#1481 — it replaced every match whatever the flags), the
// pattern read through translateAndCompile, the replacement's \1 … \9 and
// \& expanded. A flag this engine does not implement is refused, never
// ignored (parseREFlags).
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

// cachedRegexpReplace is the prepared state for (pattern, flags) over the
// pattern translateAndCompile compiled (and cached).
func cachedRegexpReplace(pattern, flags string) (*preparedRegexp, error) {
	f, err := parseREFlags("regexp_replace", flags)
	if err != nil {
		return nil, err
	}
	rx, err := translateAndCompile(pattern, f)
	if err != nil {
		return nil, err
	}
	return preparedFrom(rx, f.global), nil
}

// hasLazyIn reports whether a parsed RE2 pattern holds a non-greedy
// quantifier.
func hasLazyIn(r *syntax.Regexp) bool {
	switch r.Op {
	case syntax.OpStar, syntax.OpPlus, syntax.OpQuest, syntax.OpRepeat:
		if r.Flags&syntax.NonGreedy != 0 {
			return true
		}
	}
	for _, sub := range r.Sub {
		if hasLazyIn(sub) {
			return true
		}
	}
	return false
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
