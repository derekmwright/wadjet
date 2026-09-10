// This file holds expr string regex; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"regexp"
	"strings"
	"unicode/utf8"
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
} // pattern string → *regexp.Regexp (nil for invalid)

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

func fnRegexpReplace(args []any) any {
	if len(args) < 3 || args[0] == nil || args[1] == nil || args[2] == nil {
		return nil
	}
	re := compileRegexpCached(toString(args[1]))
	if re == nil {
		return nil
	}
	return re.ReplaceAllString(toString(args[0]), sqlBackrefsToGo(toString(args[2])))
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
