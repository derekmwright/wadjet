// This file holds expr string padding; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"strings"
)

// --- String: padding and character ---

func fnLPad(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	// LPAD measures WIDTH in characters, and both the truncation and the fill
	// used to measure bytes: LPAD('éàü', 5, 'x') answered two characters and
	// half of a third — invalid UTF-8 — where PostgreSQL answers `xxéàü`
	// (#856).
	r := []rune(toString(args[0]))
	n := int(ToFloat64(args[1]))
	pad := []rune(" ")
	if len(args) >= 3 && args[2] != nil {
		pad = []rune(toString(args[2]))
	}
	if len(pad) == 0 || n <= len(r) {
		if n < 0 {
			return ""
		}
		if n <= len(r) {
			return string(r[:n])
		}
		return string(r)
	}
	fill := make([]rune, 0, n-len(r))
	for len(fill) < n-len(r) {
		fill = append(fill, pad...)
	}
	return string(fill[:n-len(r)]) + string(r)
}

func fnRPad(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	// CHARACTERS, like LPAD above (#856).
	r := []rune(toString(args[0]))
	n := int(ToFloat64(args[1]))
	pad := []rune(" ")
	if len(args) >= 3 && args[2] != nil {
		pad = []rune(toString(args[2]))
	}
	if len(pad) == 0 || n <= len(r) {
		if n < 0 {
			return ""
		}
		if n <= len(r) {
			return string(r[:n])
		}
		return string(r)
	}
	out := append([]rune(nil), r...)
	for len(out) < n {
		out = append(out, pad...)
	}
	return string(out[:n])
}

func fnChr(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	code := int64(ToFloat64(args[0]))
	// PostgreSQL's three refusals, each with its own SQLSTATE (#855). Zero is
	// the one that matters most: a NUL cannot travel in a text-format DataRow
	// and libpq truncates at one, so `CHR(0)` answered a one-character string
	// here and an empty one to psql — #570's shape reached through a function.
	switch {
	case code < 0:
		raiseChrNotPositive()
	case code == 0:
		raiseChrNul()
	case code > 0x10FFFF:
		raiseChrTooLarge(code)
	}
	return string(rune(code))
}

func fnCodepoint(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := toString(args[0])
	if len(s) == 0 {
		return nil
	}
	runes := []rune(s)
	return int32(runes[0])
}

func fnConcatWS(args []any) any {
	if len(args) < 2 || args[0] == nil {
		return nil
	}
	sep := toString(args[0])
	parts := make([]string, 0, len(args)-1)
	for _, a := range args[1:] {
		if a == nil {
			continue // skip nulls
		}
		parts = append(parts, toString(a))
	}
	return strings.Join(parts, sep)
}

func fnCharLength(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	// bytea has no characters, so this is its BYTE count — the same answer
	// `length` gives, which is what keeps the two synonyms from disagreeing
	// over one value. PostgreSQL has no `char_length(bytea)` at all (42883,
	// measured), so answering is the superset ADR-0012 records for the
	// text-only family; answering two different numbers for two spellings of
	// one function is not (#583 round 2, B4).
	if raw, ok := args[0].([]byte); ok {
		return int32Count(len(raw))
	}
	return int32Count(len([]rune(toString(args[0]))))
}

func fnTranslate(args []any) any {
	if len(args) < 3 || args[0] == nil || args[1] == nil || args[2] == nil {
		return nil
	}
	s := toString(args[0])
	from := []rune(toString(args[1]))
	to := []rune(toString(args[2]))
	mapping := make(map[rune]rune)
	for i, r := range from {
		if i < len(to) {
			mapping[r] = to[i]
		} else {
			mapping[r] = -1 // mark for deletion
		}
	}
	var sb strings.Builder
	for _, r := range s {
		if repl, ok := mapping[r]; ok {
			if repl >= 0 {
				sb.WriteRune(repl)
			}
		} else {
			sb.WriteRune(r)
		}
	}
	return sb.String()
}
