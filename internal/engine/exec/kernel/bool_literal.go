package kernel

import "strings"

// ParseBoolText is `parse_bool_with_len` (src/backend/utils/adt/bool.c), which
// is what PostgreSQL's `text::boolean` runs — the boolean INPUT grammar, not a
// rendered-bool string match.
//
// It accepts, case-insensitively and after trimming C `isspace` whitespace,
// any non-empty PREFIX of "true", "false", "yes" or "no", plus "on"/"off" and
// the single characters "1" and "0". The prefix rule is not decoration:
// `'tr'::boolean` and `'fals'::boolean` answer on live PostgreSQL 17, and a
// stricter reader would raise 22P02 for values PostgreSQL accepts. "o" alone
// is the one prefix REFUSED, because it cannot choose between "on" and "off".
//
// This is the ONE binding for the grammar (#574): both comparison paths read a
// BOOL-column-versus-text-literal through it — the vectorized kernel here
// (ResolveFilterKernel's TypeBool arm and inFilterBool) and the row-at-a-time
// expr.compare — so they can no longer disagree with each other or with
// PostgreSQL. internal/engine/expr.parseBoolText delegates here rather than
// keeping a second copy. Before this, kernel.toBool read every string as
// false (so `bo = 't'` matched the FALSE rows) while expr.compare rendered the
// bool as "true"/"false" and matched only those exact spellings — two wrong
// answers in opposite directions, ADR-0012 item 8's two-path split one type
// over from the boxed-pair fixes.
func ParseBoolText(s string) (val, ok bool) {
	t := strings.Trim(s, " \t\n\v\f\r")
	if t == "" {
		return false, false
	}
	switch t[0] {
	case 't', 'T':
		if isPrefixFold(t, "true") {
			return true, true
		}
	case 'f', 'F':
		if isPrefixFold(t, "false") {
			return false, true
		}
	case 'y', 'Y':
		if isPrefixFold(t, "yes") {
			return true, true
		}
	case 'n', 'N':
		if isPrefixFold(t, "no") {
			return false, true
		}
	case 'o', 'O':
		if len(t) < 2 {
			return false, false
		}
		if isPrefixFold(t, "on") {
			return true, true
		}
		if isPrefixFold(t, "off") {
			return false, true
		}
	case '1':
		if len(t) == 1 {
			return true, true
		}
	case '0':
		if len(t) == 1 {
			return false, true
		}
	}
	return false, false
}

// isPrefixFold reports whether s is a case-insensitive prefix of word.
func isPrefixFold(s, word string) bool {
	return len(s) <= len(word) && strings.EqualFold(s, word[:len(s)])
}

// BoolFilterConst resolves a BOOL-column filter constant to a Go bool. A Go
// bool arrives from a parameter; a SQL text literal (Go string, or []byte from
// a binary parameter) is read through PostgreSQL's boolean input grammar. ok
// is false only for a string that names no boolean, and a nil kernel is how
// this package asks the caller (exec.boolConstError) to raise 22P02 — the same
// "nil kernel, caller raises" convention the network and DECIMAL arms use.
func BoolFilterConst(v any) (val, ok bool) {
	switch tv := v.(type) {
	case bool:
		return tv, true
	case string:
		return ParseBoolText(tv)
	case []byte:
		return ParseBoolText(string(tv))
	}
	return false, false
}
