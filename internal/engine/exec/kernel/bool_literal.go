package kernel

import "strings"

// ParseBoolText is the shared PostgreSQL boolean input grammar (#574),
// used by vector and boxed comparisons and expr.parseBoolText.
// Trim C isspace only and accept case-insensitive nonempty prefixes of
// true/false/yes/no, unambiguous on/off prefixes, and single 1/0.
// Reject empty input and ambiguous "o"; do not merely match rendered booleans.
// Every path must use the same grammar (ADR-0012 item 8).
// See docs/internals/kernel-boolean-input-grammar.md for the design.
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
