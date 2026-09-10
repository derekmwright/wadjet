// This file holds expr string fns; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"strings"
	"unicode/utf8"
)

// --- String function implementations ---

func fnUpper(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return strings.ToUpper(toString(args[0]))
}

func fnLower(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return strings.ToLower(toString(args[0]))
}

// ConcatOpFunc is the registry name of the `||` OPERATOR's implementation.
//
// It is punctuation on purpose. A registry entry is reachable from SQL only
// if a query can spell its name, and no SQL identifier — bare or delimited —
// is `||`: the lexer reads those two bytes as an operator token before any
// identifier rule sees them. So the operator's NULL-propagating kernels
// cannot be invoked as a function, and `CONCAT` cannot reach them (#609).
// The census carries a fixture that attempts both spellings, because
// "unspellable" is a claim and the protocol's method 10 says a claim gets a
// fixture rather than a comment.
const ConcatOpFunc = "||"

// fnConcat is the CONCAT() FUNCTION, which IGNORES NULL arguments —
// `CONCAT('a', NULL, 'b')` is `ab` and `CONCAT(NULL, NULL)` is the EMPTY
// STRING, never NULL (PostgreSQL 17; ADR-0012 makes it the authority). It is
// fnConcatWS without a separator, which is where the rule was already right
// (#609).
//
// The `||` OPERATOR is the other rule and has its own kernels below: it
// propagates NULL. The two were one function until #609, which is why
// making this one NULL-tolerant is only half the fix.
func fnConcat(args []any) any {
	var sb strings.Builder
	for _, a := range args {
		if a == nil {
			continue
		}
		sb.WriteString(toString(a))
	}
	return sb.String()
}

// fnConcatOp is the `||` OPERATOR: NULL in any operand makes the whole
// expression NULL. Registered under a name no SQL identifier can spell, so
// the operator and the function cannot be confused for one another by a
// query — compile.go lowers `||` to it (#609).
func fnConcatOp(args []any) any {
	// `bytea || bytea` is BYTEA on the server, and an unknown-typed literal
	// beside one is read as bytea too — so `b || 'x'` is the bytes of b
	// followed by 0x78, under OID 17 rather than under text's 25 (#583). The
	// raw bytes of a non-UTF-8 value under a declared `text` is exactly the
	// embedded-NUL field #570 removed for the column itself.
	bytesResult := false
	for _, a := range args {
		if _, ok := a.([]byte); ok {
			bytesResult = true
			break
		}
	}
	if bytesResult {
		out := make([]byte, 0, 16)
		for _, a := range args {
			if a == nil {
				return nil
			}
			if raw, ok := a.([]byte); ok {
				out = append(out, raw...)
				continue
			}
			out = append(out, toString(a)...)
		}
		return out
	}
	var sb strings.Builder
	for _, a := range args {
		if a == nil {
			return nil
		}
		sb.WriteString(toString(a))
	}
	return sb.String()
}

// substrBytes is fnSubstr's bytea arm: PostgreSQL's substring over bytea takes
// the same window rule and the same 22011 refusal for a negative length, over
// BYTES rather than characters.
func substrBytes(raw []byte, args []any) any {
	start := int(ToFloat64(args[1])) - 1
	if len(args) >= 3 && args[2] != nil {
		length := int(ToFloat64(args[2]))
		if length < 0 {
			raiseNegativeSubstringLength()
		}
		lo, hi := substrWindow(start, length, len(raw))
		out := make([]byte, hi-lo)
		copy(out, raw[lo:hi])
		return out
	}
	if start < 0 {
		start = 0
	}
	if start >= len(raw) {
		return []byte{}
	}
	out := make([]byte, len(raw)-start)
	copy(out, raw[start:])
	return out
}

// fnLength is LENGTH / LEN, and it counts CHARACTERS.
//
// `length` and `character_length` are synonyms in PostgreSQL and disagreed
// here: LENGTH('éàü') was 6 — a byte count — beside CHARACTER_LENGTH's 3 on
// the same input (#856). OCTET_LENGTH and BIT_LENGTH are the byte-counting
// spellings and are unchanged.
func fnLength(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	// bytea has no characters, so `length(bytea)` is its BYTE count on the
	// server — the same number octet_length gives — and reading its bytes as
	// UTF-8 answered 1 for the two bytes of an encoded 'e' (#583).
	if raw, ok := args[0].([]byte); ok {
		return int32Count(len(raw))
	}
	return int32Count(utf8.RuneCountInString(toString(args[0])))
}

// fnSubstr is SUBSTRING / SUBSTR, and it indexes CHARACTERS.
//
// Byte indexing did not merely mislabel: `SUBSTR('éàü', 2, 2)` cut both
// two-byte characters in half and produced a string that is not valid UTF-8
// (#856). PostgreSQL answers `àü`.
func fnSubstr(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	// `substring(bytea, ...)` exists on the server and returns BYTEA, indexed
	// by BYTES: `substring('\xff\xfe\x00A' from 1 for 1)` is `\xff`. Read
	// as text it produced the UTF-8 replacement character instead — #570's own
	// hazard coming back through a derived value (#583).
	if raw, ok := args[0].([]byte); ok {
		return substrBytes(raw, args)
	}
	r := []rune(toString(args[0]))
	start := int(ToFloat64(args[1])) - 1 // SQL is 1-indexed
	if len(args) >= 3 && args[2] != nil {
		length := int(ToFloat64(args[2]))
		// PostgreSQL refuses a NEGATIVE length with 22011 rather than
		// answering the empty string, which is what substrWindow's own doc
		// recorded as unreachable while the per-row error channel did not
		// exist. It does (#347), so this refuses like the server.
		if length < 0 {
			raiseNegativeSubstringLength()
		}
		lo, hi := substrWindow(start, length, len(r))
		return string(r[lo:hi])
	}
	if start < 0 {
		start = 0
	}
	if start >= len(r) {
		return ""
	}
	return string(r[start:])
}

// substrWindow clamps SUBSTR's [start, start+length) character window (both
// 0-based here) into valid slice bounds for a string of length n. The window
// rule is PostgreSQL's (#373): a start below position 1 consumes part of the
// length before the string begins — SUBSTR('abcdef', 0, 3) selects positions
// 0,1,2 of which only 1 and 2 exist, so 'ab' and not 'abc'. A non-positive
// or overflowed window is empty rather than an error; a NEGATIVE length is
// SQLSTATE 22011 on the server and the callers raise it before reaching here
// (#856). n is a CHARACTER count in every caller.
func substrWindow(start, length, n int) (int, int) {
	end := start + length
	if length < 0 || end < start { // negative length or overflow
		end = start
	}
	if start < 0 {
		start = 0
	}
	if start > n {
		start = n
	}
	if end < start {
		end = start
	}
	if end > n {
		end = n
	}
	return start, end
}

func fnTrim(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return strings.TrimSpace(toString(args[0]))
}

func fnLTrim(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return strings.TrimLeft(toString(args[0]), " \t\n\r")
}

func fnRTrim(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return strings.TrimRight(toString(args[0]), " \t\n\r")
}

func fnReplace(args []any) any {
	if len(args) < 3 || args[0] == nil || args[1] == nil || args[2] == nil {
		return nil
	}
	return strings.ReplaceAll(toString(args[0]), toString(args[1]), toString(args[2]))
}

func fnReverse(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	runes := []rune(toString(args[0]))
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}

func fnLeft(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	// LEFT counts CHARACTERS (#856). It is not reachable from SQL today — the
	// parser reserves LEFT and RIGHT for the join keywords — and it is fixed
	// with the rest of the family anyway, because a byte-indexing sibling left
	// behind is exactly the two-implementation drift this class keeps
	// producing.
	r := []rune(toString(args[0]))
	n := int(ToFloat64(args[1]))
	if n < 0 {
		return ""
	}
	if n >= len(r) {
		return string(r)
	}
	return string(r[:n])
}

func fnRight(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	// CHARACTERS, like LEFT above (#856).
	r := []rune(toString(args[0]))
	n := int(ToFloat64(args[1]))
	if n < 0 {
		return ""
	}
	if n >= len(r) {
		return string(r)
	}
	return string(r[len(r)-n:])
}

func fnStartsWith(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	return strings.HasPrefix(toString(args[0]), toString(args[1]))
}

func fnEndsWith(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	return strings.HasSuffix(toString(args[0]), toString(args[1]))
}

func fnContains(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	return strings.Contains(toString(args[0]), toString(args[1]))
}

func fnRepeat(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	n := int(ToFloat64(args[1]))
	if n < 0 {
		return ""
	}
	return strings.Repeat(toString(args[0]), n)
}
