// This file holds expr encoding fns; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"encoding/base64"
	"strconv"
	"strings"
)

// --- Encoding functions ---

// to_hex(n) is PostgreSQL's, and PostgreSQL renders the TWO'S COMPLEMENT of a
// negative argument at the argument's own width, measured on 17.11:
//
//	to_hex((-1)::int4)                  ffffffff
//	to_hex((-1)::int8)                  ffffffffffffffff
//	to_hex(4611686018427387922::int8)   4000000000000012
//	to_hex(255)                         ff   (no padding)
//
// `fmt.Sprintf("%x", int64(...))` answered `-1` — Go's rendering of a negative
// integer, not a hexadecimal machine word — and the argument came through a
// float64, so the wide value above lost its low bits and answered
// 4000000000000000 (#966 round 2). The width comes from the argument's own box,
// which is what makes an INT32 column answer eight digits and a BIGINT one
// sixteen.
func fnToHex(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	if v, ok := args[0].(int32); ok {
		return strconv.FormatUint(uint64(uint32(v)), 16)
	}
	return strconv.FormatUint(uint64(bitIntArg(args[0])), 16)
}

// from_hex(s) is to_hex's inverse and PostgreSQL has no integer-returning
// counterpart for it, so the contract is set by the two things that DO
// adjudicate: PostgreSQL's own hexadecimal decoder, and this engine's own
// FROM_BASE.
//
//	decode('12zz','hex')     ERROR 22023, invalid hexadecimal digit
//	FROM_BASE('12z', 16)     NULL  (strconv.ParseInt requires exhaustion)
//
// It read its argument with `fmt.Sscanf(..., "%x")`, which stops at the first
// character it cannot use and reports success for what it consumed, so
// `FROM_HEX('12zz')` answered 18 — a number derived from text that is not a
// number, and the opposite of what FROM_BASE answers for the same input
// (#966 round 2, N2). Two spellings of one operation disagreeing is the
// defect; the whole string is required now, and text that is not hexadecimal
// is NULL, which is FROM_BASE's answer and this family's existing shape for
// unparseable input.
//
// A 16-digit word with the top bit set (`ffffffffffffffff`) stays NULL: the
// result is a signed int64 and that value does not fit one. FROM_BASE declines
// it identically.
func fnFromHex(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	n, err := strconv.ParseInt(strings.TrimSpace(toString(args[0])), 16, 64)
	if err != nil {
		return nil
	}
	// The int64 it parsed, not a double of it: FROM_HEX('4000000000000012')
	// answered 4611686018427387904 for 4611686018427387922 (#966 round 2).
	return n
}

func fnToBase64(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return base64.StdEncoding.EncodeToString([]byte(toString(args[0])))
}

func fnFromBase64(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	decoded, err := base64.StdEncoding.DecodeString(toString(args[0]))
	if err != nil {
		return nil
	}
	return string(decoded)
}
