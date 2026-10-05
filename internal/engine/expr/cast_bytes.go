// SPDX-License-Identifier: MIT

package expr

import (
	"encoding/hex"
	"fmt"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// castToBytes is `CAST(x AS BYTES)`, PostgreSQL's `x::bytea`. It had no arm and
// reached `default: return v`, so the operand came back unchanged under a
// STRING declaration: `CAST('\x6869' AS BYTES)` was the six characters of its
// spelling, a CTAS over it minted a STRING column, and an INSERT of it into a
// BYTES column was 42804 (#1501). Measured on PostgreSQL 17.11:
//
//	CAST('\x6869' AS bytea)            \x6869 — byteain, the text's bytes
//	CAST('hi ' :: char(5) AS bytea)    \x6869202020 — the text, padding kept
//	CAST(b AS bytea)                   b
//	CAST(5 / 1.5 / true / date / uuid / inet AS bytea)
//	                                   42846 cannot cast type integer to bytea
//
// A TEXT operand is read by byteain (kernel.ByteaIn), the one reading every
// text → BYTES door shares, so a cast, an assignment and a comparison of the
// same literal name the same bytes and refuse alike.
func (e *Cast) castToBytes(b *batch.RecordBatch, v any) any {
	if raw, ok := v.([]byte); ok {
		return raw
	}
	s, isText := v.(string)
	if typ, have := e.boolSourceType(b); have && typ != batch.TypeString && typ != batch.TypeBytes {
		// The DECLARATION decides, never the box: a DECIMAL, a network
		// address and a UUID all box as a Go string, and PostgreSQL has no
		// cast from any of them to bytea.
		raiseCannotCastToBytea(pgCastSourceName(typ))
	}
	if !isText {
		raiseCannotCastToBytea(bytesCastSourceName(v))
	}
	raw, err := kernel.ByteaIn(s)
	if err != nil {
		panic(fatalEval{err})
	}
	return raw
}

func raiseCannotCastToBytea(from string) {
	panic(fatalEval{sqlerr.New("42846", "cannot cast type %s to bytea", from)})
}

// bytesCastSourceName names an undeclared non-text box for the 42846 sentence.
func bytesCastSourceName(v any) string {
	switch v.(type) {
	case bool:
		return "boolean"
	case int32:
		return "integer"
	case int, int64:
		return "bigint"
	case float32:
		return "real"
	case float64:
		return "double precision"
	}
	return fmt.Sprintf("%T", v)
}

// BytesValueLiteral is the SQL spelling of a BYTES VALUE that has to travel as
// SQL text — a bound bytea parameter, a scalar subquery's answer substituted
// into a stage, a correlated re-run's outer value: `CAST('\x6869' AS BYTES)`,
// the bytes in byteain's hex form under the type they have.
//
// Spliced raw into a quoted literal, the bytes were SQL's `unknown`, which a
// BYTES consumer reads through byteain a SECOND time (#582): bytes that happen
// to spell another value (`\x41`, two backslashes) became that value, and a
// lone backslash a 22P02 (#1501; parameters-pgwire#r10). The hex form
// round-trips every byte — a NUL and invalid UTF-8 included — and the cast
// makes the value BYTES wherever it lands.
func BytesValueLiteral(raw []byte) *plansql.CastNode {
	return &plansql.CastNode{
		Inner:    &plansql.Lit{Value: `\x` + hex.EncodeToString(raw), Kind: plansql.LitString},
		TypeName: "BYTES",
	}
}
