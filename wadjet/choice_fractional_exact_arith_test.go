package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
)

// Arithmetic OVER a choice that folded to numeric is computed EXACTLY, not in
// float64 (round-2 review, B1r2).
//
// The round-1 fix moved the DECLARATION: `COALESCE(bigint, 1.5)` is numeric on
// PostgreSQL and declares numeric here now, OID 1700 on the wire. The KERNEL
// did not move with it. `resolveDecimalMode` asks each operand for a
// `decimalOperand`, the three CHOOSING constructs do not implement it, so the
// pair fell through to float64 — and the float answer was then boxed at the
// declared scale and published under an exact type:
//
//	                                        PostgreSQL 17.11       was
//	COALESCE(9007199254740993, 1.5) + 1     9007199254740994       9007199254740992.0
//	COALESCE(c_i64, 1.5) * 999999999999     1000002999998999997    1000002999998999900.0
//	(CASE … ELSE 1.5 END) * 12345678901     12345715938036703      12345715938036704.0
//	(CASE … ELSE 1.5 END) + 1234…567        12345678902234570      12345678902234572.0
//
// That is worse than the float8 declaration it replaced: at 2d4220c9 the
// number and the type at least agreed. A declaration is a promise about the
// kernel, which is the B3 position stated the other way round, so the exact
// kernel is selected whenever the fold is numeric — `boxedDecimalOperand` in
// binop_decimal.go reads the choice's own decimal box back at the fold's scale.
//
// The controls below are the reviewer's: `c_dec * 999999999999` was already
// exact through the same kernel, which is what says the kernel existed and was
// simply not being selected.
//
// Every `pg` value here was measured on PostgreSQL 17.11:
//
//	SELECT COALESCE(9007199254740993::bigint,1.5)+1,
//	       COALESCE(1000003::bigint,1.5)*999999999999,
//	       (CASE WHEN true THEN 1000003::bigint ELSE 1.5 END)*12345678901,
//	       (CASE WHEN true THEN 100::bigint ELSE 1.5 END)*9223372036854775807;
//	 9007199254740994 | 1000002999998999997 | 12345715938036703 | 922337203685477580700
//
// The DIGITS must be PostgreSQL's byte for byte. The trailing `.0` this engine
// adds is the recorded per-value-scale divergence (ADR-0024, "a trailing zero
// is the same number", open as #764): a single-scale vector renders every row
// at the fold's scale where PostgreSQL's numeric carries each value's own
// dscale. `sameNumber` strips exactly that and nothing else — a differing
// digit fails.
func TestArithmeticOverANumericChoiceIsExact(t *testing.T) {
	ctx := context.Background()
	db := tmOpen(t)
	tbl := typematrix.Table
	// typemx id 1: c_i64 = 1000003.
	for _, c := range []struct {
		name, sql, pg string
	}{
		// 2^53 + 1 — the first integer a float64 cannot hold. The choice's own
		// BOXING already survived it; the arithmetic did not.
		{"coalesce_past_2_53", `SELECT COALESCE(CAST(9007199254740993 AS BIGINT), 1.5) + 1 AS v FROM ` +
			tbl + ` WHERE id = 1`, "9007199254740994"},
		{"coalesce_wide_product", `SELECT COALESCE(c_i64, 1.5) * 999999999999 AS v FROM ` +
			tbl + ` WHERE id = 1`, "1000002999998999997"},
		{"case_wide_product", `SELECT (CASE WHEN id > 0 THEN c_i64 ELSE 1.5 END) * 12345678901 AS v FROM ` +
			tbl + ` WHERE id = 1`, "12345715938036703"},
		{"case_wide_sum", `SELECT (CASE WHEN id > 0 THEN c_i64 ELSE 1.5 END) + 12345678901234567 AS v FROM ` +
			tbl + ` WHERE id = 1`, "12345678902234570"},
		// The other two choosing constructs, so the fix is the CLASS and not
		// two node types. LEAST/GREATEST are a polymorphic FuncCall; NULLIF
		// resolves under its own operator rule.
		{"least_wide_product", `SELECT LEAST(c_i64, 1.5) * 999999999999 AS v FROM ` +
			tbl + ` WHERE id = 1`, "1499999999998.5"},
		{"greatest_wide_product", `SELECT GREATEST(c_i64, 1.5) * 999999999999 AS v FROM ` +
			tbl + ` WHERE id = 1`, "1000002999998999997"},
		// The CONTROLS the reviewer used: a real DECIMAL column through the
		// same kernel, exact before this change and after it.
		{"ctl_decimal_wide_product", `SELECT c_dec * 999999999999 AS v FROM ` + tbl +
			` WHERE id = 1`, "1000099999998.9999"},
		{"ctl_decimal_wide_sum", `SELECT c_dec + 12345678901234567 AS v FROM ` + tbl +
			` WHERE id = 1`, "12345678901234568.0001"},
		// And an all-integer choice, which must NOT acquire the decimal
		// kernel: no fractional literal, so it stays on the integer rung and
		// keeps PostgreSQL's bigint.
		{"ctl_whole_literal_choice", `SELECT COALESCE(c_i64, 1) * 3 AS v FROM ` + tbl +
			` WHERE id = 1`, "3000009"},
	} {
		t.Run(c.name, func(t *testing.T) {
			res, err := db.Query(ctx, c.sql)
			if err != nil {
				t.Fatalf("%v\n  SQL: %s", err, c.sql)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("%d rows, want 1", len(res.Rows))
			}
			got := fmt.Sprint(res.Rows[0]["v"])
			if !sameNumberText(got, c.pg) {
				t.Errorf("= %q, want PostgreSQL 17.11's %q. A choice that folded to numeric "+
					"declares an EXACT type, so its arithmetic runs on the Int128 carrier; a "+
					"float64 here publishes a rounded number under OID 1700 (round-2 review, "+
					"B1r2)\n  SQL: %s", got, c.pg, c.sql)
			}
		})
	}
}

// sameNumberText compares two rendered decimals for VALUE, allowing only the
// trailing zeros ADR-0024 records as the same number. A differing digit, a
// differing sign or a differing magnitude fails.
func sameNumberText(got, want string) bool {
	trim := func(s string) string {
		if !strings.Contains(s, ".") {
			return s
		}
		s = strings.TrimRight(s, "0")
		return strings.TrimSuffix(s, ".")
	}
	return trim(got) == trim(want)
}
