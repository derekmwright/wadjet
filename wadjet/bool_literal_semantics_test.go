package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
)

// The end-to-end half of #574. The unit tests next door
// (internal/engine/exec/kernel/bool_literal_test.go) pin the vectorized
// kernel; these run real SQL through db.Query, where the SAME
// BOOL-column-versus-text-literal predicate reaches the scan's kernel from a
// WHERE clause and the row-at-a-time evaluator from a SELECT list (and from a
// WHERE clause the scan cannot push, forced with a CASE wrapper). Answering
// them differently was the shipped defect: the kernel read every string as
// FALSE (so `bo = 't'` matched the FALSE rows) while the row evaluator
// rendered the bool as "true"/"false" and string-matched only those exact
// spellings — two wrong answers in opposite directions, and neither was
// PostgreSQL's, which parses t/f/true/false/yes/no/y/n/on/off/1/0 and word
// prefixes as boolean text input.

// tmBoolCount returns the number of fixture rows whose c_bool equals want,
// computed from the fixture itself so a wrong expectation is never copied
// from a wrong engine (the poisoned-baseline failure mode, ADR-0012).
func tmBoolCount(tb testing.TB, want bool) int {
	tb.Helper()
	n := 0
	for _, r := range typematrix.Data(typematrix.Rows) {
		if b, ok := r["c_bool"].(bool); ok && b == want {
			n++
		}
	}
	return n
}

// tmScalarInt runs a query expected to project a single "n" and returns it.
func tmScalarInt(tb testing.TB, ctx context.Context, db *DB, sql string) int64 {
	tb.Helper()
	res, err := tmRun(ctx, db, sql)
	if err != nil {
		tb.Fatalf("%s: %v", sql, err)
	}
	if len(res.Rows) != 1 {
		tb.Fatalf("%s: expected 1 row, got %d", sql, len(res.Rows))
	}
	n, ok := tmAsInt64(res.Rows[0]["n"])
	if !ok {
		tb.Fatalf("%s: n came back as %#v", sql, res.Rows[0]["n"])
	}
	return n
}

// TestBoolLiteralComparisonEndToEnd walks the accepted boolean spellings
// against a real BOOL column on both comparison paths, and asserts they agree
// with each other and with the fixture's own truth.
func TestBoolLiteralComparisonEndToEnd(t *testing.T) {
	ctx := context.Background()
	db := tmOpen(t)

	trueRows := int64(tmBoolCount(t, true))
	falseRows := int64(tmBoolCount(t, false))
	if trueRows == 0 || falseRows == 0 {
		t.Fatalf("fixture needs both TRUE and FALSE c_bool rows, got true=%d false=%d", trueRows, falseRows)
	}

	for _, tc := range []struct {
		lit  string
		want int64
	}{
		{"t", trueRows}, {"true", trueRows}, {"TRUE", trueRows},
		{"True", trueRows}, {"tr", trueRows}, {"yes", trueRows},
		{"y", trueRows}, {"on", trueRows}, {"1", trueRows},
		{"f", falseRows}, {"false", falseRows}, {"FALSE", falseRows},
		{"fals", falseRows}, {"no", falseRows}, {"n", falseRows},
		{"off", falseRows}, {"0", falseRows},
	} {
		t.Run(tc.lit, func(t *testing.T) {
			// Kernel path: a plain WHERE the scanner evaluates vectorized.
			kernelN := tmScalarInt(t, ctx, db, fmt.Sprintf(
				"SELECT COUNT(*) AS n FROM %s WHERE c_bool = '%s'", typematrix.Table, tc.lit))
			if kernelN != tc.want {
				t.Errorf("kernel: WHERE c_bool = '%s' counted %d, want %d", tc.lit, kernelN, tc.want)
			}

			// Row path: a CASE wrapper the scanner cannot push, so the same
			// predicate is evaluated by expr.compare row-at-a-time.
			rowN := tmScalarInt(t, ctx, db, fmt.Sprintf(
				"SELECT COUNT(*) AS n FROM %s WHERE (CASE WHEN c_bool = '%s' THEN 1 ELSE 0 END) = 1",
				typematrix.Table, tc.lit))
			if rowN != tc.want {
				t.Errorf("row: CASE WHEN c_bool = '%s' counted %d, want %d", tc.lit, rowN, tc.want)
			}

			if kernelN != rowN {
				t.Errorf("the two paths disagree for '%s': kernel=%d row=%d", tc.lit, kernelN, rowN)
			}
		})
	}
}

// TestBoolLiteralInListEndToEnd: `c_bool IN ('t','no')` covers both classes,
// on the kernel path and the row path.
func TestBoolLiteralInListEndToEnd(t *testing.T) {
	ctx := context.Background()
	db := tmOpen(t)

	total := int64(tmBoolCount(t, true) + tmBoolCount(t, false)) // non-NULL rows

	kernelN := tmScalarInt(t, ctx, db, fmt.Sprintf(
		"SELECT COUNT(*) AS n FROM %s WHERE c_bool IN ('t','no')", typematrix.Table))
	rowN := tmScalarInt(t, ctx, db, fmt.Sprintf(
		"SELECT COUNT(*) AS n FROM %s WHERE (CASE WHEN c_bool IN ('t','no') THEN 1 ELSE 0 END) = 1",
		typematrix.Table))
	if kernelN != total || rowN != total {
		t.Errorf("c_bool IN ('t','no') counted kernel=%d row=%d, want all %d non-NULL rows", kernelN, rowN, total)
	}
}

// TestNonBooleanLiteralAgainstABoolColumnIsAQueryError: a literal that names
// no boolean cannot mean anything against a BOOL column, and PostgreSQL
// refuses it with 22P02. It used to answer instead — a silent FALSE match
// through the kernel, an exact-spelling string miss through the row
// evaluator, each a wrong answer to a query that has none.
func TestNonBooleanLiteralAgainstABoolColumnIsAQueryError(t *testing.T) {
	ctx := context.Background()
	db := tmOpen(t)

	for _, sql := range []string{
		// Kernel path (plain WHERE).
		fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE c_bool = 'maybe'", typematrix.Table),
		fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE c_bool <> 'maybe'", typematrix.Table),
		fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE c_bool IN ('maybe')", typematrix.Table),
		// Row path (SELECT list, and CASE-wrapped WHERE).
		fmt.Sprintf("SELECT id, c_bool = 'maybe' AS m FROM %s WHERE id = 3", typematrix.Table),
		fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE (CASE WHEN c_bool = 'maybe' THEN 1 ELSE 0 END) = 1",
			typematrix.Table),
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := tmRun(ctx, db, sql)
			if err == nil {
				t.Fatalf("answered instead of refusing: %s", sql)
			}
			if !strings.Contains(err.Error(), "maybe") {
				t.Errorf("the error must quote the literal, got %q", err.Error())
			}
			if !strings.Contains(strings.ToLower(err.Error()), "boolean") {
				t.Errorf("the error must name the boolean type, got %q", err.Error())
			}
		})
	}
}
