package coordinator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestDecimalArithmeticTwoPath holds the single-process engine and the stage
// DAG to the same EXACT answer for `+ - * / %` over DECIMAL columns — #555's
// execution half, ADR-0024 items 3 and 4.
//
// The two paths reach the arithmetic by different routes and that is the whole
// point of the gate. The single-process pipeline compiles exec.Project from
// the AST and takes the VECTORIZED kernel
// (expr.BinOpNumeric.EvalDecimalVec → kernel.DecimalArithVec), writing
// unscaled carriers straight into the output vector. The DAG ships a
// ProjectExprSpec the worker re-parses and evaluates ROW AT A TIME through the
// boxed path, where the value arrives as decimal TEXT and
// Vector.SetValueChecked parses it back. Two different kernels, one answer
// required — and before this change both answered in float64, so `a - b` over
// 12.75 and 12.7500 came back -9.999999999976694e-05 instead of 0.
//
// Every expectation here is what postgres:17.11 answers for the same values,
// modulo the trailing zeros a single declared scale carries (ADR-0012 item 12's
// recorded class: same number, same rows).
func TestDecimalArithmeticTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	for _, tc := range []struct {
		name string
		expr string
		want []string
	}{
		// a is DECIMAL(9,2), b is DECIMAL(18,4).
		//   +/- : scale max(2,4) = 4
		//   *   : scale 2+4 = 6
		//   %   : scale max(2,4) = 4
		{"add", "a + b", []string{
			"25.5000", "25.5001", "25.4999", "-0.0200", "12.0000", "0.0000", "", "", ""}},
		{"sub", "a - b", []string{
			"0.0000", "-0.0001", "0.0001", "0.0000", "-8.0000", "0.0000", "", "", ""}},
		{"mul", "a * b", []string{
			"162.562500", "162.563775", "162.561225", "0.000100", "20.000000", "0.000000", "", "", ""}},
		{"mod", "a % 3", []string{
			"0.75", "0.75", "0.75", "-0.01", "2.00", "0.00", "", "0.75", ""}},
		// A whole-number literal is DECIMAL(1,0) by its spelling, so the
		// product keeps the column's own scale rather than widening to the
		// INT32 range's ten digits.
		{"column times literal", "a * 2", []string{
			"25.50", "25.50", "25.50", "-0.02", "4.00", "0.00", "", "25.50", ""}},
		{"literal minus column", "100 - a", []string{
			"87.25", "87.25", "87.25", "100.01", "98.00", "100.00", "", "87.25", ""}},
		// A fractional literal contributes its own scale: `a + 0.005` is
		// DECIMAL(11,3), which is what PostgreSQL's numeric answers too.
		{"fractional literal", "a + 0.005", []string{
			"12.755", "12.755", "12.755", "-0.005", "2.005", "0.005", "", "12.755", ""}},
		// A DECIMAL beside an INTEGER COLUMN: the integer brings its whole
		// range at scale 0 (ADR-0024 item 2), so the scale is the decimal's.
		{"column times integer column", "a * id", []string{
			"12.75", "25.50", "38.25", "-0.04", "10.00", "0.00", "", "102.00", ""}},
		// Nesting: the inner result's type is the outer operand's.
		{"nested", "(a + b) * a", []string{
			"325.125000", "325.126275", "325.123725", "0.000200", "24.000000", "0.000000", "", "", ""}},
		// Unary minus keeps the column's own (p,s) and moves no digit.
		{"unary minus", "-a", []string{
			"-12.75", "-12.75", "-12.75", "0.01", "-2.00", "0.00", "", "-12.75", ""}},
		{"unary minus in arithmetic", "-a + b", []string{
			"0.0000", "0.0001", "-0.0001", "0.0000", "8.0000", "0.0000", "", "", ""}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql := fmt.Sprintf("SELECT %s AS v FROM %s ORDER BY id", tc.expr, dbpTable)
			var singleAns, dagAns string
			for _, arm := range []struct {
				name string
				dag  bool
			}{{"single", false}, {"dag", true}} {
				rows := dtpRun(t, ctx, single, coord, sql, arm.dag)
				got := make([]string, 0, len(rows))
				for _, r := range rows {
					if r["v"] == nil {
						got = append(got, "")
						continue
					}
					s, ok := r["v"].(string)
					if !ok {
						t.Fatalf("%s: v = %#v (%T), want the DECIMAL text — a non-string box "+
							"means the answer came back through float64", arm.name, r["v"], r["v"])
					}
					got = append(got, s)
				}
				joined := strings.Join(got, ",")
				if arm.dag {
					dagAns = joined
				} else {
					singleAns = joined
				}
				if joined != strings.Join(tc.want, ",") {
					t.Errorf("%s: %s\n  got  %v\n  want %v", arm.name, sql, got, tc.want)
				}
			}
			if singleAns != dagAns {
				t.Errorf("the two paths disagree:\n  single %s\n  dag    %s", singleAns, dagAns)
			}
		})
	}
}

// TestDecimalArithmeticThroughShuffleTwoPath puts the arithmetic where the DAG
// actually has to CARRY it: a GROUP BY on a computed DECIMAL key, a join key,
// a window PARTITION BY and an ORDER BY.
//
// These are the sites a computed DECIMAL's (p,s) has to survive a stage
// boundary. The key is hashed and shipped as an unscaled carrier plus a scale
// in the .wshf header (ADR-0010), so a stage that rebuilt the expression at a
// different scale would group values a hundredfold apart together — which is
// #533's failure mode one expression up.
func TestDecimalArithmeticThroughShuffleTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"grouped on a computed decimal key",
			"SELECT a * 2 AS g, COUNT(*) AS n FROM " + dbpTable + " GROUP BY a * 2 ORDER BY g"},
		{"grouped on a cross-scale sum",
			"SELECT a + b AS g, COUNT(*) AS n FROM " + dbpTable + " GROUP BY a + b ORDER BY g"},
		{"aggregate over an arithmetic argument",
			"SELECT SUM(a * b) AS s, MIN(a - b) AS lo, MAX(a + b) AS hi FROM " + dbpTable},
		{"ordered by a computed decimal",
			"SELECT id FROM " + dbpTable + " ORDER BY a * b, id"},
		{"filtered on a computed decimal",
			"SELECT COUNT(*) AS n FROM " + dbpTable + " WHERE a * 2 > 10"},
		{"window partitioned by a computed decimal",
			"SELECT id, MIN(b) OVER (PARTITION BY a * 2) AS w FROM " + dbpTable + " ORDER BY id"},
		{"joined on a computed decimal key",
			"SELECT COUNT(*) AS n FROM " + dbpTable + " x JOIN " + dbpTable + " y ON x.a * 2 = y.a * 2"},
		{"set operation over a computed arm",
			"SELECT a * 2 AS v FROM " + dbpTable + " UNION ALL SELECT b FROM " + dbpTable + " ORDER BY 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			singleRows := dtpRun(t, ctx, single, coord, tc.sql, false)
			dagRows := dtpRun(t, ctx, single, coord, tc.sql, true)
			if got, want := fmt.Sprint(dagRows), fmt.Sprint(singleRows); got != want {
				t.Errorf("%s\n  single %s\n  dag    %s", tc.sql, want, got)
			}
		})
	}
}

// TestDecimalArithmeticFaultsAreErrorsOnBothPaths is ADR-0024 item 4 at the
// arithmetic sites: a value with no carrier at its declared type is a 22003
// ERROR and a zero divisor is 22012, on BOTH paths — never a wrapped number,
// and never a NULL that looks like missing data.
//
// The two SQLSTATEs are separate because PostgreSQL keeps them separate, and a
// caller that branched on "did it produce a value" would report a numeric
// overflow for `x / 0`.
func TestDecimalArithmeticFaultsAreErrorsOnBothPaths(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	for _, tc := range []struct {
		name string
		sql  string
		want string
	}{
		{"division by zero", "SELECT a / 0 AS v FROM " + dbpTable, "division by zero"},
		{"modulo by zero", "SELECT a % 0 AS v FROM " + dbpTable, "division by zero"},
		{"division by a zero column", "SELECT b / a AS v FROM " + dbpTable, "division by zero"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range []struct {
				name string
				dag  bool
			}{{"single", false}, {"dag", true}} {
				var err error
				if arm.dag {
					_, err = tmdRunDAG(ctx, coord, tc.sql)
				} else {
					_, err = tmdRunSingle(ctx, single, tc.sql)
				}
				if err == nil {
					t.Fatalf("%s: %s answered instead of failing — an unrepresentable value "+
						"is an error, never a number (ADR-0024 item 4)", arm.name, tc.sql)
				}
				if !strings.Contains(err.Error(), tc.want) {
					t.Errorf("%s: %s failed with %q, want a message naming %q",
						arm.name, tc.sql, err, tc.want)
				}
			}
		})
	}
}
