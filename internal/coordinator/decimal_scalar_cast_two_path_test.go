package coordinator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestDecimalScalarAndCastTwoPath holds both execution paths to the same exact
// answer for the scalar math functions (#668) and for CAST to and from DECIMAL
// (#555's cast half).
//
// The two arms reach them differently, and that is the point. The
// single-process pipeline takes the VECTORIZED kernel
// (expr.decimalScalarFn.EvalDecimalVec into kernel.DecimalScalarVec), writing
// carriers straight into the output vector; the DAG evaluates row at a time
// through the boxed path, where the value arrives as decimal TEXT and
// Vector.SetValueChecked parses it back. Before this both answered through
// float64: ROUND over a DECIMAL made a round trip through a double, and a
// parameterized CAST passed its operand through with the (p,s) ignored
// entirely.
//
// Every expectation is what postgres:17.11 answers for the same value, modulo
// the trailing zeros one declared scale carries (ADR-0012 item 12's class).
func TestDecimalScalarAndCastTwoPath(t *testing.T) {
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
		// a is DECIMAL(9,2) and holds 12.75, 12.75, 12.75, -0.01, 2.00, 0.00,
		// NULL, 12.75, NULL across the nine rows.
		{"round to one digit", "ROUND(a, 1)", []string{
			"12.8", "12.8", "12.8", "0.0", "2.0", "0.0", "", "12.8", ""}},
		{"round to integer", "ROUND(a)", []string{
			"13", "13", "13", "0", "2", "0", "", "13", ""}},
		{"abs", "ABS(a)", []string{
			"12.75", "12.75", "12.75", "0.01", "2.00", "0.00", "", "12.75", ""}},
		{"ceil", "CEIL(a)", []string{
			"13", "13", "13", "0", "2", "0", "", "13", ""}},
		{"floor", "FLOOR(a)", []string{
			"12", "12", "12", "-1", "2", "0", "", "12", ""}},
		{"sign", "SIGN(a)", []string{
			"1", "1", "1", "-1", "1", "0", "", "1", ""}},
		{"trunc", "TRUNC(a, 1)", []string{
			"12.7", "12.7", "12.7", "0.0", "2.0", "0.0", "", "12.7", ""}},
		{"mod", "MOD(a, 3)", []string{
			"0.75", "0.75", "0.75", "-0.01", "2.00", "0.00", "", "0.75", ""}},

		// CAST: narrowing rounds, widening is exact, an integer gains the
		// scale, and a BARE destination keeps the operand's own.
		{"cast narrowing", "CAST(b AS DECIMAL(9,2))", []string{
			"12.75", "12.75", "12.75", "-0.01", "10.00", "0.00", "1.00", "", ""}},
		{"cast widening", "CAST(a AS DECIMAL(18,6))", []string{
			"12.750000", "12.750000", "12.750000", "-0.010000", "2.000000", "0.000000",
			"", "12.750000", ""}},
		{"cast from an integer", "CAST(id AS DECIMAL(10,2))", []string{
			"1.00", "2.00", "3.00", "4.00", "5.00", "6.00", "7.00", "8.00", "9.00"}},
		{"bare cast", "CAST(a AS NUMERIC)", []string{
			"12.75", "12.75", "12.75", "-0.01", "2.00", "0.00", "", "12.75", ""}},

		// A cast that NAMES its type is an exact arithmetic operand, and so is
		// a scalar function's result.
		{"cast in arithmetic", "CAST(b AS DECIMAL(9,2)) * 2", []string{
			"25.50", "25.50", "25.50", "-0.02", "20.00", "0.00", "2.00", "", ""}},
		{"scalar function in arithmetic", "ROUND(a, 1) * 2", []string{
			"25.6", "25.6", "25.6", "0.0", "4.0", "0.0", "", "25.6", ""}},
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
