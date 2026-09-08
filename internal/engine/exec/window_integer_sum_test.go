package exec

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// THE ACCUMULATOR SEAM — #987, #813, #953.
//
// A HashAggregate and a Window are two producers of one answer: `SUM(x)
// GROUP BY g` and `SUM(x) OVER (PARTITION BY g)` are the same question
// written twice, and a BI tool flips between the spellings freely. Until
// #987 they held two accumulators — an exact Int128 in the aggregate (#784)
// and a float64 in the window — so past 2^53 they answered different numbers
// under different types, and no gate in the tree compared them.
//
// This is the group-key seam gate's rule applied one operator over: it needs
// no plan, no budget and no cluster, which is the point — a gate whose
// trigger is a CONDITION cannot be relied on to fire.

// wisRows is the fixture: values chosen so the total sits ABOVE 2^53
// (9007199254740992), where a float64's ulp is 2 and the answer therefore
// depends on the order the rows are added in.
func wisRows() (int64, []int64) {
	vals := []int64{9007199254740993, 2147483648, 16777217, -20, 12, 3, 2, 0}
	var total int64
	for _, v := range vals {
		total += v
	}
	return total, vals
}

// wisAggValue runs the GROUPED spelling over one group and returns the
// rendered answer and its declared column type.
func wisAggValue(tb testing.TB, in parquet.Column, vals []int64, avg bool) (string, parquet.Column) {
	tb.Helper()
	fn := AggSum
	if avg {
		fn = AggAvg
	}
	out, prec, scale, ok := IntegerAccOutputType(avg, in.Type)
	if !ok {
		tb.Fatalf("IntegerAccOutputType declines %v", in.Type)
	}
	agg := NewHashAggregate([]string{"g"}, []AggColumn{{
		Func: fn, InputCol: in.Name, OutputCol: "v",
		OutputType: out, OutputPrecision: prec, OutputScale: scale,
	}})
	ctx := context.Background()
	if err := agg.Init(ctx); err != nil {
		tb.Fatal(err)
	}
	defer agg.Close()
	schema := []parquet.Column{{Name: "g", Type: parquet.TypeInt64}, in}
	if err := agg.Consume(ctx, batch.FromRows(schema, wisInputRows(in, vals))); err != nil {
		tb.Fatal(err)
	}
	if err := agg.Finalize(ctx); err != nil {
		tb.Fatal(err)
	}
	b, err := agg.Next(ctx)
	if err != nil {
		tb.Fatal(err)
	}
	if b == nil || b.Len != 1 {
		tb.Fatalf("expected one group, got %v", b)
	}
	col := parquet.Column{}
	for _, c := range b.Schema {
		if c.Name == "v" {
			col = c
		}
	}
	return fmt.Sprint(b.ToRows()[0]["v"]), col
}

// wisWindowValue runs the WINDOWED spelling over the same one group.
func wisWindowValue(tb testing.TB, in parquet.Column, vals []int64, avg bool) (string, parquet.Column) {
	tb.Helper()
	fn := WinSum
	if avg {
		fn = WinAvg
	}
	schema := []parquet.Column{{Name: "g", Type: parquet.TypeInt64}, in}
	// OutputType is deliberately left at the pre-#987 FLOAT64 fallback: what
	// this asserts is that the OPERATOR corrects a declaration it was handed
	// (retypeValueColumns), which is the path a stage spec the coordinator
	// could not type actually takes.
	cols := []WindowColumn{{
		Func: fn, InputCol: in.Name, OutputCol: "v",
		OutputType: parquet.TypeFloat64, PartitionBy: []string{"g"},
	}}
	rows, outSchema := runWindowInMemory(tb, schema, cols, wisInputRows(in, vals))
	if len(rows) == 0 {
		tb.Fatal("the window produced no rows")
	}
	col := parquet.Column{}
	for _, c := range outSchema {
		if c.Name == "v" {
			col = c
		}
	}
	return fmt.Sprint(rows[0]["v"]), col
}

func wisInputRows(in parquet.Column, vals []int64) []map[string]any {
	rows := make([]map[string]any, 0, len(vals)+1)
	for _, v := range vals {
		var boxed any = v
		if in.Type != parquet.TypeInt64 {
			boxed = int32(v)
		}
		rows = append(rows, map[string]any{"g": int64(0), in.Name: boxed})
	}
	// A NULL row, which is excluded from the sum AND from AVG's divisor on
	// both spellings.
	rows = append(rows, map[string]any{"g": int64(0), in.Name: nil})
	return rows
}

// TestTheWindowAndGroupedIntegerSumWriteTheSameValue is the seam. Per integer
// type, per function, the two producers must render the same digits AND
// declare the same column.
func TestTheWindowAndGroupedIntegerSumWriteTheSameValue(t *testing.T) {
	_, wide := wisRows()
	narrow := []int64{2147483647, 16777217, -20, 12, 3, 2, 0}
	for _, in := range []parquet.Column{
		{Name: "v", Type: parquet.TypeInt64, Nullable: true},
		{Name: "v", Type: parquet.TypeInt32, Nullable: true},
	} {
		vals := wide
		if in.Type != parquet.TypeInt64 {
			vals = narrow
		}
		for _, avg := range []bool{false, true} {
			name := fmt.Sprintf("%s/%s", in.Type, map[bool]string{false: "sum", true: "avg"}[avg])
			t.Run(name, func(t *testing.T) {
				gv, gc := wisAggValue(t, in, vals, avg)
				wv, wc := wisWindowValue(t, in, vals, avg)
				if gv != wv {
					t.Errorf("GROUPED = %s, WINDOWED = %s — one question, two numbers", gv, wv)
				}
				if gc.Type != wc.Type || gc.Precision != wc.Precision || gc.Scale != wc.Scale {
					t.Errorf("GROUPED declares %v(%d,%d), WINDOWED declares %v(%d,%d)",
						gc.Type, gc.Precision, gc.Scale, wc.Type, wc.Precision, wc.Scale)
				}
			})
		}
	}
}

// TestWindowIntegerSumIsExactAboveTwoToThe53 is #987 itself: the digits, in
// EVERY frame form, over values whose running totals sit above 2^53.
//
// Every `want` is PostgreSQL 17.11's, taken live over the same eight values
// (`k2_numwidth`-shaped: ORDER BY the row index, one partition). A float64
// accumulator answers some of these two ulps low and — because its error
// depends on the ORDER the rows arrive in — not always the same two.
func TestWindowIntegerSumIsExactAboveTwoToThe53(t *testing.T) {
	// ts 0..7, ascending; the values are wisRows' in that order.
	_, vals := wisRows()
	schema := []parquet.Column{
		{Name: "ts", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64, Nullable: true},
	}
	rows := make([]map[string]any, len(vals))
	for i, v := range vals {
		rows[i] = map[string]any{"ts": int64(i), "v": v}
	}

	for _, tc := range []struct {
		name  string
		col   WindowColumn
		want  map[int64]string
		wantT parquet.TypeID
	}{
		{
			name: "OVER ()",
			col:  WindowColumn{Func: WinSum, InputCol: "v", OutputCol: "w", OutputType: parquet.TypeFloat64},
			want: map[int64]string{
				0: "9007201419001855", 7: "9007201419001855",
			},
			wantT: parquet.TypeDecimal,
		},
		{
			name: "ORDER BY — the running frame",
			col: WindowColumn{Func: WinSum, InputCol: "v", OutputCol: "w", OutputType: parquet.TypeFloat64,
				OrderBy: []SortKey{{Column: "ts", Order: Ascending}}},
			want: map[int64]string{
				0: "9007199254740993",
				1: "9007201402224641",
				2: "9007201419001858",
				3: "9007201419001838",
				4: "9007201419001850",
				5: "9007201419001853",
				6: "9007201419001855",
				7: "9007201419001855",
			},
			wantT: parquet.TypeDecimal,
		},
		{
			name: "ROWS 1 PRECEDING AND 1 FOLLOWING — the sliding frame's exit subtraction",
			col: WindowColumn{Func: WinSum, InputCol: "v", OutputCol: "w", OutputType: parquet.TypeFloat64,
				OrderBy: []SortKey{{Column: "ts", Order: Ascending}},
				Frame: &WindowFrameSpec{Mode: "rows",
					Start: WindowBound{Type: "preceding", Offset: 1},
					End:   WindowBound{Type: "following", Offset: 1}}},
			want: map[int64]string{
				0: "9007201402224641",
				1: "9007201419001858",
				2: "2164260845",
				3: "16777209",
				4: "-5",
				5: "17",
				6: "5",
				7: "2",
			},
			wantT: parquet.TypeDecimal,
		},
		{
			name: "AVG OVER () — the digits the float mean loses",
			col:  WindowColumn{Func: WinAvg, InputCol: "v", OutputCol: "w", OutputType: parquet.TypeFloat64},
			want: map[int64]string{
				0: "1125900177375231.8750",
			},
			wantT: parquet.TypeDecimal,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, outSchema := runWindowInMemory(t, schema, []WindowColumn{tc.col}, rows)
			got := byTS(t, out)
			for _, c := range outSchema {
				if c.Name == "w" && c.Type != tc.wantT {
					t.Fatalf("declared %v, want %v — the exact carrier is what the type says it is",
						c.Type, tc.wantT)
				}
			}
			for ts, want := range tc.want {
				if g := fmt.Sprint(got[ts]["w"]); g != want {
					t.Errorf("ts=%d: w = %s, want %s (PostgreSQL 17)", ts, g, want)
				}
			}
		})
	}
}

// TestAnInt64WindowSumPastTheCarrierAnswersInNumeric is the CONTROL that says
// why the bigint arm below has to be reached directly.
//
// `SUM(int8) OVER ()` declares NUMERIC, so a total of 3*(2^63-1) is a number
// this engine can answer, and PostgreSQL answers the same digits. The
// operator's own correction (retypeValueColumns) is what gets it there: the
// spec below asks for an INT64 output over an int8 column, which is the
// declaration a pre-#987 stage spec carries, and the operator moves it UP to
// the accumulator's type rather than writing a wrapped int64.
func TestAnInt64WindowSumPastTheCarrierAnswersInNumeric(t *testing.T) {
	schema := []parquet.Column{
		{Name: "ts", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64, Nullable: true},
	}
	rows := []map[string]any{
		{"ts": int64(0), "v": int64(9223372036854775807)},
		{"ts": int64(1), "v": int64(9223372036854775807)},
		{"ts": int64(2), "v": int64(9223372036854775807)},
	}
	cols := []WindowColumn{{
		Func: WinSum, InputCol: "v", OutputCol: "w", OutputType: parquet.TypeInt64,
	}}
	out, outSchema := runWindowInMemory(t, schema, cols, rows)
	for _, c := range outSchema {
		if c.Name == "w" && c.Type != parquet.TypeDecimal {
			t.Fatalf("declared %v — sum(int8) is numeric, and an int64 there would wrap", c.Type)
		}
	}
	// PostgreSQL 17.11 over the same three rows, measured live.
	const want = "27670116110564327421"
	if got := fmt.Sprint(out[0]["w"]); got != want {
		t.Errorf("w = %s, want %s (PostgreSQL 17)", got, want)
	}
}

// TestWindowBigintSumRefusesAWrappedTotal — a total the BIGINT declaration
// cannot hold is 22003, PostgreSQL's own SQLSTATE for `bigint out of range`,
// never a wrapped number (ADR-0024 item 4).
//
// The bigint arm is `SUM(int4)`, and its input cannot overflow an int64
// without 2^32 rows, so no query-level fixture can reach the refusal — which
// makes it exactly the "cannot happen" that method 10 of the correctness-fix
// protocol says needs a fixture attempting it. windowExactFrames is called
// directly, with the vectors a spec of that shape would produce.
func TestWindowBigintSumRefusesAWrappedTotal(t *testing.T) {
	const n = 3
	inCol := parquet.Column{Name: "v", Type: parquet.TypeInt64, Nullable: true}
	inputVec := batch.NewColumnVector(inCol, n)
	for i := 0; i < n; i++ {
		inputVec.SetValue(i, int64(9223372036854775807))
	}
	winVec := batch.NewColumnVector(parquet.Column{
		Name: "w", Type: parquet.TypeInt64, Nullable: true}, n)
	winVec.Nulls = batch.NewBitmapAllNull(n)

	wc := WindowColumn{Func: WinSum, InputCol: "v", OutputCol: "w", OutputType: parquet.TypeInt64}
	cells, exact := resolveWindowExactCells(winVec, inputVec)
	if !exact {
		t.Fatal("an int64 column into a bigint output is not on the exact path")
	}
	err := windowExactFrames(winVec, inputVec, cells, resolveFrame(wc, n, false, nil), 0, n, wc)
	if err == nil {
		t.Fatalf("a total of 3*(2^63-1) came back as %v — a wrapped number wearing "+
			"the right type is what ADR-0024 item 4 forbids", winVec.GetValue(n-1))
	}
	if got := sqlerr.StateOf(err); got != "22003" {
		t.Errorf("SQLSTATE = %q, want 22003 (PostgreSQL says `22003: bigint out of range`): %v", got, err)
	}
	if !strings.Contains(err.Error(), "w") {
		t.Errorf("the message does not name the output column: %v", err)
	}
}

// TestWindowIntegerSumOverflowsTheCarrierLoudly is the NUMERIC arm's own
// ceiling: `SUM(int8) OVER ()` accumulates in Int128, and a total past 38
// digits is 22003 rather than a wrapped Int128 — the same refusal the GROUPED
// spelling makes over the same rows.
func TestWindowIntegerSumOverflowsTheCarrierLoudly(t *testing.T) {
	// 2^63-1 summed 2^65 times would be needed to leave Int128, which no
	// fixture can hold; the reachable ceiling is the AVG division's, which
	// scales the sum by 10^4 first. A sum near 10^35 therefore has no exact
	// quotient at scale 4.
	schema := []parquet.Column{
		{Name: "ts", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeDecimal, Precision: 38, Scale: 0, Nullable: true},
	}
	big := strings.Repeat("9", 37)
	rows := []map[string]any{
		{"ts": int64(0), "v": big},
		{"ts": int64(1), "v": big},
	}
	cols := []WindowColumn{{
		Func: WinAvg, InputCol: "v", OutputCol: "w", OutputType: parquet.TypeDecimal,
	}}
	ctx := context.Background()
	w := NewWindow(cols)
	if err := w.Init(ctx); err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.Consume(ctx, batch.FromRows(schema, rows)); err != nil {
		t.Fatal(err)
	}
	err := w.Finalize(ctx)
	if err == nil {
		_, err = w.Next(ctx)
	}
	if err == nil {
		t.Fatal("a quotient with no exact 128-bit value came back as a number")
	}
	if got := sqlerr.StateOf(err); got != "22003" {
		t.Errorf("SQLSTATE = %q, want 22003: %v", got, err)
	}
}

// TestSpilledWindowIntegerSumIsExact runs the same question through the two
// SPILLED evaluators — the empty-PARTITION-BY two-pass streamer
// (window_global.go) and the partition-at-a-time external walker
// (window_external.go) — and asserts the exact total, not merely that the
// spilled arm agrees with the in-memory one.
//
// Both halves matter. A spill is a CONDITION, not a query shape (ADR-0027),
// and the streaming evaluator is a SECOND implementation of the accumulator:
// it collects the whole-partition total in pass 1 and carries the running one
// through backfillPeerFrame, neither of which windowExactFrames touches. An
// arm-versus-arm comparison alone would have passed with both float64.
func TestSpilledWindowIntegerSumIsExact(t *testing.T) {
	forceTinyRuns(t)
	ctx := context.Background()
	// 200 rows, each 2^53 + a small distinct amount: every running total is
	// above 2^53 where a float64's ulp is 2, so a float accumulator cannot
	// answer any of these exactly.
	const base = int64(9007199254740992)
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "g", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64, Nullable: true},
	}
	var rows []map[string]any
	var total batch.Int128
	for i := 0; i < 200; i++ {
		v := base + int64(i)
		rows = append(rows, map[string]any{"id": int64(i), "g": int64(i % 4), "v": v})
		total = total.Add(batch.Int128From(v))
	}
	// 200*2^53 + (0+…+199); a float64 renders this 1801439850948218400.
	if got := total.String(); got != "1801439850948218300" {
		t.Fatalf("fixture arithmetic drifted: %s", got)
	}

	for _, tc := range []struct {
		name string
		cols []WindowColumn
	}{
		{name: "empty PARTITION BY — the two-pass streamer", cols: []WindowColumn{{
			Func: WinSum, InputCol: "v", OutputCol: "w", OutputType: parquet.TypeFloat64}}},
		{name: "PARTITION BY — the external walker", cols: []WindowColumn{{
			Func: WinSum, InputCol: "v", OutputCol: "w", OutputType: parquet.TypeFloat64,
			PartitionBy: []string{"g"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			partitioned := len(tc.cols[0].PartitionBy) > 0
			group := func(r map[string]any) int64 {
				if !partitioned {
					return -1
				}
				return r["g"].(int64)
			}
			want := map[int64]batch.Int128{}
			for _, r := range rows {
				g := group(r)
				want[g] = want[g].Add(batch.Int128From(r["v"].(int64)))
			}

			w := newWindowSpillHarness(t, tc.cols, 512)
			for i := 0; i < len(rows); i += 16 {
				end := min(i+16, len(rows))
				if err := w.Consume(ctx, batch.FromRows(schema, rows[i:end])); err != nil {
					t.Fatal(err)
				}
			}
			if len(w.runFiles) == 0 {
				t.Fatal("the window never spilled; this gate would compare two in-memory runs")
			}
			if err := w.Finalize(ctx); err != nil {
				t.Fatal(err)
			}
			out := drainWindowRows(t, w)
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			if len(out) != len(rows) {
				t.Fatalf("got %d rows, want %d", len(out), len(rows))
			}
			for _, r := range out {
				if got := fmt.Sprint(r["w"]); got != want[group(r)].String() {
					t.Fatalf("id=%v: w = %s, want %s — the spilled accumulator is not exact",
						r["id"], got, want[group(r)].String())
				}
			}
		})
	}
}
