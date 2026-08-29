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

// #586 / #475: a windowed SUM/AVG over a DECIMAL answers what the GROUPED
// SUM/AVG answer — an exact DECIMAL(38,s) / DECIMAL(38,min(s+4,38)), not a
// float64.
//
// The fixture is #475's: DECIMAL(38,10), 8 rows, k = i%2, unscaled
// (i+1)*12345678901. Every expected value below is written out in full
// because that is the whole assertion: the float path answered
// 19.753086241600002 for a partition whose exact total is 19.7530862416, and
// only a digit-for-digit comparison can tell those apart.

const winDecScale = 10

func winDecSchema() []parquet.Column {
	return []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "ts", Type: parquet.TypeInt64},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 38, Scale: winDecScale, Nullable: true},
	}
}

// winDecText renders an unscaled Int128 the way the DECIMAL column does, so a
// fixture value and the value read back out of a result row are one spelling.
func winDecText(unscaled int64, scale int) string {
	return batch.Int128From(unscaled).FormatDecimal(scale)
}

func winDecRows() []map[string]any {
	rows := make([]map[string]any, 8)
	for i := range rows {
		rows[i] = map[string]any{
			"k":  int64(i % 2),
			"ts": int64(i),
			"d":  winDecText(int64(i+1)*12345678901, winDecScale),
		}
	}
	return rows
}

// runWindowInMemory feeds rows through a non-spilling Window and returns the
// output rows in emission order plus the schema it declared.
func runWindowInMemory(tb testing.TB, schema []parquet.Column, cols []WindowColumn,
	rows []map[string]any) ([]map[string]any, []parquet.Column) {
	tb.Helper()
	ctx := context.Background()
	w := NewWindow(cols)
	if err := w.Init(ctx); err != nil {
		tb.Fatal(err)
	}
	if err := w.Consume(ctx, batch.FromRows(schema, rows)); err != nil {
		tb.Fatal(err)
	}
	if err := w.Finalize(ctx); err != nil {
		tb.Fatalf("Finalize: %v", err)
	}
	var out []map[string]any
	var outSchema []parquet.Column
	for {
		b, err := w.Next(ctx)
		if err != nil {
			tb.Fatalf("Next: %v", err)
		}
		if b == nil {
			break
		}
		outSchema = b.Schema
		out = append(out, b.ToRows()...)
	}
	return out, outSchema
}

// byTS keys the output rows by their ts column, which is unique in every
// fixture here, so an assertion names a row rather than a position.
func byTS(tb testing.TB, rows []map[string]any) map[int64]map[string]any {
	tb.Helper()
	out := make(map[int64]map[string]any, len(rows))
	for _, r := range rows {
		ts, ok := r["ts"].(int64)
		if !ok {
			tb.Fatalf("row has no int64 ts: %v", r)
		}
		out[ts] = r
	}
	return out
}

func winDecFrame(mode string, start, end WindowBound) *WindowFrameSpec {
	return &WindowFrameSpec{Mode: mode, Start: start, End: end}
}

// TestWindowDecimalSumAvgAreExactAcrossFrames is the frame x function matrix
// on the in-memory path. It fails before the fix on every row: the operator
// wrote float64 into a float64 output vector, so both the TYPE and the digits
// past ~16 significant ones were wrong.
func TestWindowDecimalSumAvgAreExactAcrossFrames(t *testing.T) {
	// want[ts] is the exact rendering the frame owes that row.
	cases := []struct {
		name  string
		fn    WindowFunc
		frame *WindowFrameSpec
		order bool
		want  map[int64]string
	}{
		{
			// No ORDER BY: the default frame widens to the whole partition,
			// so every row of a partition sees its total.
			name: "sum/whole-partition", fn: WinSum,
			want: map[int64]string{
				0: "19.7530862416", 2: "19.7530862416", 4: "19.7530862416", 6: "19.7530862416",
				1: "24.6913578020", 3: "24.6913578020", 5: "24.6913578020", 7: "24.6913578020",
			},
		},
		{
			name: "avg/whole-partition", fn: WinAvg,
			want: map[int64]string{
				0: "4.93827156040000", 2: "4.93827156040000", 4: "4.93827156040000", 6: "4.93827156040000",
				1: "6.17283945050000", 3: "6.17283945050000", 5: "6.17283945050000", 7: "6.17283945050000",
			},
		},
		{
			// The DEFAULT frame with an ORDER BY: RANGE UNBOUNDED PRECEDING
			// AND CURRENT ROW, i.e. the running total through the row's
			// ORDER-BY peer group. ts is unique, so each row is its own peer.
			name: "sum/default-frame-running", fn: WinSum, order: true,
			want: map[int64]string{
				0: "1.2345678901", 2: "4.9382715604", 4: "11.1111110109", 6: "19.7530862416",
				1: "2.4691357802", 3: "7.4074073406", 5: "14.8148146812", 7: "24.6913578020",
			},
		},
		{
			name: "avg/default-frame-running", fn: WinAvg, order: true,
			want: map[int64]string{
				0: "1.23456789010000", 2: "2.46913578020000", 4: "3.70370367030000", 6: "4.93827156040000",
				1: "2.46913578020000", 3: "3.70370367030000", 5: "4.93827156040000", 7: "6.17283945050000",
			},
		},
		{
			// An EXPLICIT RANGE frame takes the same peer-group path by a
			// different route through resolveFrame.
			name: "sum/explicit-range", fn: WinSum, order: true,
			frame: winDecFrame("range", WindowBound{Type: "unbounded_preceding"}, WindowBound{Type: "current_row"}),
			want: map[int64]string{
				0: "1.2345678901", 2: "4.9382715604", 4: "11.1111110109", 6: "19.7530862416",
				1: "2.4691357802", 3: "7.4074073406", 5: "14.8148146812", 7: "24.6913578020",
			},
		},
		{
			// The SLIDING frame: the lower end moves, so every row past the
			// first RETRACTS one. That subtraction is what has to be exact —
			// a float running total also loses associativity there.
			name: "sum/rows-1-preceding", fn: WinSum, order: true,
			frame: winDecFrame("rows", WindowBound{Type: "preceding", Offset: 1}, WindowBound{Type: "current_row"}),
			want: map[int64]string{
				0: "1.2345678901", 2: "4.9382715604", 4: "9.8765431208", 6: "14.8148146812",
				1: "2.4691357802", 3: "7.4074073406", 5: "12.3456789010", 7: "17.2839504614",
			},
		},
		{
			name: "avg/rows-1-preceding", fn: WinAvg, order: true,
			frame: winDecFrame("rows", WindowBound{Type: "preceding", Offset: 1}, WindowBound{Type: "current_row"}),
			want: map[int64]string{
				0: "1.23456789010000", 2: "2.46913578020000", 4: "4.93827156040000", 6: "7.40740734060000",
				1: "2.46913578020000", 3: "3.70370367030000", 5: "6.17283945050000", 7: "8.64197523070000",
			},
		},
		{
			// UNBOUNDED on both ends: the whole partition, reached with an
			// ORDER BY in force.
			name: "sum/rows-unbounded", fn: WinSum, order: true,
			frame: winDecFrame("rows", WindowBound{Type: "unbounded_preceding"}, WindowBound{Type: "unbounded_following"}),
			want: map[int64]string{
				0: "19.7530862416", 2: "19.7530862416", 4: "19.7530862416", 6: "19.7530862416",
				1: "24.6913578020", 3: "24.6913578020", 5: "24.6913578020", 7: "24.6913578020",
			},
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			wc := WindowColumn{
				Func: tc.fn, InputCol: "d", OutputCol: "w",
				// The FLOAT64 the planner falls back to for an argument it
				// could not type. retypeValueColumns has to correct it, the
				// way it corrects MIN/MAX (#569).
				OutputType:  parquet.TypeFloat64,
				PartitionBy: []string{"k"},
				Frame:       tc.frame,
			}
			if tc.order {
				wc.OrderBy = []SortKey{{Column: "ts", Order: Ascending}}
			}
			rows, outSchema := runWindowInMemory(t, winDecSchema(), []WindowColumn{wc}, winDecRows())

			var out parquet.Column
			for _, c := range outSchema {
				if c.Name == "w" {
					out = c
				}
			}
			if out.Type != parquet.TypeDecimal {
				t.Fatalf("output column type = %v, want DECIMAL", out.Type)
			}
			wantScale := winDecScale
			if tc.fn == WinAvg {
				wantScale = batch.AvgScale(winDecScale)
			}
			if out.Precision != batch.MaxDecimalPrecision || out.Scale != wantScale {
				t.Errorf("output column = DECIMAL(%d,%d), want DECIMAL(%d,%d)",
					out.Precision, out.Scale, batch.MaxDecimalPrecision, wantScale)
			}

			got := byTS(t, rows)
			for ts, want := range tc.want {
				if got[ts]["w"] != want {
					t.Errorf("ts=%d: w = %v (%T), want %q", ts, got[ts]["w"], got[ts]["w"], want)
				}
			}
		})
	}
}

// TestWindowDecimalSumAvgSpillMatchesInMemory drives the same specs through
// the EXTERNAL partition-at-a-time path (sorted columnar runs on disk), which
// is where the exact accumulator has to survive the run format's DECIMAL
// scale header. runWindowBothPaths fails if the spill never happened.
func TestWindowDecimalSumAvgSpillMatchesInMemory(t *testing.T) {
	rows := make([]map[string]any, 0, 240)
	for i := 0; i < 240; i++ {
		r := map[string]any{"k": int64(i % 5), "ts": int64(i)}
		if i%9 != 8 {
			r["d"] = winDecText(int64(i+1)*12345678901, winDecScale)
		} else {
			r["d"] = nil // a NULL in every partition, excluded from sum AND count
		}
		rows = append(rows, r)
	}
	cols := []WindowColumn{
		{Func: WinSum, InputCol: "d", OutputCol: "w_sum", OutputType: parquet.TypeFloat64,
			PartitionBy: []string{"k"}, OrderBy: []SortKey{{Column: "ts", Order: Ascending}}},
		{Func: WinAvg, InputCol: "d", OutputCol: "w_avg", OutputType: parquet.TypeFloat64,
			PartitionBy: []string{"k"}, OrderBy: []SortKey{{Column: "ts", Order: Ascending}}},
		{Func: WinSum, InputCol: "d", OutputCol: "w_slide", OutputType: parquet.TypeFloat64,
			PartitionBy: []string{"k"}, OrderBy: []SortKey{{Column: "ts", Order: Ascending}},
			Frame: winDecFrame("rows", WindowBound{Type: "preceding", Offset: 3}, WindowBound{Type: "current_row"})},
		{Func: WinAvg, InputCol: "d", OutputCol: "w_follow", OutputType: parquet.TypeFloat64,
			PartitionBy: []string{"k"}, OrderBy: []SortKey{{Column: "ts", Order: Ascending}},
			Frame: winDecFrame("rows", WindowBound{Type: "current_row"}, WindowBound{Type: "following", Offset: 2})},
	}
	runWindowBothPaths(t, winDecSchema(), cols, rows, 16,
		[]string{"k", "ts", "d", "w_sum", "w_avg", "w_slide", "w_follow"})
}

// TestWindowGlobalDecimalSumAvgMatchesInMemory is the empty-PARTITION-BY half.
// Past the budget that group is answered by the streaming two-pass evaluator
// (window_global.go), whose running total is carried across batches rather
// than read out of one partition's vector — a second accumulator that owes
// the same answer.
func TestWindowGlobalDecimalSumAvgMatchesInMemory(t *testing.T) {
	rows := make([]map[string]any, 0, 240)
	for i := 0; i < 240; i++ {
		r := map[string]any{"k": int64(i % 5), "ts": int64(i)}
		if i%9 != 8 {
			r["d"] = winDecText(int64(i+1)*12345678901, winDecScale)
		} else {
			r["d"] = nil
		}
		rows = append(rows, r)
	}
	// No ORDER BY: the pass-1 scalar form (collectGlobalWindowStats).
	runWindowBothPaths(t, winDecSchema(), []WindowColumn{
		{Func: WinSum, InputCol: "d", OutputCol: "g_sum", OutputType: parquet.TypeFloat64},
		{Func: WinAvg, InputCol: "d", OutputCol: "g_avg", OutputType: parquet.TypeFloat64},
	}, rows, 16, []string{"k", "ts", "d", "g_sum", "g_avg"})

	// With an ORDER BY: the running form, backfilled per closed peer group.
	runWindowBothPaths(t, winDecSchema(), []WindowColumn{
		{Func: WinSum, InputCol: "d", OutputCol: "r_sum", OutputType: parquet.TypeFloat64,
			OrderBy: []SortKey{{Column: "ts", Order: Ascending}}},
		{Func: WinAvg, InputCol: "d", OutputCol: "r_avg", OutputType: parquet.TypeFloat64,
			OrderBy: []SortKey{{Column: "ts", Order: Ascending}}},
	}, rows, 16, []string{"k", "ts", "d", "r_sum", "r_avg"})
}

// TestWindowDecimalNullsLeaveSumAndCount pins the NULL rule: a NULL is not
// part of an aggregate's input, so it contributes to neither the sum nor
// AVG's denominator, and a frame holding ONLY NULLs answers NULL rather than
// 0. AVG divided by the frame's WIDTH before this, so one NULL in a frame
// moved every average in it.
func TestWindowDecimalNullsLeaveSumAndCount(t *testing.T) {
	rows := []map[string]any{
		{"k": int64(0), "ts": int64(0), "d": "1.0000000000"},
		{"k": int64(0), "ts": int64(1), "d": nil},
		{"k": int64(0), "ts": int64(2), "d": "3.0000000000"},
		// A partition whose every row is NULL.
		{"k": int64(1), "ts": int64(3), "d": nil},
		{"k": int64(1), "ts": int64(4), "d": nil},
	}
	cols := []WindowColumn{
		{Func: WinSum, InputCol: "d", OutputCol: "w_sum", OutputType: parquet.TypeFloat64,
			PartitionBy: []string{"k"}},
		{Func: WinAvg, InputCol: "d", OutputCol: "w_avg", OutputType: parquet.TypeFloat64,
			PartitionBy: []string{"k"}},
	}
	got := byTS(t, mustRows(t, winDecSchema(), cols, rows))
	for _, ts := range []int64{0, 1, 2} {
		if got[ts]["w_sum"] != "4.0000000000" {
			t.Errorf("ts=%d: sum = %v, want 4.0000000000", ts, got[ts]["w_sum"])
		}
		// 4 / 2, not 4 / 3: the NULL row is not an input.
		if got[ts]["w_avg"] != "2.00000000000000" {
			t.Errorf("ts=%d: avg = %v, want 2.00000000000000", ts, got[ts]["w_avg"])
		}
	}
	for _, ts := range []int64{3, 4} {
		if got[ts]["w_sum"] != nil {
			t.Errorf("ts=%d: sum over an all-NULL partition = %v, want NULL", ts, got[ts]["w_sum"])
		}
		if got[ts]["w_avg"] != nil {
			t.Errorf("ts=%d: avg over an all-NULL partition = %v, want NULL", ts, got[ts]["w_avg"])
		}
	}
}

// TestWindowDecimalEmptyFrameIsNull: `ROWS BETWEEN 2 PRECEDING AND 1
// PRECEDING` gives the first row of a partition an EMPTY frame, whose SUM and
// AVG are NULL — not 0, which is what a sum with nothing in it looks like.
func TestWindowDecimalEmptyFrameIsNull(t *testing.T) {
	cols := []WindowColumn{
		{Func: WinSum, InputCol: "d", OutputCol: "w_sum", OutputType: parquet.TypeFloat64,
			PartitionBy: []string{"k"}, OrderBy: []SortKey{{Column: "ts", Order: Ascending}},
			Frame: winDecFrame("rows", WindowBound{Type: "preceding", Offset: 2}, WindowBound{Type: "preceding", Offset: 1})},
		{Func: WinAvg, InputCol: "d", OutputCol: "w_avg", OutputType: parquet.TypeFloat64,
			PartitionBy: []string{"k"}, OrderBy: []SortKey{{Column: "ts", Order: Ascending}},
			Frame: winDecFrame("rows", WindowBound{Type: "preceding", Offset: 2}, WindowBound{Type: "preceding", Offset: 1})},
	}
	got := byTS(t, mustRows(t, winDecSchema(), cols, winDecRows()))
	for _, ts := range []int64{0, 1} { // first row of each partition
		if got[ts]["w_sum"] != nil || got[ts]["w_avg"] != nil {
			t.Errorf("ts=%d: empty frame gave sum=%v avg=%v, want NULL/NULL",
				ts, got[ts]["w_sum"], got[ts]["w_avg"])
		}
	}
	// The second row of partition k=0 sees exactly row ts=0.
	if got[2]["w_sum"] != "1.2345678901" {
		t.Errorf("ts=2: sum = %v, want 1.2345678901", got[2]["w_sum"])
	}
}

// TestWindowDecimalSumOverflowIs22003: two values near 10^38 overflow the
// exact carrier, and a wrapped total is a different number wearing the right
// type. The query fails with PostgreSQL's numeric_value_out_of_range rather
// than answering it (ADR-0012 item 9, ADR-0024 item 4). Before the fix the
// same input answered a float64 approximation.
func TestWindowDecimalSumOverflowIs22003(t *testing.T) {
	big := strings.Repeat("9", 38) // 10^38 - 1, the widest DECIMAL(38,0)
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "ts", Type: parquet.TypeInt64},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 38, Scale: 0, Nullable: true},
	}
	rows := []map[string]any{
		{"k": int64(0), "ts": int64(0), "d": big},
		{"k": int64(0), "ts": int64(1), "d": big},
	}
	for _, fn := range []struct {
		name string
		f    WindowFunc
	}{{"sum", WinSum}, {"avg", WinAvg}} {
		t.Run(fn.name, func(t *testing.T) {
			ctx := context.Background()
			w := NewWindow([]WindowColumn{{Func: fn.f, InputCol: "d", OutputCol: "w",
				OutputType: parquet.TypeFloat64, PartitionBy: []string{"k"}}})
			if err := w.Init(ctx); err != nil {
				t.Fatal(err)
			}
			if err := w.Consume(ctx, batch.FromRows(schema, rows)); err != nil {
				t.Fatal(err)
			}
			err := w.Finalize(ctx)
			if err == nil {
				t.Fatal("overflowing window SUM answered instead of failing")
			}
			if code := sqlerr.StateOf(err); code != "22003" {
				t.Errorf("SQLSTATE = %q, want 22003 (%v)", code, err)
			}
		})
	}
}

// TestWindowDecimalOverflowFailsOnEveryPath: the same overflow has to fail on
// the SPILLED paths too. A partition-at-a-time walker or a streaming
// evaluator that answered a wrapped total where the in-memory path failed
// would be the two-path defect class, one operator over.
func TestWindowDecimalOverflowFailsOnEveryPath(t *testing.T) {
	forceTinyRuns(t)
	big := strings.Repeat("9", 38)
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "ts", Type: parquet.TypeInt64},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 38, Scale: 0, Nullable: true},
	}
	rows := make([]map[string]any, 240)
	for i := range rows {
		rows[i] = map[string]any{"k": int64(i % 5), "ts": int64(i), "d": big}
	}
	for _, tc := range []struct {
		name string
		cols []WindowColumn
	}{
		{"partition-walker", []WindowColumn{{Func: WinSum, InputCol: "d", OutputCol: "w",
			OutputType: parquet.TypeFloat64, PartitionBy: []string{"k"},
			OrderBy: []SortKey{{Column: "ts", Order: Ascending}}}}},
		{"global-streamer", []WindowColumn{{Func: WinSum, InputCol: "d", OutputCol: "w",
			OutputType: parquet.TypeFloat64, OrderBy: []SortKey{{Column: "ts", Order: Ascending}}}}},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			w := newWindowSpillHarness(t, tc.cols, 512)
			defer w.Close()
			for i := 0; i < len(rows); i += 16 {
				end := min(i+16, len(rows))
				if err := w.Consume(ctx, batch.FromRows(schema, rows[i:end])); err != nil {
					t.Fatal(err)
				}
			}
			if len(w.runFiles) == 0 {
				t.Fatal("spill path was never exercised")
			}
			err := w.Finalize(ctx)
			if err == nil {
				// The final group streams through Next(), so a walker or
				// streamer error surfaces there rather than at Finalize.
				for err == nil {
					var b *batch.RecordBatch
					b, err = w.Next(ctx)
					if b == nil && err == nil {
						break
					}
				}
			}
			if err == nil {
				t.Fatal("overflowing window SUM answered instead of failing")
			}
			if code := sqlerr.StateOf(err); code != "22003" {
				t.Errorf("SQLSTATE = %q, want 22003 (%v)", code, err)
			}
		})
	}
}

// TestWindowOutputColumnDeclaresAccumulatorScale is the declaration rule on
// its own: SUM keeps the input's SCALE at the carrier's full precision, AVG
// adds four digits, and MIN/MAX (which copy a value rather than accumulate
// one) keep the input's (p,s) untouched.
func TestWindowOutputColumnDeclaresAccumulatorScale(t *testing.T) {
	for _, in := range []parquet.Column{
		{Name: "d", Type: parquet.TypeDecimal, Precision: 9, Scale: 2},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 18, Scale: 4},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 38, Scale: 10},
	} {
		schema := []parquet.Column{in}
		t.Run(fmt.Sprintf("dec_%d_%d", in.Precision, in.Scale), func(t *testing.T) {
			sum := windowOutputColumn(WindowColumn{Func: WinSum, InputCol: "d",
				OutputCol: "w", OutputType: parquet.TypeDecimal}, schema)
			if sum.Precision != 38 || sum.Scale != in.Scale {
				t.Errorf("SUM = DECIMAL(%d,%d), want DECIMAL(38,%d)", sum.Precision, sum.Scale, in.Scale)
			}
			avg := windowOutputColumn(WindowColumn{Func: WinAvg, InputCol: "d",
				OutputCol: "w", OutputType: parquet.TypeDecimal}, schema)
			if avg.Precision != 38 || avg.Scale != batch.AvgScale(in.Scale) {
				t.Errorf("AVG = DECIMAL(%d,%d), want DECIMAL(38,%d)",
					avg.Precision, avg.Scale, batch.AvgScale(in.Scale))
			}
			mx := windowOutputColumn(WindowColumn{Func: WinMax, InputCol: "d",
				OutputCol: "w", OutputType: parquet.TypeDecimal}, schema)
			if mx.Precision != in.Precision || mx.Scale != in.Scale {
				t.Errorf("MAX = DECIMAL(%d,%d), want the input's DECIMAL(%d,%d)",
					mx.Precision, mx.Scale, in.Precision, in.Scale)
			}
		})
	}
}

// TestWindowSumAvgOverNonDecimalStaysFloat64 pins the OTHER direction: the
// exact accumulator is for DECIMAL only, and a float or integer column keeps
// the float64 answer it has always had. A declaration that says DECIMAL over
// such an input is corrected DOWN rather than writing float sums into an
// Int128 array.
func TestWindowSumAvgOverNonDecimalStaysFloat64(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeFloat64, Nullable: true},
	}
	rows := []map[string]any{
		{"k": int64(0), "v": 1.5},
		{"k": int64(0), "v": 2.25},
	}
	cols := []WindowColumn{
		// The wrong declaration on purpose.
		{Func: WinSum, InputCol: "v", OutputCol: "w", OutputType: parquet.TypeDecimal,
			PartitionBy: []string{"k"}},
	}
	out, outSchema := runWindowInMemory(t, schema, cols, rows)
	for _, c := range outSchema {
		if c.Name == "w" && c.Type != parquet.TypeFloat64 {
			t.Fatalf("output column type = %v, want FLOAT64", c.Type)
		}
	}
	for _, r := range out {
		if r["w"] != 3.75 {
			t.Errorf("w = %v, want 3.75", r["w"])
		}
	}
}

func mustRows(tb testing.TB, schema []parquet.Column, cols []WindowColumn,
	rows []map[string]any) []map[string]any {
	tb.Helper()
	out, _ := runWindowInMemory(tb, schema, cols, rows)
	return out
}
