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

// The slide's TRANSIENT is not the frame (F1).
//
// The accumulator carries state from one frame to the next, so the order it
// applies the two edges in decides what it holds in between. Adding the
// arriving row before subtracting the departing one makes it transiently hold
// sum(previous frame + arriving rows) — a value belonging to NEITHER frame —
// and on an exact carrier that value can leave the range while both frames sit
// comfortably inside it. Three DECIMAL(38,0) rows of 9x10^37 under
// `ROWS BETWEEN CURRENT ROW AND CURRENT ROW` are the smallest case: each frame
// holds 9x10^37, the transient held 1.8x10^38, and the query was refused with
// 22003 where PostgreSQL and the GROUPED spelling both answer.
//
// The discrimination this pins has three sides, and a fix that gets any one of
// them by giving up on the others is not a fix:
//
//	CURRENT ROW AND CURRENT ROW      -> 9x10^37 on every row
//	1 PRECEDING AND CURRENT ROW      -> 22003 (1.8x10^38 has no Int128, and
//	                                   `SUM(d) ... GROUP BY` over those same
//	                                   two rows refuses it too)
//	an EMPTY frame                   -> NULL, never 22003
//
// wdoNine37 is the widest value a DECIMAL(38,0) holds; two of them exceed the
// carrier's 1.70x10^38 ceiling and three exceed it further.
const wdoNine37 = "90000000000000000000000000000000000000"

func wdoSchema() []parquet.Column {
	return []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "ts", Type: parquet.TypeInt64},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 38, Scale: 0, Nullable: true},
	}
}

// wdoRows builds n rows of wdoNine37 spread over groups partitions.
func wdoRows(n, groups int) []map[string]any {
	rows := make([]map[string]any, n)
	for i := range rows {
		rows[i] = map[string]any{"k": int64(i % groups), "ts": int64(i), "d": wdoNine37}
	}
	return rows
}

// wdoCol builds one windowed SUM spec over d with the given frame.
func wdoCol(frame *WindowFrameSpec) []WindowColumn {
	return []WindowColumn{{
		Func: WinSum, InputCol: "d", OutputCol: "w", OutputType: parquet.TypeFloat64,
		PartitionBy: []string{"k"},
		OrderBy:     []SortKey{{Column: "ts", Order: Ascending}},
		Frame:       frame,
	}}
}

// wdoFrames is the three-sided discrimination, shared by the in-memory and the
// two spilled arms so none of them can drift into testing a different shape.
type wdoCase struct {
	name  string
	frame *WindowFrameSpec
	// want is the value every row owes; "" means SQL NULL. refuse overrides
	// it: the query must fail with 22003.
	want   string
	refuse bool
}

func wdoCases() []wdoCase {
	return []wdoCase{
		{name: "current_row_only", want: wdoNine37, frame: winDecFrame("rows",
			WindowBound{Type: "current_row"}, WindowBound{Type: "current_row"})},
		// Genuinely past the carrier: two rows of 9x10^37 sum to 1.8x10^38,
		// which no Int128 holds. The GROUPED spelling over the same two rows
		// refuses it as well, which is the property being kept.
		{name: "one_preceding_refuses", refuse: true, frame: winDecFrame("rows",
			WindowBound{Type: "preceding", Offset: 1}, WindowBound{Type: "current_row"})},
		// An always-empty frame. The old accumulator reached it by adding
		// every row of the partition and subtracting them again, so the
		// partition TOTAL became a transient and a frame that holds nothing
		// could raise 22003.
		{name: "empty_frame_is_null", want: "", frame: winDecFrame("rows",
			WindowBound{Type: "unbounded_following"}, WindowBound{Type: "unbounded_preceding"})},
	}
}

func TestWindowDecimalSlideTransientIsNotTheFrame(t *testing.T) {
	for _, tc := range wdoCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			rows := wdoRows(6, 2) // three rows per partition
			w := NewWindow(wdoCol(tc.frame))
			if err := w.Init(ctx); err != nil {
				t.Fatal(err)
			}
			if err := w.Consume(ctx, batch.FromRows(wdoSchema(), rows)); err != nil {
				t.Fatal(err)
			}
			err := w.Finalize(ctx)
			if tc.refuse {
				if err == nil {
					t.Fatal("a frame whose own rows overflow the carrier answered instead of failing")
				}
				if code := sqlerr.StateOf(err); code != "22003" {
					t.Errorf("SQLSTATE = %q, want 22003 (%v)", code, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("a frame every row of which is representable was refused: %v", err)
			}
			var out []map[string]any
			for {
				b, nerr := w.Next(ctx)
				if nerr != nil {
					t.Fatalf("Next: %v", nerr)
				}
				if b == nil {
					break
				}
				out = append(out, b.ToRows()...)
			}
			if len(out) != len(rows) {
				t.Fatalf("got %d rows, want %d", len(out), len(rows))
			}
			for _, r := range out {
				if tc.want == "" {
					if r["w"] != nil {
						t.Errorf("ts=%v: w = %v, want NULL", r["ts"], r["w"])
					}
					continue
				}
				if r["w"] != tc.want {
					t.Errorf("ts=%v: w = %v, want %q", r["ts"], r["w"], tc.want)
				}
			}
		})
	}
}

// TestWindowDecimalSlideTransientOnTheSpilledPaths is the same three-sided
// discrimination through the two evaluators that do not hold the partition in
// memory: the partition-at-a-time walker over sorted runs, and — for an empty
// PARTITION BY — the streaming two-pass evaluator. A transient that refused a
// representable frame on one path and not another would be the two-path defect
// class on top of F1.
func TestWindowDecimalSlideTransientOnTheSpilledPaths(t *testing.T) {
	for _, tc := range wdoCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			forceTinyRuns(t)
			ctx := context.Background()
			rows := wdoRows(240, 80) // three rows per partition, 80 partitions
			w := newWindowSpillHarness(t, wdoCol(tc.frame), 512)
			defer w.Close()
			for i := 0; i < len(rows); i += 16 {
				if err := w.Consume(ctx, batch.FromRows(wdoSchema(), rows[i:min(i+16, len(rows))])); err != nil {
					t.Fatal(err)
				}
			}
			if len(w.runFiles) == 0 {
				t.Fatal("spill path was never exercised")
			}
			// The final spec group streams through Next(), so a walker error
			// surfaces there rather than at Finalize.
			err := w.Finalize(ctx)
			var out []map[string]any
			for err == nil {
				b, nerr := w.Next(ctx)
				if nerr != nil {
					err = nerr
					break
				}
				if b == nil {
					break
				}
				out = append(out, b.ToRows()...)
			}
			if tc.refuse {
				if err == nil {
					t.Fatal("a frame whose own rows overflow the carrier answered instead of failing")
				}
				if code := sqlerr.StateOf(err); code != "22003" {
					t.Errorf("SQLSTATE = %q, want 22003 (%v)", code, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("a frame every row of which is representable was refused: %v", err)
			}
			if len(out) != len(rows) {
				t.Fatalf("got %d rows, want %d", len(out), len(rows))
			}
			for _, r := range out {
				if tc.want == "" {
					if r["w"] != nil {
						t.Errorf("ts=%v: w = %v, want NULL", r["ts"], r["w"])
					}
					continue
				}
				if r["w"] != tc.want {
					t.Errorf("ts=%v: w = %v, want %q", r["ts"], r["w"], tc.want)
				}
			}
		})
	}
}

// TestWindowDecimalRunningTotalStillRefusesWhatItCannotCarry is the other side
// of the recompute: a frame whose own rows leave the range and come BACK is
// still refused, because summing them in order is what the grouped SUM does
// and item 9 says that refusal stands. The recompute exists to drop transients
// that belong to no frame — not to widen the carrier.
func TestWindowDecimalRunningTotalStillRefusesWhatItCannotCarry(t *testing.T) {
	ctx := context.Background()
	neg := "-" + wdoNine37
	rows := []map[string]any{
		{"k": int64(0), "ts": int64(0), "d": wdoNine37},
		{"k": int64(0), "ts": int64(1), "d": wdoNine37},
		{"k": int64(0), "ts": int64(2), "d": neg},
	}
	// The whole partition: the exact total is 9x10^37 and representable, but
	// the running total passes through 1.8x10^38 on the way.
	w := NewWindow([]WindowColumn{{Func: WinSum, InputCol: "d", OutputCol: "w",
		OutputType: parquet.TypeFloat64, PartitionBy: []string{"k"}}})
	if err := w.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := w.Consume(ctx, batch.FromRows(wdoSchema(), rows)); err != nil {
		t.Fatal(err)
	}
	err := w.Finalize(ctx)
	if err == nil {
		t.Fatal("a running total that left the carrier answered; ADR-0012 item 9 refuses it")
	}
	if code := sqlerr.StateOf(err); code != "22003" {
		t.Errorf("SQLSTATE = %q, want 22003 (%v)", code, err)
	}
}

// TestWindowFloat64SlideTransientIsNotTheFrame is F1 on the INEXACT carrier,
// where the same add-before-retract order is catastrophic cancellation rather
// than an overflow. `ROWS BETWEEN CURRENT ROW AND CURRENT ROW` over 1e300
// followed by 1.0 computed row 1 as (1e300 + 1.0) - 1e300 — the 1.0 is below
// the sum's last bit, so it vanished and the row answered 0.
func TestWindowFloat64SlideTransientIsNotTheFrame(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "ts", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeFloat64, Nullable: true},
	}
	rows := []map[string]any{
		{"k": int64(0), "ts": int64(0), "v": 1e300},
		{"k": int64(0), "ts": int64(1), "v": 1.0},
		{"k": int64(0), "ts": int64(2), "v": 2.0},
	}
	cols := []WindowColumn{{
		Func: WinSum, InputCol: "v", OutputCol: "w", OutputType: parquet.TypeFloat64,
		PartitionBy: []string{"k"},
		OrderBy:     []SortKey{{Column: "ts", Order: Ascending}},
		Frame: winDecFrame("rows",
			WindowBound{Type: "current_row"}, WindowBound{Type: "current_row"}),
	}}
	got := byTS(t, mustRows(t, schema, cols, rows))
	for ts, want := range map[int64]float64{0: 1e300, 1: 1.0, 2: 2.0} {
		if got[ts]["w"] != want {
			t.Errorf("ts=%d: w = %v, want %v — a frame of one row is that row",
				ts, got[ts]["w"], want)
		}
	}
}

// TestWindowSumAvgReadEveryNumericTypeWithoutAPerRowSwitch pins the reader
// hoist across the whole promotion table, not only FLOAT64.
//
// resolveWindowNumeric replaced vecFloat64's two per-row type switches with
// one resolution per partition, and its switch and numericPromotable's are the
// same list stated twice — a type in one and not the other answers 0 for every
// row rather than NULL, which is #412's exact symptom through a new door.
func TestWindowSumAvgReadEveryNumericTypeWithoutAPerRowSwitch(t *testing.T) {
	for _, tc := range []struct {
		typ  parquet.TypeID
		col  parquet.Column
		vals []any
		want float64
	}{
		{typ: parquet.TypeFloat64, col: parquet.Column{Name: "v", Type: parquet.TypeFloat64, Nullable: true},
			vals: []any{1.5, 2.25}, want: 3.75},
		{typ: parquet.TypeFloat32, col: parquet.Column{Name: "v", Type: parquet.TypeFloat32, Nullable: true},
			vals: []any{float32(1.5), float32(2.25)}, want: 3.75},
		{typ: parquet.TypeInt64, col: parquet.Column{Name: "v", Type: parquet.TypeInt64, Nullable: true},
			vals: []any{int64(7), int64(11)}, want: 18},
		{typ: parquet.TypeInt32, col: parquet.Column{Name: "v", Type: parquet.TypeInt32, Nullable: true},
			vals: []any{int32(7), int32(11)}, want: 18},
		{typ: parquet.TypePort, col: parquet.Column{Name: "v", Type: parquet.TypePort, Nullable: true},
			vals: []any{int32(80), int32(443)}, want: 523},
		{typ: parquet.TypeProtocol, col: parquet.Column{Name: "v", Type: parquet.TypeProtocol, Nullable: true},
			vals: []any{int32(6), int32(17)}, want: 23},
		{typ: parquet.TypeDuration, col: parquet.Column{Name: "v", Type: parquet.TypeDuration, Nullable: true},
			vals: []any{int64(1000), int64(2000)}, want: 3000},
		{typ: parquet.TypeDate, col: parquet.Column{Name: "v", Type: parquet.TypeDate, Nullable: true},
			vals: []any{int32(10), int32(20)}, want: 30},
		{typ: parquet.TypeTimestamp, col: parquet.Column{Name: "v", Type: parquet.TypeTimestamp, Nullable: true},
			vals: []any{int64(1000), int64(2000)}, want: 3000},
	} {
		tc := tc
		t.Run(tc.typ.String(), func(t *testing.T) {
			schema := []parquet.Column{{Name: "k", Type: parquet.TypeInt64}, tc.col}
			rows := []map[string]any{
				{"k": int64(0), "v": tc.vals[0]},
				{"k": int64(0), "v": tc.vals[1]},
				{"k": int64(0), "v": nil}, // excluded from the sum AND the count
			}
			cols := []WindowColumn{
				{Func: WinSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeFloat64,
					PartitionBy: []string{"k"}},
				{Func: WinAvg, InputCol: "v", OutputCol: "a", OutputType: parquet.TypeFloat64,
					PartitionBy: []string{"k"}},
			}
			for _, r := range mustRows(t, schema, cols, rows) {
				if r["s"] != tc.want {
					t.Errorf("SUM = %v, want %v", r["s"], tc.want)
				}
				if r["a"] != tc.want/2 {
					t.Errorf("AVG = %v, want %v (two non-NULL rows, not three)", r["a"], tc.want/2)
				}
			}
		})
	}
	// The other half of the same list: a column with NO numeric reading
	// answers NULL, never 0.
	for _, col := range []parquet.Column{
		{Name: "v", Type: parquet.TypeIPv4, Nullable: true},
		{Name: "v", Type: parquet.TypeMAC, Nullable: true},
		{Name: "v", Type: parquet.TypeString, Nullable: true},
	} {
		col := col
		t.Run("not_summable_"+col.Type.String(), func(t *testing.T) {
			v := any("1.2.3.4")
			switch col.Type {
			case parquet.TypeMAC:
				v = "aa:bb:cc:dd:ee:ff"
			case parquet.TypeString:
				v = "x"
			}
			schema := []parquet.Column{{Name: "k", Type: parquet.TypeInt64}, col}
			rows := []map[string]any{{"k": int64(0), "v": v}, {"k": int64(0), "v": v}}
			cols := []WindowColumn{{Func: WinSum, InputCol: "v", OutputCol: "s",
				OutputType: parquet.TypeFloat64, PartitionBy: []string{"k"}}}
			for _, r := range mustRows(t, schema, cols, rows) {
				if r["s"] != nil {
					t.Errorf("SUM over a %s = %v, want NULL", col.Type, r["s"])
				}
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
