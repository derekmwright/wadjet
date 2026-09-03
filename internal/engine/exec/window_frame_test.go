package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// --- #350: an explicit window frame is obeyed ---
//
// WindowColumn.Frame was parsed, carried through the logical plan, put on the
// exec spec and shipped on the wire, and then read by nothing: every value and
// aggregate function decided on the presence of an ORDER BY alone. LAST_VALUE
// over an explicit whole-partition frame returned the CURRENT row's value and
// SUM over one returned a running total — the default-frame answers, to a
// query that spelled out a different frame.
//
// Every expectation below is DuckDB's, verified against the SF0.01 fixture
// (benchmarks/tpch/duckdb_compare_test.go, WindowFrame*).

var frameTestSchema = []parquet.Column{
	{Name: "k", Type: parquet.TypeInt64},
	{Name: "s", Type: parquet.TypeString},
	{Name: "v", Type: parquet.TypeFloat64},
}

// frameTestRows: k is unique and ascending, so ROWS and RANGE agree and the
// frame is the only thing under test.
func frameTestRows() []map[string]any {
	return []map[string]any{
		{"k": int64(1), "s": "a", "v": 1.0},
		{"k": int64(2), "s": "b", "v": 2.0},
		{"k": int64(3), "s": "c", "v": 3.0},
		{"k": int64(4), "s": "d", "v": 4.0},
	}
}

func frameOrderBy() []SortKey {
	return []SortKey{{Column: "k", Order: Ascending, NullsLast: true}}
}

func rowsFrame(startType string, startOff int, endType string, endOff int) *WindowFrameSpec {
	return &WindowFrameSpec{
		Mode:  "rows",
		Start: WindowBound{Type: startType, Offset: startOff},
		End:   WindowBound{Type: endType, Offset: endOff},
	}
}

// runFrameWindow runs one WindowColumn over frameTestRows and returns the
// output column, in k order.
func runFrameWindow(t *testing.T, wc WindowColumn) []any {
	t.Helper()
	win := NewWindow([]WindowColumn{wc})
	pipe := &Pipeline{Source: NewSliceSource(frameTestSchema, frameTestRows()), Sink: win}
	ctx := context.Background()
	if err := pipe.Run(ctx); err != nil {
		t.Fatal(err)
	}
	var out []any
	for {
		b, err := win.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		for _, r := range b.ToRows() {
			out = append(out, r[wc.OutputCol])
		}
	}
	return out
}

func checkFrameOut(t *testing.T, name string, got, want []any) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: got %d rows %v, want %d rows %v", name, len(got), got, len(want), want)
		return
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("%s: row %d = %v, want %v (full: %v)", name, i, got[i], want[i], got)
			return
		}
	}
}

// TestWindowValueFunctionsHonorFrame is #350's regression test: the three
// positional value functions with an explicit frame, and with none.
func TestWindowValueFunctionsHonorFrame(t *testing.T) {
	cases := []struct {
		name  string
		fn    WindowFunc
		nth   int
		frame *WindowFrameSpec
		want  []any
	}{
		// The issue's query. Without the fix every row answered with its own
		// value ("a","b","c","d") — the default-frame answer.
		{"LAST_VALUE whole partition", WinLastValue, 0,
			rowsFrame("unbounded_preceding", 0, "unbounded_following", 0),
			[]any{"d", "d", "d", "d"}},
		// The default frame, which legitimately IS the current row: RANGE
		// BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW. Pinned so a fix that
		// "corrects" this case is caught.
		{"LAST_VALUE default frame", WinLastValue, 0, nil,
			[]any{"a", "b", "c", "d"}},
		{"LAST_VALUE current row and 1 following", WinLastValue, 0,
			rowsFrame("current_row", 0, "following", 1),
			[]any{"b", "c", "d", "d"}},
		// FIRST_VALUE agrees with the partition's first row under the
		// default frame for the same reason LAST_VALUE agrees with the
		// current one — so only a MOVING lower bound can tell them apart.
		{"FIRST_VALUE default frame", WinFirstValue, 0, nil,
			[]any{"a", "a", "a", "a"}},
		{"FIRST_VALUE 1 preceding", WinFirstValue, 0,
			rowsFrame("preceding", 1, "current_row", 0),
			[]any{"a", "a", "b", "c"}},
		{"FIRST_VALUE 2 preceding", WinFirstValue, 0,
			rowsFrame("preceding", 2, "current_row", 0),
			[]any{"a", "a", "a", "b"}},
		// NTH_VALUE used to evaluate over the whole partition always, which
		// agrees with an explicit whole-partition frame by accident and with
		// nothing else. Under the default frame row 0 has no second row.
		{"NTH_VALUE(2) default frame", WinNthValue, 2, nil,
			[]any{nil, "b", "b", "b"}},
		{"NTH_VALUE(2) whole partition", WinNthValue, 2,
			rowsFrame("unbounded_preceding", 0, "unbounded_following", 0),
			[]any{"b", "b", "b", "b"}},
		{"NTH_VALUE(2) 1 preceding", WinNthValue, 2,
			rowsFrame("preceding", 1, "current_row", 0),
			[]any{nil, "b", "c", "d"}},
		// A frame that is EMPTY for the leading rows: every function is NULL
		// there, not the nearest value it could have reached for.
		{"FIRST_VALUE empty leading frame", WinFirstValue, 0,
			rowsFrame("preceding", 3, "preceding", 2),
			[]any{nil, nil, "a", "a"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := runFrameWindow(t, WindowColumn{
				Func:       tc.fn,
				InputCol:   "s",
				OutputCol:  "out",
				OutputType: parquet.TypeString,
				OrderBy:    frameOrderBy(),
				NthValueN:  tc.nth,
				Frame:      tc.frame,
			})
			checkFrameOut(t, tc.name, got, tc.want)
		})
	}
}

// TestWindowAggregatesHonorFrame: a running total and a whole-partition total
// differ only by the frame, and answering one for the other is a plausible
// wrong number rather than an error.
func TestWindowAggregatesHonorFrame(t *testing.T) {
	cases := []struct {
		name   string
		fn     WindowFunc
		outTyp parquet.TypeID
		frame  *WindowFrameSpec
		want   []any
	}{
		{"SUM default frame is a running total", WinSum, parquet.TypeFloat64, nil,
			[]any{1.0, 3.0, 6.0, 10.0}},
		{"SUM whole partition", WinSum, parquet.TypeFloat64,
			rowsFrame("unbounded_preceding", 0, "unbounded_following", 0),
			[]any{10.0, 10.0, 10.0, 10.0}},
		{"SUM sliding 1 preceding to 1 following", WinSum, parquet.TypeFloat64,
			rowsFrame("preceding", 1, "following", 1),
			[]any{3.0, 6.0, 9.0, 7.0}},
		// An empty frame makes SUM and AVG NULL — but not COUNT, which
		// counts what it can see and seeing nothing is 0.
		{"SUM empty frame is NULL", WinSum, parquet.TypeFloat64,
			rowsFrame("preceding", 3, "preceding", 2),
			[]any{nil, nil, 1.0, 3.0}},
		{"COUNT default frame", WinCount, parquet.TypeInt64, nil,
			[]any{int64(1), int64(2), int64(3), int64(4)}},
		{"COUNT whole partition", WinCount, parquet.TypeInt64,
			rowsFrame("unbounded_preceding", 0, "unbounded_following", 0),
			[]any{int64(4), int64(4), int64(4), int64(4)}},
		{"COUNT empty frame is 0", WinCount, parquet.TypeInt64,
			rowsFrame("preceding", 3, "preceding", 2),
			[]any{int64(0), int64(0), int64(1), int64(2)}},
		{"AVG default frame", WinAvg, parquet.TypeFloat64, nil,
			[]any{1.0, 1.5, 2.0, 2.5}},
		{"AVG whole partition", WinAvg, parquet.TypeFloat64,
			rowsFrame("unbounded_preceding", 0, "unbounded_following", 0),
			[]any{2.5, 2.5, 2.5, 2.5}},
		{"AVG sliding", WinAvg, parquet.TypeFloat64,
			rowsFrame("preceding", 1, "following", 1),
			[]any{1.5, 2.0, 3.0, 3.5}},
		// MIN/MAX over a moving lower bound: the value that leaves the frame
		// must stop winning, which a running accumulator cannot express.
		{"MIN sliding 1 preceding", WinMin, parquet.TypeFloat64,
			rowsFrame("preceding", 1, "current_row", 0),
			[]any{1.0, 1.0, 2.0, 3.0}},
		{"MAX current row to 1 following", WinMax, parquet.TypeFloat64,
			rowsFrame("current_row", 0, "following", 1),
			[]any{2.0, 3.0, 4.0, 4.0}},
		{"MIN default frame", WinMin, parquet.TypeFloat64, nil,
			[]any{1.0, 1.0, 1.0, 1.0}},
		{"MAX default frame", WinMax, parquet.TypeFloat64, nil,
			[]any{1.0, 2.0, 3.0, 4.0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := "v"
			if tc.fn == WinCount {
				input = ""
			}
			got := runFrameWindow(t, WindowColumn{
				Func:       tc.fn,
				InputCol:   input,
				OutputCol:  "out",
				OutputType: tc.outTyp,
				OrderBy:    frameOrderBy(),
				Frame:      tc.frame,
			})
			checkFrameOut(t, tc.name, got, tc.want)
		})
	}
}

// TestWindowFrameRangePeerGroups pins RANGE mode's defining behaviour: its
// bounds move by ORDER-BY PEER GROUP, not by row. Every row here ties with
// its neighbour, so a RANGE frame and the row-counting ROWS frame of the same
// spelling give different answers — which is the whole reason SQL has two
// modes, and the reason the default frame (RANGE) is peer-aware.
func TestWindowFrameRangePeerGroups(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"k": int64(1), "v": 1.0},
		{"k": int64(1), "v": 2.0},
		{"k": int64(2), "v": 3.0},
		{"k": int64(2), "v": 4.0},
	}
	orderBy := []SortKey{{Column: "k", Order: Ascending, NullsLast: true}}

	cases := []struct {
		name  string
		frame *WindowFrameSpec
		want  []any
	}{
		// The default frame: through the end of the peer group, so both rows
		// of a tied pair see the same total. Row-at-a-time running sums
		// (1, 3, 6, 10) are the answer this shape used to give.
		{"default frame is peer-aware", nil, []any{3.0, 3.0, 10.0, 10.0}},
		{"RANGE CURRENT ROW to UNBOUNDED FOLLOWING", &WindowFrameSpec{
			Mode:  "range",
			Start: WindowBound{Type: "current_row"},
			End:   WindowBound{Type: "unbounded_following"},
		}, []any{10.0, 10.0, 7.0, 7.0}},
		// The same spelling in ROWS mode counts rows and ignores ties.
		{"ROWS CURRENT ROW to UNBOUNDED FOLLOWING",
			rowsFrame("current_row", 0, "unbounded_following", 0),
			[]any{10.0, 9.0, 7.0, 4.0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wc := WindowColumn{
				Func: WinSum, InputCol: "v", OutputCol: "out",
				OutputType: parquet.TypeFloat64, OrderBy: orderBy, Frame: tc.frame,
			}
			win := NewWindow([]WindowColumn{wc})
			pipe := &Pipeline{Source: NewSliceSource(schema, rows), Sink: win}
			ctx := context.Background()
			if err := pipe.Run(ctx); err != nil {
				t.Fatal(err)
			}
			var got []any
			for {
				b, err := win.Next(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if b == nil {
					break
				}
				for _, r := range b.ToRows() {
					got = append(got, r["out"])
				}
			}
			checkFrameOut(t, tc.name, got, tc.want)
		})
	}
}
