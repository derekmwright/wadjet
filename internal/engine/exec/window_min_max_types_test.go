package exec

import (
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #569's spilled half, over all 22 types.
//
// The in-memory MIN/MAX deque compares COLUMNAR (kernel.CompareValuesAt on the
// input vector). The two spilled evaluators do not have a vector to compare:
// the partition-at-a-time walker reads rows back off sorted runs, and the
// empty-PARTITION-BY streamer carries its running extreme as a BOX across
// batches. Both resolve their comparator from the DECLARED column
// (newBoxedCompare, compare_boxed.go), and a type whose box is a DISPLAY
// rendering — IPV4, IPV6, CIDR — orders differently there unless the
// comparator re-keys it.
//
// runWindowBothPaths is the assertion: identical batches through an in-memory
// Window and a spilling one, row multisets compared exactly, and it FAILS if
// the spill never happened, so the arm cannot quietly become a second
// in-memory run.
//
// The declaration handed to the operator is deliberately the WRONG one —
// parquet.TypeFloat64, which is what the planner falls back to for an input
// type it could not resolve — so this also covers exec.Window's own retyping
// (retypeValueColumns), the way TestWindowValueFunctionsRetypeFromInput does
// for the five value functions.

// wmmTypeCase is one column type plus the values the fixture writes for it.
type wmmTypeCase struct {
	name string
	col  parquet.Column
	val  func(i int) any
}

// wmmTypeCases covers every type the engine has. Each generator crosses at
// least one boundary where the RENDERED order and the VALUE order disagree,
// where such a boundary exists: c_ipv4 walks 9.255.255.x into 10.0.0.x, c_ipv6
// changes hex-digit count, c_cidr mixes prefix lengths and families, c_dec
// spans "10.0010" against "2.0002".
func wmmTypeCases() []wmmTypeCase {
	dec := parquet.Column{Name: "v", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true}
	return []wmmTypeCase{
		{"bool", parquet.Column{Name: "v", Type: parquet.TypeBool, Nullable: true},
			func(i int) any { return i%3 == 0 }},
		{"int32", parquet.Column{Name: "v", Type: parquet.TypeInt32, Nullable: true},
			func(i int) any { return int32(i*7 - 200) }},
		{"int64", parquet.Column{Name: "v", Type: parquet.TypeInt64, Nullable: true},
			func(i int) any { return int64(i)*1_000_003 - 5_000_000 }},
		{"float32", parquet.Column{Name: "v", Type: parquet.TypeFloat32, Nullable: true},
			func(i int) any { return float32(i)/7 - 3 }},
		{"float64", parquet.Column{Name: "v", Type: parquet.TypeFloat64, Nullable: true},
			func(i int) any { return float64(i)/3 - 11 }},
		{"string", parquet.Column{Name: "v", Type: parquet.TypeString, Nullable: true},
			func(i int) any { return fmt.Sprintf("s-%04d", i) }},
		{"bytes", parquet.Column{Name: "v", Type: parquet.TypeBytes, Nullable: true},
			func(i int) any { return []byte(fmt.Sprintf("b-%04d", i)) }},
		{"timestamp", parquet.Column{Name: "v", Type: parquet.TypeTimestamp, Nullable: true},
			func(i int) any { return int64(1_700_000_000_000 + i*61_000) }},
		{"ipv4", parquet.Column{Name: "v", Type: parquet.TypeIPv4, Nullable: true},
			func(i int) any {
				v := uint32(9)<<24 | uint32(255)<<16 | uint32(255)<<8 | 240 + uint32(i)
				return fmt.Sprintf("%d.%d.%d.%d", byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
			}},
		{"ipv6", parquet.Column{Name: "v", Type: parquet.TypeIPv6, Nullable: true},
			func(i int) any { return fmt.Sprintf("2001:db8::%x", i+9) }},
		{"cidr", parquet.Column{Name: "v", Type: parquet.TypeCIDR, Nullable: true},
			func(i int) any {
				switch i % 4 {
				case 0:
					return fmt.Sprintf("9.255.255.%d/24", i%256)
				case 1:
					return fmt.Sprintf("10.0.%d.0/16", i%256)
				case 2:
					return fmt.Sprintf("10.0.%d.7/24", i%256)
				default:
					return fmt.Sprintf("2001:db8:%x::/48", i)
				}
			}},
		{"mac", parquet.Column{Name: "v", Type: parquet.TypeMAC, Nullable: true},
			func(i int) any { return fmt.Sprintf("aa:bb:cc:00:%02x:%02x", (i/256)%256, i%256) }},
		{"port", parquet.Column{Name: "v", Type: parquet.TypePort, Nullable: true},
			func(i int) any { return int32(1024 + i) }},
		{"protocol", parquet.Column{Name: "v", Type: parquet.TypeProtocol, Nullable: true},
			func(i int) any { return int32(i % 256) }},
		{"duration", parquet.Column{Name: "v", Type: parquet.TypeDuration, Nullable: true},
			func(i int) any { return int64(i)*1_000_000 - 50_000_000 }},
		{"uuid", parquet.Column{Name: "v", Type: parquet.TypeUUID, Nullable: true},
			func(i int) any { return fmt.Sprintf("00000000-0000-4000-8000-%012x", i+9) }},
		{"date", parquet.Column{Name: "v", Type: parquet.TypeDate, Nullable: true},
			func(i int) any { return fmt.Sprintf("20%02d-%02d-%02d", 10+i%15, 1+i%12, 1+i%28) }},
		{"decimal", dec,
			func(i int) any { return float64(i) + 0.0001*float64(i%997) - 50 }},
		{"array", parquet.Column{Name: "v", Type: parquet.TypeArray, Nullable: true,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}},
			func(i int) any { return []any{fmt.Sprintf("a%04d", i)} }},
		{"row", parquet.Column{Name: "v", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
			{Name: "z", Type: parquet.TypeString, Nullable: true},
			{Name: "a", Type: parquet.TypeInt64, Nullable: true},
		}}, func(i int) any {
			return map[string]any{"z": fmt.Sprintf("r-%04d", i), "a": int64(i) * 11}
		}},
		{"map", parquet.Column{Name: "v", Type: parquet.TypeMap, Nullable: true,
			ElementType: &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
				{Name: "key", Type: parquet.TypeString},
				{Name: "value", Type: parquet.TypeInt64, Nullable: true},
			}}}, func(i int) any {
			return map[string]any{fmt.Sprintf("k%d", i%5): int64(i)}
		}},
		{"vector", parquet.Column{Name: "v", Type: parquet.TypeVector, Nullable: true, Dimension: 4},
			func(i int) any { return []float32{float32(i), float32(i) + 0.5, -float32(i), 0.25} }},
	}
}

// wmmRows builds the fixture: a low-cardinality partition key, a unique order
// key, and the typed value, NULL every 9th row so a partition always carries
// some NULLs and never only NULLs.
func wmmRows(tc wmmTypeCase, n int) []map[string]any {
	rows := make([]map[string]any, n)
	for i := range rows {
		r := map[string]any{"grp": int64(i % 5), "ts": int64(i)}
		if i%9 != 8 {
			r["v"] = tc.val(i)
		} else {
			r["v"] = nil
		}
		rows[i] = r
	}
	return rows
}

func wmmSchema(tc wmmTypeCase) []parquet.Column {
	return []parquet.Column{
		{Name: "grp", Type: parquet.TypeInt64},
		{Name: "ts", Type: parquet.TypeInt64},
		tc.col,
	}
}

func TestWindowExternalMinMaxEveryType(t *testing.T) {
	for _, tc := range wmmTypeCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			cols := []WindowColumn{
				// The whole partition, so the answer is the partition's
				// extreme and the deque's advance covers every row.
				{Func: WinMin, InputCol: "v", OutputCol: "w_min", OutputType: parquet.TypeFloat64,
					PartitionBy: []string{"grp"}, OrderBy: []SortKey{{Column: "ts", Order: Ascending}},
					Frame: &WindowFrameSpec{Mode: "rows",
						Start: WindowBound{Type: "unbounded_preceding"},
						End:   WindowBound{Type: "unbounded_following"}}},
				{Func: WinMax, InputCol: "v", OutputCol: "w_max", OutputType: parquet.TypeFloat64,
					PartitionBy: []string{"grp"}, OrderBy: []SortKey{{Column: "ts", Order: Ascending}},
					Frame: &WindowFrameSpec{Mode: "rows",
						Start: WindowBound{Type: "unbounded_preceding"},
						End:   WindowBound{Type: "unbounded_following"}}},
				// A sliding frame, where a value is also EVICTED from the
				// deque: an order that is merely stable is not enough here.
				{Func: WinMin, InputCol: "v", OutputCol: "w_slide", OutputType: parquet.TypeFloat64,
					PartitionBy: []string{"grp"}, OrderBy: []SortKey{{Column: "ts", Order: Ascending}},
					Frame: &WindowFrameSpec{Mode: "rows",
						Start: WindowBound{Type: "preceding", Offset: 3},
						End:   WindowBound{Type: "current_row"}}},
				// A FORWARD-reaching frame (N FOLLOWING). Its lower end also
				// moves, so the deque both appends and evicts, and the frame
				// needs rows AFTER the current one — the case
				// groupNeedsMaterializedFrame routes to the partition walker
				// rather than the streaming evaluator (window.go), which no
				// other spec here exercises.
				{Func: WinMax, InputCol: "v", OutputCol: "w_follow", OutputType: parquet.TypeFloat64,
					PartitionBy: []string{"grp"}, OrderBy: []SortKey{{Column: "ts", Order: Ascending}},
					Frame: &WindowFrameSpec{Mode: "rows",
						Start: WindowBound{Type: "current_row"},
						End:   WindowBound{Type: "following", Offset: 2}}},
				// An explicit RANGE frame. ts is unique, so each row is its own
				// peer group and RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT
				// ROW is the running extreme — the peer-group bounds path
				// (columnarPeerGroups), reached only in RANGE mode with an
				// ORDER BY, which the ROWS specs above never take.
				{Func: WinMin, InputCol: "v", OutputCol: "w_range", OutputType: parquet.TypeFloat64,
					PartitionBy: []string{"grp"}, OrderBy: []SortKey{{Column: "ts", Order: Ascending}},
					Frame: &WindowFrameSpec{Mode: "range",
						Start: WindowBound{Type: "unbounded_preceding"},
						End:   WindowBound{Type: "current_row"}}},
			}
			runWindowBothPaths(t, wmmSchema(tc), cols, wmmRows(tc, 240), 16,
				[]string{"grp", "ts", "v", "w_min", "w_max", "w_slide", "w_follow", "w_range"})
		})
	}
}

// TestWindowGlobalMinMaxEveryType is the empty-PARTITION-BY half: the whole
// input is one partition, and past the budget it is answered by the streaming
// two-pass evaluator (window_global.go) whose running MIN/MAX is a BOX carried
// across batches. That is the site newBoxedCompare decides, and the one where
// an address type's display rendering is not its order.
func TestWindowGlobalMinMaxEveryType(t *testing.T) {
	for _, tc := range wmmTypeCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			// Two runs, not one spec list with four columns. A window's
			// spec GROUP is (PARTITION BY, ORDER BY), and only the LAST
			// group reaches the streaming evaluator — the earlier ones take
			// a disk-to-disk pass through computePartitionColumnar. Putting
			// both shapes in one list would leave whichever came first
			// gating the columnar path a second time instead of the boxed
			// one it is here for.
			rows := wmmRows(tc, 240)

			// No ORDER BY: the pass-1 scalar form, collectGlobalWindowStats,
			// whose running extreme is compared by globalInputCompares.
			runWindowBothPaths(t, wmmSchema(tc), []WindowColumn{
				{Func: WinMin, InputCol: "v", OutputCol: "g_min", OutputType: parquet.TypeFloat64},
				{Func: WinMax, InputCol: "v", OutputCol: "g_max", OutputType: parquet.TypeFloat64},
			}, rows, 16, []string{"grp", "ts", "v", "g_min", "g_max"})

			// With an ORDER BY: the running form, whose extreme is carried in
			// globalWindowStreamer.runMin/runMax across batches and
			// backfilled per peer group.
			runWindowBothPaths(t, wmmSchema(tc), []WindowColumn{
				{Func: WinMin, InputCol: "v", OutputCol: "r_min", OutputType: parquet.TypeFloat64,
					OrderBy: []SortKey{{Column: "ts", Order: Ascending}}},
				{Func: WinMax, InputCol: "v", OutputCol: "r_max", OutputType: parquet.TypeFloat64,
					OrderBy: []SortKey{{Column: "ts", Order: Ascending}}},
			}, rows, 16, []string{"grp", "ts", "v", "r_min", "r_max"})
		})
	}
}
