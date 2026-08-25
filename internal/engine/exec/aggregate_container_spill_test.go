package exec

import (
	"fmt"
	"math"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestContainerGroupByAcrossASpillMatchesMemory gates #566: a GROUP BY over an
// ARRAY, ROW, MAP or VECTOR column answers the SAME VALUES past a
// partial-aggregate drain as it does with the whole table in memory.
//
// This replaces the pin that recorded the opposite. Before the fix, the
// spilling run FAILED THE QUERY — a container group key was captured as a
// partialTagString (its `%v` rendering) and writePartialKeyFallback handed
// that text to a container vector's SetValue, which refuses it (#361's
// silent-write guard, doing its job: the alternative is a silently wrong
// group key). The repair gave the drained key its own lossless encoding
// (appendContainerKeyValue, aggregate_container_key.go) and rebuilt the value
// through the same GetValue → SetValue round trip the un-spilled emit path
// already performs, which is why the two are comparable value for value
// rather than merely both non-empty.
//
// Both halves are load-bearing:
//
//   - the reference run must produce the right group COUNT, so the comparison
//     is against a correct answer rather than against a matching failure, and
//   - the spilling run must actually SPILL (runHashAggToMapSpillChecked fails
//     when the external-merge path was never entered), so a future change that
//     stops spilling here cannot turn this gate green by skipping the path it
//     exists to cover.
//
// The element types are chosen for what they distinguish. CIDR and DECIMAL
// leaves box as TEXT out of GetValue and re-parse on the way in, so they are
// where a "just keep the display form" repair would look right and be wrong.
// NULL elements and an EMPTY container separate three values that all render
// alike. A container inside a container walks the encoder's recursion. And a
// VECTOR carries raw float bits, which the MERGE key deliberately folds
// (NaN payloads onto one, -0.0 onto +0.0) and the VALUE must not.
func TestContainerGroupByAcrossASpillMatchesMemory(t *testing.T) {
	strElem := func() *parquet.Column {
		return &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}
	}
	mapEntry := func() *parquet.Column {
		return &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
			{Name: "key", Type: parquet.TypeString},
			{Name: "value", Type: parquet.TypeInt64, Nullable: true},
		}}
	}

	for _, tc := range []struct {
		name string
		col  parquet.Column
		val  func(g int64) any
	}{
		{
			name: "array of int64",
			col: parquet.Column{Name: "k", Type: parquet.TypeArray, Nullable: true,
				ElementType: &parquet.Column{Name: "element", Type: parquet.TypeInt64, Nullable: true}},
			// Length cycles 0..2 so an EMPTY array is one of the groups and
			// must stay distinct from the one-element ones.
			val: func(g int64) any {
				switch g % 3 {
				case 0:
					return []any{}
				case 1:
					return []any{g}
				default:
					return []any{g, -g}
				}
			},
		},
		{
			name: "array of string with null elements",
			col: parquet.Column{Name: "k", Type: parquet.TypeArray, Nullable: true,
				ElementType: strElem()},
			val: func(g int64) any {
				if g%2 == 0 {
					return []any{fmt.Sprintf("s%d", g), nil}
				}
				return []any{nil, fmt.Sprintf("s%d", g)}
			},
		},
		{
			name: "array of cidr",
			col: parquet.Column{Name: "k", Type: parquet.TypeArray, Nullable: true,
				ElementType: &parquet.Column{Name: "element", Type: parquet.TypeCIDR, Nullable: true}},
			val: func(g int64) any { return []any{fmt.Sprintf("10.0.%d.%d/24", g/256, g%256)} },
		},
		{
			name: "array of decimal",
			col: parquet.Column{Name: "k", Type: parquet.TypeArray, Nullable: true,
				ElementType: &parquet.Column{Name: "element", Type: parquet.TypeDecimal,
					Precision: 18, Scale: 4, Nullable: true}},
			val: func(g int64) any { return []any{fmt.Sprintf("%d.%04d", g, g%10000)} },
		},
		{
			name: "row",
			col: parquet.Column{Name: "k", Type: parquet.TypeRow, Nullable: true,
				Fields: []parquet.Column{
					{Name: "n", Type: parquet.TypeInt64, Nullable: true},
					{Name: "s", Type: parquet.TypeString, Nullable: true},
				}},
			val: func(g int64) any {
				if g%4 == 3 {
					return map[string]any{"n": g, "s": nil}
				}
				return map[string]any{"n": g, "s": fmt.Sprintf("s%d", g)}
			},
		},
		{
			name: "row of containers",
			col: parquet.Column{Name: "k", Type: parquet.TypeRow, Nullable: true,
				Fields: []parquet.Column{
					{Name: "l", Type: parquet.TypeArray, Nullable: true, ElementType: strElem()},
					{Name: "m", Type: parquet.TypeMap, Nullable: true, ElementType: mapEntry()},
				}},
			val: func(g int64) any {
				return map[string]any{
					"l": []any{fmt.Sprintf("e%d", g)},
					"m": map[string]any{fmt.Sprintf("k%d", g%5): g},
				}
			},
		},
		{
			name: "map",
			col: parquet.Column{Name: "k", Type: parquet.TypeMap, Nullable: true,
				ElementType: mapEntry()},
			val: func(g int64) any {
				return map[string]any{fmt.Sprintf("a%d", g): g, "b": g * 2}
			},
		},
		{
			name: "vector",
			col:  parquet.Column{Name: "k", Type: parquet.TypeVector, Nullable: true, Dimension: 4},
			// A NaN and a negative zero ride in every vector. The merge key
			// folds both onto one canonical bit pattern (keyFloat32bits) so
			// the group is the same group either way; the VALUE must come
			// back with the bits it went in with, which is what a decode off
			// the merge key would get wrong.
			val: func(g int64) any {
				return []float32{float32(g), float32(math.NaN()), float32(math.Copysign(0, -1)), 0.25}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const numBatches, rowsPerBatch, numGroups = 25, 200, 200
			schema := []parquet.Column{tc.col, {Name: "v", Type: parquet.TypeInt64}}
			batches := make([]*batch.RecordBatch, 0, numBatches)
			for bi := 0; bi < numBatches; bi++ {
				rows := make([]map[string]any, 0, rowsPerBatch)
				for ri := 0; ri < rowsPerBatch; ri++ {
					g := int64((bi*rowsPerBatch + ri) % numGroups)
					var k any
					// Every 17th group is a NULL container — a value of its
					// own, and the one the typed-null arm answers for.
					if g%17 != 3 {
						k = tc.val(g)
					}
					rows = append(rows, map[string]any{"k": k, "v": int64(bi*1000 + ri + 1)})
				}
				batches = append(batches, batch.FromRows(schema, rows))
			}
			mk := func(spill *memory.SpillManager) *HashAggregate {
				h := NewHashAggregate([]string{"k"}, []AggColumn{
					{Func: AggSum, InputCol: "v", OutputCol: "total", OutputType: parquet.TypeInt64},
					{Func: AggCount, InputCol: "v", OutputCol: "n", OutputType: parquet.TypeInt64},
				})
				h.Spill = spill
				return h
			}

			// The reference run's own group count, computed here rather
			// than read off the run: several of the value cycles above map
			// more than one g to the same container (the empty array, the
			// NULL rows), and a comparison against a run that lost groups
			// would still agree with itself.
			distinct := map[string]bool{}
			for g := 0; g < numGroups; g++ {
				if g%17 == 3 {
					distinct["<null>"] = true
					continue
				}
				distinct[fmt.Sprintf("%v", tc.val(int64(g)))] = true
			}

			want := containerAggRows(runHashAggToMap(t, mk(nil), batches))
			if len(want) != len(distinct) {
				t.Fatalf("in-memory run: %d groups, want %d distinct container keys", len(want), len(distinct))
			}

			// A tight budget with the reservation already made forces the
			// partial-drain path from the first batch, the way
			// TestExternalMergeSpill_SingleIntKeySum does.
			tracker := memory.NewTracker("test", 1_024)
			sm, err := memory.NewSpillManager(t.TempDir(), tracker)
			if err != nil {
				t.Fatal(err)
			}
			tracker.ForceReserve(900)
			got := containerAggRows(runHashAggToMapSpillChecked(t, mk(sm), batches))

			if len(got) != len(want) {
				t.Fatalf("spilled run: %d groups, want %d", len(got), len(want))
			}
			for i := range want {
				if got[i] != want[i] {
					t.Errorf("row %d differs between the spilled and in-memory runs\n  spilled: %s\n  memory:  %s",
						i, got[i], want[i])
				}
			}
		})
	}
}

// containerAggRows renders each output row to a comparable string and sorts
// them. A container value is a []any / map[string]any / []float32 tree, which
// is not comparable with == and not orderable on its own; fmt's rendering
// sorts map keys, so the same tree always renders the same way, and a NaN
// renders as "NaN" rather than comparing unequal to itself.
func containerAggRows(rows []map[string]any) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, fmt.Sprintf("k=%v total=%v n=%v", r["k"], r["total"], r["n"]))
	}
	sort.Strings(out)
	return out
}
