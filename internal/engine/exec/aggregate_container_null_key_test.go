package exec

import (
	"fmt"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestNullContainerGroupKeyDoesNotDesyncLaterRows gates the offsets-on-NULL
// corruption class for a CONTAINER group key past a partial-aggregate drain.
//
// writePartialKeyToColumn's null arm used to hand-roll the bookkeeping: it set
// the null bit and then, for the five bytes-class types it enumerated, wrote
// an empty slot to keep BytesData's offsets moving. A container was in neither
// list, so a NULL ARRAY/MAP left its Offsets entry unwritten and a NULL ROW
// skipped every CHILD's slot — and a ROW with a STRING field has a
// BytesColumn down there whose offsets then describe the wrong bytes. The next
// non-null row read from the start of the arena: `s:s106` came back as
// `s:s0s1s10s100...`, a run-on of every string written so far.
//
// The shape matters as much as the type. A container's merge key starts with a
// small uvarint, so a NULL key ("<null>", appendKeyValue) sorts LAST when the
// container column is the only group column — nothing is written after it and
// the desync has nobody left to corrupt. A LEADING INT64 group column moves
// the NULL container into the middle of the merge order, which is where an
// ordinary `GROUP BY tenant_id, attrs` puts it.
//
// The fix is to stop hand-rolling: Vector.WriteNullAt already advances every
// shape's variable-length bookkeeping, containers and their children included,
// and is what decodeSerializedKeyIntoColumns uses on the same job.
func TestNullContainerGroupKeyDoesNotDesyncLaterRows(t *testing.T) {
	strElem := func() *parquet.Column {
		return &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}
	}

	for _, tc := range []struct {
		name string
		col  parquet.Column
		val  func(g int64) any
	}{
		{
			// The reviewer's repro: a bytes-class leaf UNDER a ROW, which is
			// the storage the skipped child slot desyncs.
			name: "row with a string field",
			col: parquet.Column{Name: "k", Type: parquet.TypeRow, Nullable: true,
				Fields: []parquet.Column{
					{Name: "n", Type: parquet.TypeInt64, Nullable: true},
					{Name: "s", Type: parquet.TypeString, Nullable: true},
				}},
			val: func(g int64) any { return map[string]any{"n": g, "s": fmt.Sprintf("s%d", g)} },
		},
		{
			name: "array of string",
			col: parquet.Column{Name: "k", Type: parquet.TypeArray, Nullable: true,
				ElementType: strElem()},
			val: func(g int64) any { return []any{fmt.Sprintf("a%d", g)} },
		},
		{
			name: "map",
			col: parquet.Column{Name: "k", Type: parquet.TypeMap, Nullable: true,
				ElementType: &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
					{Name: "key", Type: parquet.TypeString},
					{Name: "value", Type: parquet.TypeInt64, Nullable: true},
				}}},
			val: func(g int64) any { return map[string]any{fmt.Sprintf("k%d", g): g} },
		},
		{
			name: "vector",
			col:  parquet.Column{Name: "k", Type: parquet.TypeVector, Nullable: true, Dimension: 3},
			val:  func(g int64) any { return []float32{float32(g), float32(g) + 0.5, -float32(g)} },
		},
		{
			name: "row of containers",
			col: parquet.Column{Name: "k", Type: parquet.TypeRow, Nullable: true,
				Fields: []parquet.Column{
					{Name: "l", Type: parquet.TypeArray, Nullable: true, ElementType: strElem()},
					{Name: "s", Type: parquet.TypeString, Nullable: true},
				}},
			val: func(g int64) any {
				return map[string]any{"l": []any{fmt.Sprintf("e%d", g)}, "s": fmt.Sprintf("s%d", g)}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const numBatches, rowsPerBatch, numGroups = 25, 200, 200
			// The LEADING int64 column is what puts a NULL container key in
			// the MIDDLE of the merge order instead of last.
			schema := []parquet.Column{
				{Name: "a", Type: parquet.TypeInt64},
				tc.col,
				{Name: "v", Type: parquet.TypeInt64},
			}
			batches := make([]*batch.RecordBatch, 0, numBatches)
			for bi := 0; bi < numBatches; bi++ {
				rows := make([]map[string]any, 0, rowsPerBatch)
				for ri := 0; ri < rowsPerBatch; ri++ {
					g := int64((bi*rowsPerBatch + ri) % numGroups)
					var k any
					if g%17 != 3 {
						k = tc.val(g)
					}
					rows = append(rows, map[string]any{
						"a": g, "k": k, "v": int64(bi*1000 + ri + 1),
					})
				}
				batches = append(batches, batch.FromRows(schema, rows))
			}
			mk := func(spill *memory.SpillManager) *HashAggregate {
				h := NewHashAggregate([]string{"a", "k"}, []AggColumn{
					{Func: AggSum, InputCol: "v", OutputCol: "total", OutputType: parquet.TypeInt64},
				})
				h.Spill = spill
				return h
			}

			want := containerAggRows2(runHashAggToMap(t, mk(nil), batches))
			if len(want) != numGroups {
				t.Fatalf("in-memory run: %d groups, want %d", len(want), numGroups)
			}

			tracker := memory.NewTracker("test", 1_024)
			sm, err := memory.NewSpillManager(t.TempDir(), tracker)
			if err != nil {
				t.Fatal(err)
			}
			tracker.ForceReserve(900)
			got := containerAggRows2(runHashAggToMapSpillChecked(t, mk(sm), batches))

			if len(got) != len(want) {
				t.Fatalf("spilled run: %d groups, want %d", len(got), len(want))
			}
			bad := 0
			for i := range want {
				if got[i] != want[i] {
					if bad < 3 {
						t.Errorf("row %d differs between the spilled and in-memory runs\n  spilled: %s\n  memory:  %s",
							i, got[i], want[i])
					}
					bad++
				}
			}
			if bad > 0 {
				t.Errorf("%d of %d rows differ", bad, len(want))
			}
		})
	}
}

// containerAggRows2 renders the two-column key form.
func containerAggRows2(rows []map[string]any) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, fmt.Sprintf("a=%v k=%v total=%v", r["a"], r["k"], r["total"]))
	}
	sort.Strings(out)
	return out
}
