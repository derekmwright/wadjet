package exec

import (
	"context"
	"fmt"
	"math"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestRawRowContainerSpillMatchesMemory gates #611: a GROUP BY over an ARRAY,
// ROW, MAP or VECTOR column plus a NON-simple aggregate — the shapes
// canUseExternalMerge returns false for, here COUNT(DISTINCT) — takes the
// LEGACY raw-row spill path, and it must answer the SAME groups past a spill
// as the whole table in memory does.
//
// This is #566's sibling. #566 gave the PARTIAL-STATE drain a lossless
// container VALUE codec; the raw-row path had its own serializer
// (memory.SpillManager.SpillRows) that still rendered a container box with
// fmt.Sprintf. On read that was a STRING, and batch.FromRows refused to write
// it into a container vector (#361's silent-write guard) — the query FAILED
// with "cannot store string into ARRAY vector" the moment the spill buffer
// flushed. The fix encodes the box through the same ADR-0023 codec before
// SpillRows and decodes it back after ReadSpilledRows.
//
// Both halves are load-bearing: the reference run fixes the right group set,
// and the spilling run must ACTUALLY reach disk — the test asserts
// h.spillFiles is non-empty after Consume, so a future change that stops
// flushing here cannot turn the gate green by skipping the path it covers.
func TestRawRowContainerSpillMatchesMemory(t *testing.T) {
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
			name: "array of string with a colliding pair",
			col: parquet.Column{Name: "k", Type: parquet.TypeArray, Nullable: true,
				ElementType: strElem()},
			// g%2 toggles between ARRAY['a<g> b'] (one element with a space)
			// and ARRAY['a<g>','b'] (two elements) — the exact pair whose `%v`
			// rendering collides, so a display-form key would MERGE two groups.
			val: func(g int64) any {
				if g%2 == 0 {
					return []any{fmt.Sprintf("a%d b", g)}
				}
				return []any{fmt.Sprintf("a%d", g), "b"}
			},
		},
		{
			name: "array of int64",
			col: parquet.Column{Name: "k", Type: parquet.TypeArray, Nullable: true,
				ElementType: &parquet.Column{Name: "element", Type: parquet.TypeInt64, Nullable: true}},
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
					if g%17 != 3 { // every 17th group is a NULL container
						k = tc.val(g)
					}
					rows = append(rows, map[string]any{"k": k, "v": int64(bi*1000 + ri + 1)})
				}
				batches = append(batches, batch.FromRows(schema, rows))
			}
			mk := func(spill *memory.SpillManager) *HashAggregate {
				// COUNT(DISTINCT v) is a non-simple aggregate, so
				// canUseExternalMerge is false and the aggregate takes the
				// legacy raw-row spill path — the one #611 is about.
				h := NewHashAggregate([]string{"k"}, []AggColumn{
					{Func: AggCountDistinct, InputCol: "v", OutputCol: "nd", OutputType: parquet.TypeInt64},
				})
				h.Spill = spill
				return h
			}

			want := rawSpillAggRows(runHashAggToMap(t, mk(nil), batches))

			// Lower the flush threshold so the in-memory buffer actually
			// reaches disk, and pre-reserve most of a tight budget so the
			// spill branch is taken from the first batch (the issue repro).
			defer func(v int64) { spillFileTargetBytes = v }(spillFileTargetBytes)
			spillFileTargetBytes = 4000

			tracker := memory.NewTracker("test", 1_024)
			sm, err := memory.NewSpillManager(t.TempDir(), tracker)
			if err != nil {
				t.Fatal(err)
			}
			tracker.ForceReserve(900)

			got := rawSpillAggRows(runRawRowSpillChecked(t, mk(sm), batches))
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

// runRawRowSpillChecked drives a HashAggregate and asserts the legacy raw-row
// spill path actually wrote a file to disk during Consume (h.spillFiles is
// non-empty before Finalize), so the gate cannot pass without exercising the
// path #611 lives on.
func runRawRowSpillChecked(t *testing.T, h *HashAggregate, batches []*batch.RecordBatch) []map[string]any {
	t.Helper()
	ctx := context.Background()
	if err := h.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}
	for i, b := range batches {
		if err := h.Consume(ctx, b); err != nil {
			t.Fatalf("Consume #%d: %v", i, err)
		}
	}
	if len(h.spillFiles) == 0 {
		t.Fatal("the legacy raw-row spill path was never entered: this run never flushed a file")
	}
	if err := h.Finalize(ctx); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	var rows []map[string]any
	for {
		out, err := h.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if out == nil {
			break
		}
		rows = append(rows, out.ToRows()...)
	}
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return rows
}

// rawSpillAggRows renders each output row to a comparable, sorted string. A
// container is a []any / map[string]any / []float32 tree — not comparable
// with == — so fmt's rendering (which sorts map keys and prints NaN as "NaN")
// gives a stable key per group.
func rawSpillAggRows(rows []map[string]any) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, fmt.Sprintf("k=%v nd=%v", r["k"], r["nd"]))
	}
	sort.Strings(out)
	return out
}
