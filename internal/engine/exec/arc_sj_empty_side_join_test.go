// SPDX-License-Identifier: MIT

package exec

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// THE GRACE JOIN'S DRAIN EMITS EVERY PRESERVED ROW OF A PARTITION, IN THE
// JOIN'S OWN OUTPUT SHAPE, WHATEVER THE OTHER SIDE'S SIZE — ZERO INCLUDED
// (#1359).
//
// An evicted build partition is replayed at the end of the probe through a
// temporary join (HashJoinProbe.NextFlush). That replay's probe learns the
// probe-side schema from the first probe batch it is handed — and a probe
// side that produced no rows hands it none. Its RIGHT / FULL unmatched-row
// flush then named the NULL half from a nil schema, and the replay took the
// output narrowing (OutputFilter / OutputExclude*) from the first probe batch
// too, so the evicted partitions' preserved rows came out as a batch with no
// probe columns and every build column: a different shape from the in-memory
// partitions' rows, which the consumer then read positionally (the preserved
// side's values NULL, or shifted into the empty side's columns). The replay
// now takes the probe schema the main join recorded or was declared
// (ProbeSchemaHint) and the flushing probe's own narrowing — the rule the
// in-memory flush (FlushUnmatchedRows) already read.
//
// The grid: {RIGHT, FULL, LEFT, INNER} × which side holds the rows (the build
// side, probed by an empty side; or the probe side, against an empty build) ×
// the preserved side {1 row, 2 048 rows, 20 000 rows, every third key NULL,
// 50 distinct keys over 3 000 rows}, every row carrying a DECIMAL and an
// ARRAY column × the key {INT32, INT64, STRING, DATE} × the output narrowed or
// not × the arm {no spill manager; a 64 MiB tracker forcing an eviction on
// every arriving build batch, asserted; 4 MiB, 512 KiB and 256 KiB trackers
// alone}. Every output batch must carry the join's declared output schema,
// and the rows must be exactly the preserved side's with the other side NULL
// (or none, where the join type preserves the empty side).
func TestArcSJGraceDrainEmitsPreservedRowsOverAnEmptySide(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: 1 600 joins up to 20 000 rows")
	}
	keyTypes := []struct {
		name string
		typ  parquet.TypeID
		val  func(int) any
	}{
		{"int32", parquet.TypeInt32, func(k int) any { return int32(k) }},
		{"int64", parquet.TypeInt64, func(k int) any { return int64(k) + 10_000_000_000 }},
		{"string", parquet.TypeString, func(k int) any { return fmt.Sprintf("k%d", k) }},
		{"date", parquet.TypeDate, func(k int) any { return int32(10_957 + k) }},
	}
	shapes := []struct {
		name string
		n    int
		key  func(i int) (int, bool) // key, null
	}{
		{"one", 1, func(i int) (int, bool) { return i, false }},
		{"b2048", 2048, func(i int) (int, bool) { return i, false }},
		{"b20000", 20000, func(i int) (int, bool) { return i, false }},
		{"nullk", 300, func(i int) (int, bool) { return i, i%3 == 0 }},
		{"dup", 3000, func(i int) (int, bool) { return i % 50, false }},
	}
	arms := []struct {
		name   string
		budget int64 // -1 = no spill manager
		forced bool
	}{
		{"inmem", -1, false},
		{"forced64MiB", 64 << 20, true},
		{"4MiB", 4 << 20, false},
		{"512KiB", 512 << 10, false},
		{"256KiB", 256 << 10, false},
	}
	joinTypes := []struct {
		name string
		jt   JoinType
	}{{"right", RightJoin}, {"full", FullOuterJoin}, {"left", LeftJoin}, {"inner", InnerJoin}}

	ctx := context.Background()
	failures, cells, engagedForced := 0, 0, 0
	perArm := map[string]int{}
	for _, kt := range keyTypes {
		pSchema := []parquet.Column{
			{Name: "k", Type: kt.typ, Nullable: true},
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "v_dec", Type: parquet.TypeDecimal, Nullable: true, Precision: 12, Scale: 2},
			{Name: "v_arr", Type: parquet.TypeArray, Nullable: true,
				ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}},
		}
		eSchema := []parquet.Column{
			{Name: "ek", Type: kt.typ, Nullable: true},
			{Name: "ey", Type: parquet.TypeInt32, Nullable: true},
		}
		for _, sh := range shapes {
			rows := make([]map[string]any, sh.n)
			for i := range rows {
				r := map[string]any{"id": int64(i), "v_dec": int64(i*125 + 1),
					"v_arr": []any{fmt.Sprintf("a%d", i), fmt.Sprintf("b%d", i)}}
				if k, null := sh.key(i); null {
					r["k"] = nil
				} else {
					r["k"] = kt.val(k)
				}
				rows[i] = r
			}
			// The preserved side as batches, the way a scan delivers it —
			// fresh for every join, which may release what it consumed.
			mk := func() []*batch.RecordBatch {
				var out []*batch.RecordBatch
				for lo := 0; lo < len(rows); lo += batch.DefaultBatchSize {
					out = append(out, batch.FromRows(pSchema, rows[lo:min(lo+batch.DefaultBatchSize, len(rows))]))
				}
				return out
			}
			pBatches := mk()
			// The reference rendering of every preserved row, by id.
			want := make(map[int64]string, len(rows))
			for _, b := range pBatches {
				for i := 0; i < b.Len; i++ {
					want[b.Columns[1].Int64Data[i]] = fmt.Sprint(b.Columns[0].GetValue(i), "|",
						b.Columns[2].GetValue(i), "|", b.Columns[3].GetValue(i))
				}
			}
			for _, jt := range joinTypes {
				for _, buildPreserved := range []bool{true, false} {
					for _, narrowed := range []bool{false, true} {
						for _, arm := range arms {
							cells++
							name := fmt.Sprintf("%s/%s/%s/build=%s/narrowed=%v/%s", jt.name, sh.name, kt.name,
								map[bool]string{true: "preserved", false: "empty"}[buildPreserved], narrowed, arm.name)
							preservedOut := (buildPreserved && (jt.jt == RightJoin || jt.jt == FullOuterJoin)) ||
								(!buildPreserved && (jt.jt == LeftJoin || jt.jt == FullOuterJoin))
							before := JoinPartitionsEvicted.Load()
							msg := sjRunCell(ctx, t, jt.jt, buildPreserved, narrowed, arm.budget, arm.forced,
								pSchema, eSchema, mk(), want, preservedOut)
							evicted := JoinPartitionsEvicted.Load() - before
							if arm.forced && buildPreserved {
								if evicted == 0 {
									msg += "; no build partition was evicted — the cell compared two in-memory runs"
								} else {
									engagedForced++
								}
							}
							if msg != "" {
								failures++
								perArm[jt.name+"/"+arm.name]++
								if failures <= 40 {
									t.Errorf("%s: %s", name, msg)
								}
							}
						}
					}
				}
			}
		}
	}
	t.Logf("SJ-EXEC cells=%d failures=%d forced-engaged=%d per join/arm=%v", cells, failures, engagedForced, perArm)
	if failures > 40 {
		t.Errorf("%d failing cells in all (first 40 shown)", failures)
	}
	if cells != 1600 || engagedForced != 4*5*4*2 {
		t.Fatalf("%d cells, %d forced cells engaged: the grid must discriminate", cells, engagedForced)
	}
}

// sjRunCell runs one join and returns "" or what differed.
func sjRunCell(ctx context.Context, t *testing.T, jt JoinType, buildPreserved, narrowed bool, budget int64, forced bool,
	pSchema, eSchema []parquet.Column, pBatches []*batch.RecordBatch, want map[int64]string, preservedOut bool,
) string {
	var hj *HashJoin
	var buildSrc *testBatchSource
	var probeSchema, buildSchema []parquet.Column
	if buildPreserved {
		hj = NewHashJoin(jt, []string{"ek"}, []string{"k"})
		buildSrc = &testBatchSource{batches: pBatches}
		probeSchema, buildSchema = eSchema, pSchema
	} else {
		hj = NewHashJoin(jt, []string{"k"}, []string{"ek"})
		buildSrc = &testBatchSource{}
		probeSchema, buildSchema = pSchema, eSchema
	}
	// What the planner declares for each side (join_plan.go joinSideSchemas).
	hj.ProbeSchemaHint, hj.BuildSchemaHint = probeSchema, buildSchema
	if budget >= 0 {
		tracker := memory.NewTracker("sj", budget)
		sm, err := memory.NewSpillManager(t.TempDir(), tracker)
		if err != nil {
			t.Fatal(err)
		}
		defer sm.Cleanup()
		hj.Spill, hj.MemTracker = sm, tracker
	}
	if forced {
		prev := ForceJoinPartitionEvictEvery(1)
		defer ForceJoinPartitionEvictEvery(prev)
	}
	if err := hj.Build(ctx, buildSrc); err != nil {
		return "build: " + err.Error()
	}
	defer hj.Close()
	probe := hj.Probe()
	var filter map[string]bool
	if narrowed {
		filter = map[string]bool{"id": true, "v_dec": true, "v_arr": true, "ey": true}
		probe.OutputFilter = filter
	}
	declared := JoinOutputSchema(jt, probeSchema, buildSchema, "", nil, false, filter, nil, nil)
	sink := &CollectSink{SkipFinalizeToRows: true}
	var probeSrc Source
	if buildPreserved {
		probeSrc = NewSliceSource(eSchema, nil)
	} else {
		probeSrc = &testBatchSource{batches: pBatches}
	}
	pipe := &Pipeline{Source: probeSrc, Ops: []UnaryOperator{probe}, Sink: sink}
	if err := pipe.Run(ctx); err != nil {
		return "run: " + err.Error()
	}
	idx := func(s []parquet.Column, n string) int {
		for i, c := range s {
			if c.Name == n {
				return i
			}
		}
		return -1
	}
	got := 0
	seen := map[int64]bool{}
	for _, b := range sink.batches {
		if b.Len == 0 {
			continue
		}
		if names(b.Schema) != names(declared) {
			return fmt.Sprintf("a batch of %d rows carries columns [%s], the join declares [%s]", b.Len, names(b.Schema), names(declared))
		}
		iID, iK, iD, iA, iEK, iEY := idx(b.Schema, "id"), idx(b.Schema, "k"), idx(b.Schema, "v_dec"),
			idx(b.Schema, "v_arr"), idx(b.Schema, "ek"), idx(b.Schema, "ey")
		for _, row := range b.ToRowValues() {
			got++
			idv, ok := row[iID].(int64)
			if !ok {
				return fmt.Sprintf("a preserved row's id reads %#v", row[iID])
			}
			if seen[idv] {
				return fmt.Sprintf("id %d emitted twice", idv)
			}
			seen[idv] = true
			k := any("<filtered>")
			if iK >= 0 {
				k = row[iK]
			}
			rendered := fmt.Sprint(k, "|", row[iD], "|", row[iA])
			w := want[idv]
			if iK < 0 {
				w = "<filtered>" + w[strings.Index(w, "|"):]
			}
			if rendered != w {
				return fmt.Sprintf("id %d: got %s, want %s", idv, rendered, w)
			}
			if (iEK >= 0 && row[iEK] != nil) || row[iEY] != nil {
				return fmt.Sprintf("id %d: the empty side's columns are not NULL", idv)
			}
		}
	}
	wantRows := 0
	if preservedOut {
		wantRows = len(want)
	}
	if got != wantRows {
		return fmt.Sprintf("%d rows, want %d", got, wantRows)
	}
	return ""
}

func names(s []parquet.Column) string {
	out := make([]string, len(s))
	for i, c := range s {
		out[i] = c.Name
	}
	return strings.Join(out, ",")
}
