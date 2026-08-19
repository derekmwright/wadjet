package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// An outer join whose one side delivers no batch at all still owes the rows
// that side shapes, and it has to NAME that side's columns to emit them. The
// runtime cannot learn a schema from a batch that never arrives, so the
// planner declares one (#348/#352).

var (
	emptySideProbeSchema = []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "amount", Type: parquet.TypeFloat64},
	}
	emptySideBuildSchema = []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "label", Type: parquet.TypeString},
	}
)

// runJoinPipeline drives one join to completion the way a real driver does —
// including the FlushableOperator drain that emits a RIGHT/FULL join's
// unmatched build rows — and returns the result rows plus the output schema.
func runJoinPipeline(t *testing.T, hj *HashJoin, probeRows []map[string]any) ([]map[string]any, []parquet.Column) {
	t.Helper()
	probe := hj.Probe()
	sink := &CollectSink{}
	pipe := &Pipeline{
		Source: NewSliceSource(emptySideProbeSchema, probeRows),
		Ops:    []UnaryOperator{probe},
		Sink:   sink,
	}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatalf("pipeline: %v", err)
	}
	return sink.Rows, sink.Schema()
}

func buildEmpty(t *testing.T, hj *HashJoin) {
	t.Helper()
	if err := hj.Build(context.Background(), NewSliceSource(emptySideBuildSchema, nil)); err != nil {
		t.Fatalf("build: %v", err)
	}
}

// A LEFT JOIN over an empty build keeps every probe row AND every build
// COLUMN, present and NULL. Absent is not the same as NULL: an absent column
// reads as NULL through a projection's missing-name fallback while COUNT(col)
// counts it and IS NULL misses it — the wrong answer that looks right (#348).
func TestLeftJoinEmptyBuildKeepsBuildColumns(t *testing.T) {
	hj := NewHashJoin(LeftJoin, []string{"id"}, []string{"id"})
	hj.BuildSchemaHint = emptySideBuildSchema
	buildEmpty(t, hj)

	rows, schema := runJoinPipeline(t, hj, []map[string]any{
		{"id": int64(1), "amount": 10.0},
		{"id": int64(2), "amount": 20.0},
	})
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2 — an empty build side must not delete the preserved rows", len(rows))
	}
	var hasLabel bool
	for _, col := range schema {
		if col.Name == "label" {
			hasLabel = true
		}
	}
	if !hasLabel {
		t.Fatalf("output schema %v carries no build column; the declared BuildSchemaHint was ignored", schema)
	}
	for i, r := range rows {
		v, ok := r["label"]
		if !ok {
			t.Fatalf("row %d has no 'label' key: %v", i, r)
		}
		if v != nil {
			t.Errorf("row %d: label = %v, want NULL", i, v)
		}
	}
}

// Without a declared schema the join cannot name the missing side, so the
// columns stay absent — the pre-fix behaviour. Pinned so the hint's role is
// unambiguous: it is the declaration that buys the columns, not a change in
// the probe.
func TestLeftJoinEmptyBuildWithoutHint(t *testing.T) {
	hj := NewHashJoin(LeftJoin, []string{"id"}, []string{"id"})
	buildEmpty(t, hj)

	rows, _ := runJoinPipeline(t, hj, []map[string]any{{"id": int64(1), "amount": 10.0}})
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	if _, ok := rows[0]["label"]; ok {
		t.Errorf("row carries 'label' with no BuildSchemaHint: %v", rows[0])
	}
}

// An INNER join over an empty build emits nothing — the case the empty-build
// short-circuit exists for, and the control that says the LEFT fix did not
// simply stop dropping rows.
func TestInnerJoinEmptyBuildEmitsNothing(t *testing.T) {
	hj := NewHashJoin(InnerJoin, []string{"id"}, []string{"id"})
	hj.BuildSchemaHint = emptySideBuildSchema
	buildEmpty(t, hj)

	rows, _ := runJoinPipeline(t, hj, []map[string]any{{"id": int64(1), "amount": 10.0}})
	if len(rows) != 0 {
		t.Fatalf("got %d rows, want 0", len(rows))
	}
}

// A RIGHT JOIN whose probe matches NOTHING still emits every build row. The
// probe produces no output batch in that case, which is what used to skip the
// unmatched flush entirely (#352) — and the preserved side's own values must
// survive, not be mapped onto the NULL half.
func TestRightJoinNoMatchesFlushesEveryBuildRow(t *testing.T) {
	hj := NewHashJoin(RightJoin, []string{"id"}, []string{"id"})
	hj.BuildFromRows(emptySideBuildSchema, []map[string]any{
		{"id": int64(10), "label": "ten"},
		{"id": int64(11), "label": "eleven"},
	})

	rows, _ := runJoinPipeline(t, hj, []map[string]any{
		{"id": int64(1), "amount": 10.0},
		{"id": int64(2), "amount": 20.0},
	})
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2 (both build rows, NULL-padded on the probe side)", len(rows))
	}
	seen := map[string]bool{}
	for _, r := range rows {
		label, _ := r["label"].(string)
		seen[label] = true
		if r["amount"] != nil {
			t.Errorf("row %v: probe column 'amount' should be NULL for an unmatched build row", r)
		}
	}
	for _, want := range []string{"ten", "eleven"} {
		if !seen[want] {
			t.Errorf("build row %q missing from the flush; got %v", want, rows)
		}
	}
}

// The mirror of the empty-build case: a RIGHT JOIN whose PROBE side is empty.
// Nothing can be learned about the probe's columns at runtime, so the flush
// names them from the declared ProbeSchemaHint.
func TestRightJoinEmptyProbeUsesProbeSchemaHint(t *testing.T) {
	hj := NewHashJoin(RightJoin, []string{"id"}, []string{"id"})
	hj.ProbeSchemaHint = emptySideProbeSchema
	hj.BuildFromRows(emptySideBuildSchema, []map[string]any{
		{"id": int64(10), "label": "ten"},
	})

	rows, schema := runJoinPipeline(t, hj, nil)
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1 — an empty probe must not delete the preserved build rows", len(rows))
	}
	if rows[0]["label"] != "ten" {
		t.Errorf("label = %v, want \"ten\"", rows[0]["label"])
	}
	var hasAmount bool
	for _, col := range schema {
		if col.Name == "amount" {
			hasAmount = true
		}
	}
	if !hasAmount {
		t.Fatalf("output schema %v carries no probe column; the declared ProbeSchemaHint was ignored", schema)
	}
	if v, ok := rows[0]["amount"]; !ok || v != nil {
		t.Errorf("amount = %v (present=%v), want a present NULL", v, ok)
	}
}

// A FULL OUTER join carries both halves at once: the unmatched probe rows
// come out of the probe, the unmatched build rows out of the flush.
func TestFullOuterJoinBothSidesUnmatched(t *testing.T) {
	hj := NewHashJoin(FullOuterJoin, []string{"id"}, []string{"id"})
	hj.BuildFromRows(emptySideBuildSchema, []map[string]any{
		{"id": int64(10), "label": "ten"},
	})

	rows, _ := runJoinPipeline(t, hj, []map[string]any{{"id": int64(1), "amount": 10.0}})
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2 (one unmatched probe row + one unmatched build row): %v", len(rows), rows)
	}
	var probeSide, buildSide int
	for _, r := range rows {
		switch {
		case r["amount"] != nil && r["label"] == nil:
			probeSide++
		case r["amount"] == nil && r["label"] != nil:
			buildSide++
		default:
			t.Errorf("row %v is neither a NULL-padded probe row nor a NULL-padded build row", r)
		}
	}
	if probeSide != 1 || buildSide != 1 {
		t.Errorf("got %d probe-side and %d build-side rows, want 1 and 1", probeSide, buildSide)
	}
}

// The flush happens exactly once per join whatever reaches it first. Every
// probe clone shares one HashJoin and every driver drains FlushableOperator
// at the end, so an unguarded flush would emit the unmatched rows once per
// clone.
func TestFlushUnmatchedRowsHappensOnce(t *testing.T) {
	hj := NewHashJoin(RightJoin, []string{"id"}, []string{"id"})
	hj.BuildFromRows(emptySideBuildSchema, []map[string]any{
		{"id": int64(10), "label": "ten"},
	})
	probe := hj.Probe()
	clone := probe.Clone().(*HashJoinProbe)

	first := probe.FlushUnmatchedRows()
	if first == nil || first.Len != 1 {
		t.Fatalf("first flush returned %v, want 1 row", first)
	}
	if again := probe.FlushUnmatchedRows(); again != nil {
		t.Errorf("second flush on the same probe returned %d rows, want none", again.Len)
	}
	if cloned := clone.FlushUnmatchedRows(); cloned != nil {
		t.Errorf("flush on a clone of the same join returned %d rows, want none", cloned.Len)
	}
	if probe.HasPendingFlush() {
		t.Error("HasPendingFlush is still true after the unmatched rows were flushed")
	}
}

// An inner join has no unmatched build rows to flush and must not claim it
// does — HasPendingFlush drives the drain loop in every fragment runner.
func TestInnerJoinHasNoPendingFlush(t *testing.T) {
	hj := NewHashJoin(InnerJoin, []string{"id"}, []string{"id"})
	hj.BuildFromRows(emptySideBuildSchema, []map[string]any{{"id": int64(10), "label": "ten"}})
	probe := hj.Probe()
	if probe.HasPendingFlush() {
		t.Error("inner join reports a pending flush")
	}
	if b := probe.FlushUnmatchedRows(); b != nil {
		t.Errorf("inner join flushed %d rows", b.Len)
	}
}
