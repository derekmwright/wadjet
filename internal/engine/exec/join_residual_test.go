package exec

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The outer-join ON residual (#358): HashJoin.Residual runs on the combined
// row before a key match is accepted. The semantics under test, per join
// type:
//
//   - a probe row whose key matched but whose every candidate FAILED the
//     residual is UNMATCHED — LEFT/FULL still emit it NULL-padded, never
//     drop it (that is the difference between ON and WHERE on an outer join);
//   - a build row counts as matched only when some probe row passed key AND
//     residual, which is what FlushUnmatched reads for RIGHT/FULL — per
//     build ROW, not per key chain, because a residual accepts individual
//     candidates of a chain;
//   - a residual returning false for a NULL-bearing pair (SQL's UNKNOWN)
//     rejects the candidate exactly like false;
//   - with no join keys at all the whole build side is every probe row's
//     candidate set and the residual does all of the matching.

var (
	residProbeSchema = []parquet.Column{
		{Name: "pid", Type: parquet.TypeInt64},
		{Name: "pv", Type: parquet.TypeInt64},
	}
	residBuildSchema = []parquet.Column{
		{Name: "bid", Type: parquet.TypeInt64},
		{Name: "bv", Type: parquet.TypeInt64},
	}
)

// residGreater accepts a pair when probe.pv > build.bv, with SQL comparison
// semantics: NULL on either side is UNKNOWN, which rejects.
func residGreater(probe *batch.RecordBatch, probeRow int, build *batch.RecordBatch, buildRow int) bool {
	pv := probe.Columns[probe.ColumnIndex("pv")]
	bv := build.Columns[build.ColumnIndex("bv")]
	if pv.Nulls.IsNullFast(probeRow) || bv.Nulls.IsNullFast(buildRow) {
		return false
	}
	return pv.Int64Data[probeRow] > bv.Int64Data[buildRow]
}

func buildResidJoin(t *testing.T, jt JoinType, leftKeys, rightKeys []string, buildRows []map[string]any) *HashJoin {
	t.Helper()
	hj := NewHashJoin(jt, leftKeys, rightKeys)
	hj.Residual = residGreater
	hj.BuildSchemaHint = residBuildSchema
	hj.ProbeSchemaHint = residProbeSchema
	if err := hj.Build(context.Background(), NewSliceSource(residBuildSchema, buildRows)); err != nil {
		t.Fatalf("build: %v", err)
	}
	return hj
}

func runResidJoin(t *testing.T, hj *HashJoin, probeRows []map[string]any) []map[string]any {
	t.Helper()
	probe := hj.Probe()
	sink := &CollectSink{}
	pipe := &Pipeline{
		Source: NewSliceSource(residProbeSchema, probeRows),
		Ops:    []UnaryOperator{probe},
		Sink:   sink,
	}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatalf("pipeline: %v", err)
	}
	return sink.Rows
}

// rowKey renders one output row compactly for order-insensitive comparison;
// NULL cells print as <nil>.
func rowKey(r map[string]any) string {
	return fmt.Sprintf("pid=%v pv=%v bid=%v bv=%v", r["pid"], r["pv"], r["bid"], r["bv"])
}

func assertRows(t *testing.T, got []map[string]any, want []string) {
	t.Helper()
	gotKeys := make([]string, len(got))
	for i, r := range got {
		gotKeys[i] = rowKey(r)
	}
	sort.Strings(gotKeys)
	sort.Strings(want)
	if len(gotKeys) != len(want) {
		t.Fatalf("got %d rows, want %d\ngot:  %v\nwant: %v", len(gotKeys), len(want), gotKeys, want)
	}
	for i := range want {
		if gotKeys[i] != want[i] {
			t.Fatalf("row %d: got %q, want %q\nall got:  %v\nall want: %v", i, gotKeys[i], want[i], gotKeys, want)
		}
	}
}

// The shared fixture: key 1 has TWO build rows (a chain), so per-candidate
// acceptance is distinguishable from per-chain marking; key 2 has one build
// row whose residual always fails; key 3 exists on the build side only; the
// probe has a row with no key partner at all (pid 9) and a NULL-pv row whose
// residual is UNKNOWN against every candidate.
var (
	residBuildRows = []map[string]any{
		{"bid": int64(1), "bv": int64(10)},
		{"bid": int64(1), "bv": int64(90)},
		{"bid": int64(2), "bv": int64(50)},
		{"bid": int64(3), "bv": int64(1)},
	}
	residProbeRows = []map[string]any{
		{"pid": int64(1), "pv": int64(40)}, // beats bv=10, not bv=90 → one accepted candidate
		{"pid": int64(2), "pv": int64(40)}, // key match, residual fails → unmatched
		{"pid": int64(9), "pv": int64(99)}, // no key partner at all
		{"pid": int64(2), "pv": nil},       // key match, residual UNKNOWN → unmatched
	}
)

func TestLeftJoinResidual(t *testing.T) {
	hj := buildResidJoin(t, LeftJoin, []string{"pid"}, []string{"bid"}, residBuildRows)
	rows := runResidJoin(t, hj, residProbeRows)
	assertRows(t, rows, []string{
		"pid=1 pv=40 bid=1 bv=10",           // the accepted candidate
		"pid=2 pv=40 bid=<nil> bv=<nil>",    // residual failed → padded, NOT dropped
		"pid=9 pv=99 bid=<nil> bv=<nil>",    // no key partner → padded
		"pid=2 pv=<nil> bid=<nil> bv=<nil>", // residual UNKNOWN → padded
	})
}

func TestInnerVsLeftResidualDropsOnlyOnInner(t *testing.T) {
	hj := buildResidJoin(t, InnerJoin, []string{"pid"}, []string{"bid"}, residBuildRows)
	rows := runResidJoin(t, hj, residProbeRows)
	// Inner: rejected and keyless rows are simply gone.
	assertRows(t, rows, []string{
		"pid=1 pv=40 bid=1 bv=10",
	})
}

func TestRightJoinResidualFlushesPerBuildRow(t *testing.T) {
	hj := buildResidJoin(t, RightJoin, []string{"pid"}, []string{"bid"}, residBuildRows)
	rows := runResidJoin(t, hj, residProbeRows)
	// Probe row pid=1 accepted ONE of key 1's two build rows: the other
	// (bv=90) must still flush unmatched — per-chain marking would lose it.
	// The residual-failed key-2 row and the partnerless key-3 row flush too.
	assertRows(t, rows, []string{
		"pid=1 pv=40 bid=1 bv=10",
		"pid=<nil> pv=<nil> bid=1 bv=90",
		"pid=<nil> pv=<nil> bid=2 bv=50",
		"pid=<nil> pv=<nil> bid=3 bv=1",
	})
}

func TestFullJoinResidualBothSides(t *testing.T) {
	hj := buildResidJoin(t, FullOuterJoin, []string{"pid"}, []string{"bid"}, residBuildRows)
	rows := runResidJoin(t, hj, residProbeRows)
	assertRows(t, rows, []string{
		// Matched.
		"pid=1 pv=40 bid=1 bv=10",
		// Probe side preserved.
		"pid=2 pv=40 bid=<nil> bv=<nil>",
		"pid=9 pv=99 bid=<nil> bv=<nil>",
		"pid=2 pv=<nil> bid=<nil> bv=<nil>",
		// Build side preserved.
		"pid=<nil> pv=<nil> bid=1 bv=90",
		"pid=<nil> pv=<nil> bid=2 bv=50",
		"pid=<nil> pv=<nil> bid=3 bv=1",
	})
}

// Keyless: empty key lists degenerate the build to a single chain holding
// every row, so each probe row's candidate set is the whole build side.
func TestKeylessLeftJoinResidual(t *testing.T) {
	hj := buildResidJoin(t, LeftJoin, nil, nil, residBuildRows)
	rows := runResidJoin(t, hj, []map[string]any{
		{"pid": int64(7), "pv": int64(60)}, // beats bv 10, 50, 1
		{"pid": int64(8), "pv": int64(0)},  // beats nothing → padded
	})
	assertRows(t, rows, []string{
		"pid=7 pv=60 bid=1 bv=10",
		"pid=7 pv=60 bid=2 bv=50",
		"pid=7 pv=60 bid=3 bv=1",
		"pid=8 pv=0 bid=<nil> bv=<nil>",
	})
}

func TestKeylessFullJoinResidual(t *testing.T) {
	hj := buildResidJoin(t, FullOuterJoin, nil, nil, residBuildRows)
	rows := runResidJoin(t, hj, []map[string]any{
		{"pid": int64(7), "pv": int64(20)}, // beats bv 10 and 1 only
	})
	assertRows(t, rows, []string{
		"pid=7 pv=20 bid=1 bv=10",
		"pid=7 pv=20 bid=3 bv=1",
		// Never accepted by any probe row → flushed.
		"pid=<nil> pv=<nil> bid=1 bv=90",
		"pid=<nil> pv=<nil> bid=2 bv=50",
	})
}

func TestKeylessRightJoinResidualEmptyProbe(t *testing.T) {
	hj := buildResidJoin(t, RightJoin, nil, nil, residBuildRows)
	rows := runResidJoin(t, hj, nil)
	// No probe rows at all: every build row flushes unmatched.
	assertRows(t, rows, []string{
		"pid=<nil> pv=<nil> bid=1 bv=10",
		"pid=<nil> pv=<nil> bid=1 bv=90",
		"pid=<nil> pv=<nil> bid=2 bv=50",
		"pid=<nil> pv=<nil> bid=3 bv=1",
	})
}

// The bounded-output resume protocol must carry the "any candidate accepted
// yet" bit across a mid-chain suspension, or a LEFT join either double-pads
// or drops the suspended row. A build chain far longer than the output bound
// forces suspensions inside one probe row's chain.
func TestLeftJoinResidualBoundedOutputResume(t *testing.T) {
	const chain = 3000 // > DefaultBatchSize so the fan-out suspends mid-chain
	build := make([]map[string]any, chain)
	for i := range build {
		build[i] = map[string]any{"bid": int64(1), "bv": int64(i)}
	}
	hj := buildResidJoin(t, LeftJoin, []string{"pid"}, []string{"bid"}, build)

	probe := hj.Probe()
	probe.EnableBoundedOutput()
	sink := &CollectSink{}
	pipe := &Pipeline{
		Source: NewSliceSource(residProbeSchema, []map[string]any{
			{"pid": int64(1), "pv": int64(1000)}, // accepts bv 0..999
			{"pid": int64(1), "pv": int64(0)},    // accepts nothing → one padded row
		}),
		Ops:  []UnaryOperator{probe},
		Sink: sink,
	}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatalf("pipeline: %v", err)
	}
	matched, padded := 0, 0
	for _, r := range sink.Rows {
		if r["bv"] == nil {
			padded++
		} else {
			matched++
		}
	}
	if matched != 1000 || padded != 1 {
		t.Fatalf("matched=%d padded=%d, want 1000 matched and exactly 1 padded", matched, padded)
	}
}
