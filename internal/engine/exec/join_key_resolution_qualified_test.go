package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestBuildFromRowsResolvesQualifiedBuildKey is #600's BuildFromRows arm.
//
// BuildFromRows resolved its build-key column with a bare b.ColumnIndex, so a
// qualified ON key ("ON p.pk = b.bk") spelled "b.bk" never matched the build
// batch's bare "bk" column and resolved to -1. buildKeyFromBatch then appends a
// lone null-flag byte for that key while every probe row serializes its real
// value, so the two key byte-strings can never be equal: the inner join loses
// every row — silently. columnIndexFallback strips the qualifier and finds the
// column, restoring the join. The assertion is on the ANSWER (values), not on
// the resolved index.
func TestBuildFromRowsResolvesQualifiedBuildKey(t *testing.T) {
	buildSchema := []parquet.Column{
		{Name: "bk", Type: parquet.TypeString},
		{Name: "bname", Type: parquet.TypeString},
	}
	buildRows := []map[string]any{
		{"bk": "a", "bname": "row-a"},
		{"bk": "b", "bname": "row-b"},
		{"bk": "c", "bname": "row-c"}, // no probe match
	}

	// RightKey is qualified ("b.bk") but the build batch carries the bare
	// "bk" — the exact shape #550/#600 repair everywhere else.
	hj := NewHashJoin(InnerJoin, []string{"pk"}, []string{"b.bk"})
	hj.BuildFromRows(buildSchema, buildRows)

	probeSchema := []parquet.Column{
		{Name: "pk", Type: parquet.TypeString},
		{Name: "pval", Type: parquet.TypeString},
	}
	probeRows := []map[string]any{
		{"pk": "a", "pval": "P1"}, // matches row-a
		{"pk": "b", "pval": "P2"}, // matches row-b
		{"pk": "z", "pval": "P3"}, // no match
	}

	sink := &CollectSink{}
	pipe := &Pipeline{
		Source: NewSliceSource(probeSchema, probeRows),
		Ops:    []UnaryOperator{hj.Probe()},
		Sink:   sink,
	}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatalf("pipeline: %v", err)
	}

	got := map[string]string{}
	for _, r := range sink.Rows {
		got[r["pval"].(string)] = r["bname"].(string)
	}
	want := map[string]string{"P1": "row-a", "P2": "row-b"}
	if len(got) != len(want) {
		t.Fatalf("qualified-build-key inner join emitted %d rows, want %d: %v", len(got), len(want), got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("row %s joined to %q, want %q (full result %v)", k, got[k], v, got)
		}
	}
}

// TestFixKeyAssignmentRebuildResolvesQualifiedBuildKey is #600's
// FixKeyAssignment arm — a mixed-spelling multi-key join whose repair rebuilds
// the string index.
//
// One key needs a side swap (its build column landed on the left of "="); the
// other is already correctly on the right but spelled qualified ("b.bk2")
// against a build batch carrying the bare "bk2". FixKeyAssignment's rebuild
// re-resolved ALL build keys with a bare b.ColumnIndex, so the qualified key
// resolved to -1 and buildKeyFromBatch serialized a lone null flag for it while
// probes serialize the real value — total match loss on an otherwise valid
// join. columnIndexFallback resolves the qualifier and the join is correct.
func TestFixKeyAssignmentRebuildResolvesQualifiedBuildKey(t *testing.T) {
	// Build batch carries bare names.
	buildSchema := []parquet.Column{
		{Name: "bk1", Type: parquet.TypeString},
		{Name: "bk2", Type: parquet.TypeString},
		{Name: "bname", Type: parquet.TypeString},
	}
	buildRows := []map[string]any{
		{"bk1": "a", "bk2": "x", "bname": "row-ax"},
		{"bk1": "b", "bk2": "y", "bname": "row-by"},
		{"bk1": "a", "bk2": "z", "bname": "row-az"},
	}

	// Plan-time key spelling BEFORE repair:
	//   key0: LeftKeys[0]="bk1" is a build column, RightKeys[0]="p.pk1" is not
	//         -> FixKeyAssignment swaps it, forcing the rebuild.
	//   key1: LeftKeys[1]="p.pk2" (not in build) so no swap; RightKeys[1]
	//         stays "b.bk2" — qualified against the bare "bk2".
	hj := NewHashJoin(InnerJoin, []string{"bk1", "p.pk2"}, []string{"p.pk1", "b.bk2"})

	if err := hj.Build(context.Background(), NewSliceSource(buildSchema, buildRows)); err != nil {
		t.Fatalf("build: %v", err)
	}
	if !hj.FixKeyAssignment() {
		t.Fatal("FixKeyAssignment did not fire — test setup no longer exercises the rebuild path")
	}

	probeSchema := []parquet.Column{
		{Name: "pk1", Type: parquet.TypeString},
		{Name: "pk2", Type: parquet.TypeString},
		{Name: "pval", Type: parquet.TypeString},
	}
	probeRows := []map[string]any{
		{"pk1": "a", "pk2": "x", "pval": "P1"}, // matches row-ax
		{"pk1": "a", "pk2": "z", "pval": "P2"}, // matches row-az
		{"pk1": "b", "pk2": "q", "pval": "P3"}, // bk2 differs -> no match
	}

	sink := &CollectSink{}
	pipe := &Pipeline{
		Source: NewSliceSource(probeSchema, probeRows),
		Ops:    []UnaryOperator{hj.Probe()},
		Sink:   sink,
	}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatalf("pipeline: %v", err)
	}

	got := map[string]string{}
	for _, r := range sink.Rows {
		got[r["pval"].(string)] = r["bname"].(string)
	}
	want := map[string]string{"P1": "row-ax", "P2": "row-az"}
	if len(got) != len(want) {
		t.Fatalf("repaired multi-key inner join emitted %d rows, want %d: %v", len(got), len(want), got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("row %s joined to %q, want %q (full result %v)", k, got[k], v, got)
		}
	}
}
