package wadjet

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// mergeRuns is deliberately large. The defect was NONDETERMINISTIC — 8 of 12
// runs wrong over a three-file target, 0 of 12 over a single-file one —
// because it turned on whether `SELECT *` happened to return the target's
// files in manifest order. A gate that runs once can pass on a broken build.
const mergeRuns = 24

// mergeFixture builds a target of THREE files with THREE rows each and a
// source naming one row in the MIDDLE of the MIDDLE file.
//
// Both dimensions matter. Three files is what makes the two orderings able to
// disagree at all; three rows per file is what makes an off-by-one WITHIN a
// file visible, which a one-row-per-file fixture cannot show.
func mergeFixture(t *testing.T, matchID int64) (*DB, context.Context) {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "mt", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("mt", schema, nil, ingest.DefaultConfig())
	for f := 0; f < 3; f++ {
		rows := make([]map[string]any, 0, 3)
		for r := 0; r < 3; r++ {
			id := int64(f*3 + r + 1)
			rows = append(rows, map[string]any{"id": id, "v": id * 10})
		}
		if err := ing.Ingest(ctx, rows); err != nil {
			t.Fatal(err)
		}
		// One FlushAll per group — three files.
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
	}

	src := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "ms", src, nil); err != nil {
		t.Fatal(err)
	}
	si := db.NewIngester("ms", src, nil, ingest.DefaultConfig())
	if err := si.Ingest(ctx, []map[string]any{{"id": matchID, "v": int64(999)}}); err != nil {
		t.Fatal(err)
	}
	if err := si.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db, ctx
}

// mergeRowSet renders the target's live rows as a sorted "id=v" list, so a
// wrong row set is reported as a set difference and not as a length.
func mergeRowSet(t *testing.T, db *DB, ctx context.Context) []string {
	t.Helper()
	q, err := db.Query(ctx, "SELECT id, v FROM mt")
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, 0, len(q.Rows))
	for _, r := range q.Rows {
		out = append(out, fmt.Sprintf("%v=%v", r["id"], r["v"]))
	}
	sort.Strings(out)
	return out
}

func mergeWant(update bool, matchID int64) []string {
	var out []string
	for id := int64(1); id <= 9; id++ {
		switch {
		case id != matchID:
			out = append(out, fmt.Sprintf("%d=%d", id, id*10))
		case update:
			out = append(out, fmt.Sprintf("%d=999", id))
		}
	}
	sort.Strings(out)
	return out
}

func sameRowSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// A MERGE that RETURNS SUCCESS deleted the wrong physical row when the target
// spanned more than one file (#676).
//
// `matchedTargetIndices` were indices into the target's `SELECT *` result
// order; the delete-marker loop re-derived physical positions by walking the
// manifest's file order. Nothing made the two agree, so over a three-file
// target 8 of 12 runs destroyed a row the statement never matched and
// duplicated one it did — silently, with the statement reporting success.
func TestMergeUpdateMarksTheMatchedPhysicalRow(t *testing.T) {
	const matchID = 5 // the middle row of the middle file
	want := mergeWant(true, matchID)
	for run := 0; run < mergeRuns; run++ {
		db, ctx := mergeFixture(t, matchID)
		if _, err := db.Execute(ctx,
			"MERGE INTO mt t USING ms s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = 999"); err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
		if got := mergeRowSet(t, db, ctx); !sameRowSet(got, want) {
			t.Fatalf("run %d: surviving rows\n  got  %v\n  want %v", run, got, want)
		}
	}
}

// The same for WHEN MATCHED THEN DELETE, which shares the marker path and had
// the same defect.
func TestMergeDeleteMarksTheMatchedPhysicalRow(t *testing.T) {
	const matchID = 5
	want := mergeWant(false, matchID)
	for run := 0; run < mergeRuns; run++ {
		db, ctx := mergeFixture(t, matchID)
		if _, err := db.Execute(ctx,
			"MERGE INTO mt t USING ms s ON t.id = s.id WHEN MATCHED THEN DELETE"); err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
		if got := mergeRowSet(t, db, ctx); !sameRowSet(got, want) {
			t.Fatalf("run %d: surviving rows\n  got  %v\n  want %v", run, got, want)
		}
	}
}

// The control the issue asks for: UPDATE and DELETE over the SAME fixture,
// which have always carried (file, row-in-file) per file and were never
// wrong. If these ever diverge from the MERGE results above, the fixture —
// not the fix — is what changed.
func TestUpdateAndDeleteAgreeWithMergeOnTheSameFixture(t *testing.T) {
	const matchID = 5
	for run := 0; run < mergeRuns; run++ {
		db, ctx := mergeFixture(t, matchID)
		if _, err := db.Execute(ctx, "UPDATE mt SET v = 999 WHERE id = 5"); err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
		if got, want := mergeRowSet(t, db, ctx), mergeWant(true, matchID); !sameRowSet(got, want) {
			t.Fatalf("run %d: UPDATE left\n  got  %v\n  want %v", run, got, want)
		}
	}
	for run := 0; run < mergeRuns; run++ {
		db, ctx := mergeFixture(t, matchID)
		if _, err := db.Execute(ctx, "DELETE FROM mt WHERE id = 5"); err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
		if got, want := mergeRowSet(t, db, ctx), mergeWant(false, matchID); !sameRowSet(got, want) {
			t.Fatalf("run %d: DELETE left\n  got  %v\n  want %v", run, got, want)
		}
	}
}

// #674's MERGE arm, which could not be gated until the marker loop and the
// match scan became the same walk: a superseded copy is not a row a MERGE can
// match, and it must not occupy a position either.
//
// Re-merging the same row used to multiply it exactly the way re-updating did,
// because the target scan read the manifest's files raw.
func TestReMergeDoesNotMultiplyRows(t *testing.T) {
	const matchID = 5
	db, ctx := mergeFixture(t, matchID)
	for i := 1; i <= 5; i++ {
		if _, err := db.Execute(ctx, fmt.Sprintf(
			"MERGE INTO mt t USING ms s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = %d", i*100)); err != nil {
			t.Fatalf("merge %d: %v", i, err)
		}
		got := mergeRowSet(t, db, ctx)
		want := make([]string, 0, 9)
		for id := int64(1); id <= 9; id++ {
			if id == matchID {
				want = append(want, fmt.Sprintf("%d=%d", id, i*100))
			} else {
				want = append(want, fmt.Sprintf("%d=%d", id, id*10))
			}
		}
		sort.Strings(want)
		if !sameRowSet(got, want) {
			t.Fatalf("after %d MERGEs the target is\n  got  %v\n  want %v", i, got, want)
		}
	}
}
