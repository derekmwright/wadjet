package ingest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

func reviewCatalog(t *testing.T, schema pqt.Schema) *catalog.Catalog {
	t.Helper()
	ctx := context.Background()
	c := catalog.NewWithStore(objstore.NewMemStore(), "review")
	if e := c.Init(ctx); e != nil {
		t.Fatal(e)
	}
	if e := c.CreateTable(ctx, "events", schema, nil); e != nil {
		t.Fatal(e)
	}
	return c
}

func reviewRows(t *testing.T, c *catalog.Catalog) []map[string]any {
	t.Helper()
	ctx := context.Background()
	m, e := c.GetManifest(ctx, "events")
	if e != nil {
		t.Fatal(e)
	}
	var out []map[string]any
	for _, p := range m.Partitions {
		for _, f := range p.Files {
			r, _, e := c.Store().Get(ctx, c.Bucket(), f.Path)
			if e != nil {
				t.Fatal(e)
			}
			b, e := io.ReadAll(r)
			r.Close()
			if e != nil {
				t.Fatal(e)
			}
			pr, e := pqt.NewReader(bytes.NewReader(b), int64(len(b)))
			if e != nil {
				t.Fatal(e)
			}
			rows, e := pr.ReadRows(nil)
			if e != nil {
				t.Fatal(e)
			}
			out = append(out, rows...)
		}
	}
	return out
}

// TestReviewRejectedBatchRetainsPrefix is the #917 regression: a batch rejected
// partway must buffer nothing, so a retry of the corrected batch does not
// duplicate the accepted prefix. Ingest is all-or-nothing per call.
func TestReviewRejectedBatchRetainsPrefix(t *testing.T) {
	s := pqt.Schema{Columns: []pqt.Column{{Name: "id", Type: pqt.TypeInt64}}}
	c := reviewCatalog(t, s)
	ing := New(c, "events", s, nil, DefaultConfig())
	ctx := context.Background()
	e := ing.Ingest(ctx, []map[string]any{{"id": int64(1)}, {"id": "bad"}})
	if e == nil {
		t.Fatal("expected validation error")
	}
	t.Logf("rejected: %v", e)
	if e := ing.Ingest(ctx, []map[string]any{{"id": int64(1)}, {"id": int64(2)}}); e != nil {
		t.Fatal(e)
	}
	if e := ing.FlushAll(ctx); e != nil {
		t.Fatal(e)
	}
	if rows := reviewRows(t, c); len(rows) != 2 {
		t.Fatalf("retry of corrected rejected batch duplicated prefix: %v", rows)
	}
}

// TestReviewMissingPartitionKeyLaterInBatch is the #917 parallel: a row missing
// a partition key later in a batch must also roll back the whole call, not just
// the schema-validation path.
func TestReviewMissingPartitionKeyLaterInBatch(t *testing.T) {
	s := pqt.Schema{Columns: []pqt.Column{
		{Name: "id", Type: pqt.TypeInt64},
		{Name: "region", Type: pqt.TypeString},
	}}
	ctx := context.Background()
	c := catalog.NewWithStore(objstore.NewMemStore(), "review")
	if e := c.Init(ctx); e != nil {
		t.Fatal(e)
	}
	if e := c.CreateTable(ctx, "events", s, []string{"region"}); e != nil {
		t.Fatal(e)
	}
	ing := New(c, "events", s, []string{"region"}, DefaultConfig())
	// Second row lacks the partition key "region": the whole call must roll back.
	e := ing.Ingest(ctx, []map[string]any{
		{"id": int64(1), "region": "us"},
		{"id": int64(2)},
	})
	if e == nil {
		t.Fatal("expected missing-partition-key error")
	}
	if e := ing.Ingest(ctx, []map[string]any{
		{"id": int64(1), "region": "us"},
		{"id": int64(2), "region": "us"},
	}); e != nil {
		t.Fatal(e)
	}
	if e := ing.FlushAll(ctx); e != nil {
		t.Fatal(e)
	}
	if rows := reviewRows(t, c); len(rows) != 2 {
		t.Fatalf("retry after missing-partition-key rejection duplicated prefix: %v", rows)
	}
}

// TestReviewBufferedRowsAliasCaller is the #918 regression: the accumulator must
// own its buffered data, so a caller mutating its map or reusing its byte slice
// after Ingest returns cannot corrupt the not-yet-flushed rows.
func TestReviewBufferedRowsAliasCaller(t *testing.T) {
	s := pqt.Schema{Columns: []pqt.Column{{Name: "id", Type: pqt.TypeInt64}, {Name: "b", Type: pqt.TypeBytes}}}
	c := reviewCatalog(t, s)
	ing := New(c, "events", s, nil, DefaultConfig())
	ctx := context.Background()
	data := []byte("original")
	row := map[string]any{"id": int64(1), "b": data}
	if e := ing.Ingest(ctx, []map[string]any{row}); e != nil {
		t.Fatal(e)
	}
	row["id"] = int64(2)
	copy(data, []byte("mutated!"))
	if e := ing.FlushAll(ctx); e != nil {
		t.Fatal(e)
	}
	rows := reviewRows(t, c)
	if len(rows) != 1 || rows[0]["id"] != int64(1) || string(rows[0]["b"].([]byte)) != "original" {
		t.Fatalf("accepted data changed after return: %v", rows)
	}
}

// TestReviewNestedValueMutationAfterIngest is the #918 nested-value parallel:
// a mutable value INSIDE a container (an []byte inside an ARRAY, a nested map)
// must also be owned by the accumulator, so a caller mutating the inner value
// after Ingest returns cannot corrupt the buffered row. A shallow map copy
// alone would not fix this.
func TestReviewNestedValueMutationAfterIngest(t *testing.T) {
	s := pqt.Schema{Columns: []pqt.Column{
		{Name: "id", Type: pqt.TypeInt64},
		{Name: "a", Type: pqt.TypeArray, Nullable: true,
			ElementType: &pqt.Column{Name: "element", Type: pqt.TypeBytes, Nullable: true}},
	}}
	c := reviewCatalog(t, s)
	ing := New(c, "events", s, nil, DefaultConfig())
	ctx := context.Background()
	leaf := []byte("original")
	arr := []any{leaf}
	row := map[string]any{"id": int64(1), "a": arr}
	if e := ing.Ingest(ctx, []map[string]any{row}); e != nil {
		t.Fatal(e)
	}
	// Mutate the inner []byte and the container the caller still holds.
	copy(leaf, []byte("mutated!"))
	arr[0] = []byte("swapped!")
	if e := ing.FlushAll(ctx); e != nil {
		t.Fatal(e)
	}
	rows := reviewRows(t, c)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %v", rows)
	}
	got, ok := rows[0]["a"].([]any)
	if !ok || len(got) != 1 {
		t.Fatalf("expected 1-element array, got %#v", rows[0]["a"])
	}
	if string(got[0].([]byte)) != "original" {
		t.Fatalf("nested value changed after return: %q", got[0])
	}
}

// TestReviewFirstIngestBeforeTableExistsSucceeds guards the #919 binding
// against being over-broad: an ingester constructed for a table that does not
// exist yet (an embedder that buffers before CreateTable) must NOT be refused
// at the first Ingest — the incarnation binds lazily when the table exists.
// This is the create-then-first-ingest lane the combined battery caught
// (wadjet.TestReservedSlotNamespaceDoors/the_Ingester), pinned in-package.
func TestReviewFirstIngestBeforeTableExistsSucceeds(t *testing.T) {
	s := pqt.Schema{Columns: []pqt.Column{{Name: "id", Type: pqt.TypeInt64}}}
	ctx := context.Background()
	c := catalog.NewWithStore(objstore.NewMemStore(), "review")
	if e := c.Init(ctx); e != nil {
		t.Fatal(e)
	}
	// Table "events" does NOT exist yet. Buffering must still succeed.
	ing := New(c, "events", s, nil, DefaultConfig())
	if e := ing.Ingest(ctx, []map[string]any{{"id": int64(5)}}); e != nil {
		t.Fatalf("first ingest into a not-yet-created table was refused: %v", e)
	}
	// Now create the table and flush: the binding is taken lazily and the row
	// is written normally.
	if e := c.CreateTable(ctx, "events", s, nil); e != nil {
		t.Fatal(e)
	}
	if e := ing.FlushAll(ctx); e != nil {
		t.Fatalf("flush after the table was created was refused: %v", e)
	}
	rows := reviewRows(t, c)
	if len(rows) != 1 || rows[0]["id"] != int64(5) {
		t.Fatalf("create-later flush wrote wrong rows: %v", rows)
	}
}

// TestReviewOldIngesterWritesRecreatedTable is the #919 regression: an ingester
// holding buffered rows for a dropped-and-recreated table must not flush them
// into the new table's manifest (ADR-0030: commit against the identity read).
func TestReviewOldIngesterWritesRecreatedTable(t *testing.T) {
	s := pqt.Schema{Columns: []pqt.Column{{Name: "id", Type: pqt.TypeInt64}}}
	c := reviewCatalog(t, s)
	ing := New(c, "events", s, nil, DefaultConfig())
	ctx := context.Background()
	if e := ing.Ingest(ctx, []map[string]any{{"id": int64(123)}}); e != nil {
		t.Fatal(e)
	}
	if e := c.DropTable(ctx, "events"); e != nil {
		t.Fatal(e)
	}
	if e := c.CreateTable(ctx, "events", s, nil); e != nil {
		t.Fatal(e)
	}
	e := ing.FlushAll(ctx)
	t.Logf("old ingester flush error: %v", e)
	if rows := reviewRows(t, c); len(rows) != 0 {
		t.Fatalf("old table buffered rows resurrected into new table: %v", rows)
	}
}

// TestReviewOldIngesterRefusedOnIncompatibleRecreate is the #919 parallel: the
// recreated table has an INCOMPATIBLE schema. The stale flush must still be
// refused by incarnation (not by luck of a schema clash), and the new table
// must stay empty. The refusal is reported to the owner.
func TestReviewOldIngesterRefusedOnIncompatibleRecreate(t *testing.T) {
	s := pqt.Schema{Columns: []pqt.Column{{Name: "id", Type: pqt.TypeInt64}}}
	c := reviewCatalog(t, s)
	ing := New(c, "events", s, nil, DefaultConfig())
	ctx := context.Background()
	if e := ing.Ingest(ctx, []map[string]any{{"id": int64(123)}}); e != nil {
		t.Fatal(e)
	}
	if e := c.DropTable(ctx, "events"); e != nil {
		t.Fatal(e)
	}
	// Recreate with a different schema (a string column, not an int).
	s2 := pqt.Schema{Columns: []pqt.Column{{Name: "name", Type: pqt.TypeString}}}
	if e := c.CreateTable(ctx, "events", s2, nil); e != nil {
		t.Fatal(e)
	}
	e := ing.FlushAll(ctx)
	if e == nil {
		t.Fatal("expected the stale flush to be refused, got nil")
	}
	if !errors.Is(e, catalog.ErrTableIncarnationChanged) {
		t.Fatalf("expected ErrTableIncarnationChanged, got %v", e)
	}
	if rows := reviewRows(t, c); len(rows) != 0 {
		t.Fatalf("stale rows written into incompatibly recreated table: %v", rows)
	}
}

// TestReviewFreshIngesterAfterRecreateWrites is the #919 control: a fresh
// ingester created AFTER the recreate binds the new incarnation and writes
// normally — the guard refuses only the stale producer, not every ingest.
func TestReviewFreshIngesterAfterRecreateWrites(t *testing.T) {
	s := pqt.Schema{Columns: []pqt.Column{{Name: "id", Type: pqt.TypeInt64}}}
	c := reviewCatalog(t, s)
	ctx := context.Background()
	if e := c.DropTable(ctx, "events"); e != nil {
		t.Fatal(e)
	}
	if e := c.CreateTable(ctx, "events", s, nil); e != nil {
		t.Fatal(e)
	}
	ing := New(c, "events", s, nil, DefaultConfig())
	if e := ing.Ingest(ctx, []map[string]any{{"id": int64(7)}}); e != nil {
		t.Fatal(e)
	}
	if e := ing.FlushAll(ctx); e != nil {
		t.Fatalf("fresh ingester after recreate must write: %v", e)
	}
	rows := reviewRows(t, c)
	if len(rows) != 1 || rows[0]["id"] != int64(7) {
		t.Fatalf("fresh ingester after recreate wrote wrong rows: %v", rows)
	}
}
