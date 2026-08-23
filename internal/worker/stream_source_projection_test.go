package worker

import (
	"bytes"
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// projTestSchema mirrors the shape #410 was found on: flat columns beside a
// container column the query never mentions. The scanner picks its decode
// path from the columns the read ASKS FOR, so whether the container column
// is in the read set decides how the whole file is decoded.
var projTestSchema = parquet.Schema{Columns: []parquet.Column{
	{Name: "id", Type: parquet.TypeInt64},
	{Name: "g", Type: parquet.TypeInt32, Nullable: true},
	{Name: "c_arr", Type: parquet.TypeArray, Nullable: true,
		ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}},
	{Name: "payload", Type: parquet.TypeString, Nullable: true},
}}

func writeProjTestFile(t *testing.T, store objstore.Store, bucket, path string, rows int) {
	t.Helper()
	ctx := context.Background()
	var buf bytes.Buffer
	w, err := parquet.NewWriter(&buf, projTestSchema, parquet.DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	data := make([]map[string]any, rows)
	for i := range data {
		data[i] = map[string]any{
			"id":      int64(i),
			"g":       int32(i % 3),
			"c_arr":   []any{"a", "b"},
			"payload": "row",
		}
	}
	if err := w.WriteRows(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, bucket, path, bytes.NewReader(buf.Bytes()), int64(buf.Len()), ""); err != nil {
		t.Fatal(err)
	}
}

// TestCachedFileStreamSourceProjection is the worker half of #410: the DAG's
// fragment source honours OpSpec.Columns, so a stage that asks for three
// columns reads three columns. Before the coordinator started sending the
// projection this path was correct and simply never exercised from the DAG —
// spec.Columns was empty on every OpShuffleSource, and the base-table read
// behind it ran at full width.
func TestCachedFileStreamSourceProjection(t *testing.T) {
	ctx := context.Background()
	ex, store := newConsumer(t, "b")
	writeProjTestFile(t, store, "b", "tables/t/chunk_0.parquet", 64)
	files := []string{"tables/t/chunk_0.parquet"}

	names := func(src *cachedFileStreamSource) ([]string, int) {
		t.Helper()
		if err := src.Init(ctx); err != nil {
			t.Fatal(err)
		}
		defer src.Close()
		var cols []string
		rows := 0
		for {
			b, err := src.Next(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if b == nil {
				return cols, rows
			}
			rows += b.Len
			if cols == nil {
				for _, c := range b.Schema {
					cols = append(cols, c.Name)
				}
			}
		}
	}

	t.Run("projection narrows the read", func(t *testing.T) {
		cols, rows := names(newCachedFileStreamSourceWithProjection(ex, "", "b", files, []string{"id", "g"}))
		if rows != 64 {
			t.Fatalf("rows = %d, want 64", rows)
		}
		if len(cols) != 2 || cols[0] != "id" || cols[1] != "g" {
			t.Fatalf("columns = %v, want [id g] — the container column was read anyway", cols)
		}
	})

	t.Run("no projection reads the whole file", func(t *testing.T) {
		cols, rows := names(newCachedFileStreamSource(ex, "", "b", files))
		if rows != 64 {
			t.Fatalf("rows = %d, want 64", rows)
		}
		if len(cols) != len(projTestSchema.Columns) {
			t.Fatalf("columns = %v, want all %d", cols, len(projTestSchema.Columns))
		}
	})

	t.Run("an unknown name reverts to full width", func(t *testing.T) {
		// All-or-nothing: a derived name the plan carries but the file has
		// no column for must not over-prune the scan.
		cols, rows := names(newCachedFileStreamSourceWithProjection(ex, "", "b", files,
			[]string{"id", "sum(payload)"}))
		if rows != 64 {
			t.Fatalf("rows = %d, want 64", rows)
		}
		if len(cols) != len(projTestSchema.Columns) {
			t.Fatalf("columns = %v, want all %d after the guard declined", cols, len(projTestSchema.Columns))
		}
	})
}

// TestFragmentSourceAppliesSpecColumns pins the seam itself: OpSpec.Columns
// reaches the source. The coordinator now fills that field for base-table
// inputs (applySourceProjection), and this is the receiving end.
func TestFragmentSourceAppliesSpecColumns(t *testing.T) {
	ctx := context.Background()
	ex, store := newConsumer(t, "b")
	writeProjTestFile(t, store, "b", "tables/t/chunk_0.parquet", 32)

	task := distributed.Task{QueryID: "q", DataBucket: "b"}
	spec := distributed.OpSpec{
		Type:       distributed.OpShuffleSource,
		InputAlias: "scan-0",
		InputFiles: []string{"tables/t/chunk_0.parquet"},
		Columns:    []string{"id", "payload"},
	}
	src, err := ex.buildFragmentSource(task, spec)
	if err != nil {
		t.Fatal(err)
	}
	cfs, ok := src.(*cachedFileStreamSource)
	if !ok {
		t.Fatalf("source is %T, not the cached file stream source", src)
	}
	if err := cfs.Init(ctx); err != nil {
		t.Fatal(err)
	}
	defer cfs.Close()
	b, err := cfs.Next(ctx)
	if err != nil || b == nil {
		t.Fatalf("first batch: %v %v", b, err)
	}
	if len(b.Schema) != 2 || b.Schema[0].Name != "id" || b.Schema[1].Name != "payload" {
		var got []string
		for _, c := range b.Schema {
			got = append(got, c.Name)
		}
		t.Fatalf("fragment source read %v, want [id payload]", got)
	}
}
