package worker

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/wshf"
)

// makeScanWshf writes a .wshf payload with schema (id int64, val int64) for
// use as a synthetic scan input.
func makeScanWshf(t *testing.T, rows [][2]int64) []byte {
	t.Helper()
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: true},
		{Name: "val", Type: parquet.TypeInt64, Nullable: true},
	}
	var buf bytes.Buffer
	sw := newShuffleWriter(&buf, schema)
	if err := sw.writeHeader(); err != nil {
		t.Fatalf("writeHeader: %v", err)
	}
	b := batch.NewRecordBatch(schema, len(rows))
	for i, r := range rows {
		b.Columns[0].Int64Data[i] = r[0]
		b.Columns[0].Nulls.SetValid(i)
		b.Columns[1].Int64Data[i] = r[1]
		b.Columns[1].Nulls.SetValid(i)
	}
	if err := sw.writeChunk(b.Columns, nil, len(rows)); err != nil {
		t.Fatalf("writeChunk: %v", err)
	}
	data := buf.Bytes()
	data[4] = byte(sw.numChunks)
	return data
}

// readMemStoreInts reads a WSHF blob from MemStore at (bucket,key) and
// returns the int64 values from the named column.
func readMemStoreInts(t *testing.T, store *objstore.MemStore, bucket, key, colName string) []int64 {
	t.Helper()
	rc, _, err := store.Get(context.Background(), bucket, key)
	if err != nil {
		t.Fatalf("readMemStoreInts: get %s/%s: %v", bucket, key, err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("readMemStoreInts: read %s/%s: %v", bucket, key, err)
	}
	if len(data) == 0 {
		return nil
	}
	r, err := wshf.NewChunkReader(data)
	if err != nil {
		t.Fatalf("readMemStoreInts: parse %s/%s: %v", bucket, key, err)
	}
	colIdx := -1
	for i, col := range r.Schema() {
		if col.Name == colName {
			colIdx = i
			break
		}
	}
	if colIdx < 0 {
		t.Fatalf("readMemStoreInts: column %q not in schema for %s/%s", colName, bucket, key)
	}
	var out []int64
	for {
		b, err := r.Next()
		if err != nil {
			t.Fatalf("readMemStoreInts: chunk: %v", err)
		}
		if b == nil {
			break
		}
		vec := b.Columns[colIdx]
		for i := 0; i < b.ActiveLen(); i++ {
			row := i
			if b.Sel != nil {
				row = int(b.Sel[i])
			}
			out = append(out, vec.Int64Data[row])
		}
	}
	return out
}
