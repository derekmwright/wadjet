package worker

import (
	"context"
	"sort"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/exec"
	"github.com/citc-tech/wadjet/internal/engine/scan"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

func newTestStore(t *testing.T, bucket string) *objstore.MemStore {
	t.Helper()
	store := objstore.NewMemStore()
	if err := store.MakeBucket(context.Background(), bucket); err != nil {
		t.Fatal(err)
	}
	return store
}

func schemaFromRows(rows []map[string]any) []parquet.Column {
	if len(rows) == 0 {
		return nil
	}
	names := make([]string, 0, len(rows[0]))
	for k := range rows[0] {
		names = append(names, k)
	}
	sort.Strings(names)
	cols := make([]parquet.Column, len(names))
	for i, k := range names {
		v := rows[0][k]
		col := parquet.Column{Name: k, Nullable: true}
		switch v.(type) {
		case bool:
			col.Type = parquet.TypeBool
		case int32:
			col.Type = parquet.TypeInt32
		case int64, int:
			col.Type = parquet.TypeInt64
		case float32:
			col.Type = parquet.TypeFloat32
		case float64:
			col.Type = parquet.TypeFloat64
		case string:
			col.Type = parquet.TypeString
		case []byte:
			col.Type = parquet.TypeBytes
		default:
			col.Type = parquet.TypeString
		}
		cols[i] = col
	}
	return cols
}

func applyRuntimeFilter(b *batch.RecordBatch, ranges []exec.DynamicRange) *batch.RecordBatch {
	if b == nil || b.ActiveLen() == 0 {
		return b
	}
	n := b.ActiveLen()
	var sel []uint32
	for ri := 0; ri < n; ri++ {
		row := ri
		if b.Sel != nil {
			row = int(b.Sel[ri])
		}
		pass := true
		for _, dr := range ranges {
			ci := b.ColumnIndex(dr.Column)
			if ci < 0 {
				continue
			}
			v := b.Columns[ci]
			if v.Nulls.IsNullFast(row) {
				pass = false
				break
			}
			val := v.GetValue(row)
			if scan.CompareValues(val, dr.MinValue) < 0 || scan.CompareValues(val, dr.MaxValue) > 0 {
				pass = false
				break
			}
		}
		if pass {
			sel = append(sel, uint32(row))
		}
	}
	b.Sel = sel
	return b
}
