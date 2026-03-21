package physical

import (
	"bytes"
	"context"
	"testing"

	goparquet "github.com/parquet-go/parquet-go"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// writeTestParquetMultiRG writes row sets to separate parquet row groups.
func writeTestParquetMultiRG(t *testing.T, schema parquet.Schema, rowSets ...[]map[string]any) []byte {
	t.Helper()
	group := make(goparquet.Group)
	for _, col := range schema.Columns {
		switch col.Type {
		case parquet.TypeInt64:
			group[col.Name] = goparquet.Int(64)
		case parquet.TypeString:
			group[col.Name] = goparquet.String()
		default:
			t.Fatalf("unsupported test type: %v", col.Type)
		}
	}
	pqSchema := goparquet.NewSchema("test", group)

	var buf bytes.Buffer
	pw := goparquet.NewGenericWriter[map[string]any](&buf, pqSchema)
	for _, rows := range rowSets {
		if _, err := pw.Write(rows); err != nil {
			t.Fatal(err)
		}
		if err := pw.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func openParquetFile(t *testing.T, data []byte) *goparquet.File {
	t.Helper()
	r := bytes.NewReader(data)
	f, err := goparquet.OpenFile(r, int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestRGWorker_WithPrefetch(t *testing.T) {
	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "value", Type: parquet.TypeString},
		},
	}

	rg1 := make([]map[string]any, 10)
	rg2 := make([]map[string]any, 10)
	rg3 := make([]map[string]any, 10)
	for i := 0; i < 10; i++ {
		rg1[i] = map[string]any{"id": int64(i), "value": "rg1"}
		rg2[i] = map[string]any{"id": int64(10 + i), "value": "rg2"}
		rg3[i] = map[string]any{"id": int64(20 + i), "value": "rg3"}
	}

	data := writeTestParquetMultiRG(t, schema, rg1, rg2, rg3)
	pqFile := openParquetFile(t, data)
	rgs := pqFile.RowGroups()
	if len(rgs) < 2 {
		t.Fatalf("expected at least 2 row groups, got %d", len(rgs))
	}

	units := make([]rgUnit, len(rgs))
	var totalRows int64
	for i, rg := range rgs {
		units[i] = rgUnit{pqFile: pqFile, rg: rg, rgRowOffset: totalRows}
		totalRows += rg.NumRows()
	}

	batchSchema := schema.Columns
	readSchema := buildReadSchema(batchSchema, nil)
	rgSize := int(rgs[0].NumRows())

	inner := &scanSourceInner{
		schema:  batchSchema,
		rgUnits: units,
		batchCh: make(chan *batch.RecordBatch, len(units)+1),
		errCh:   make(chan error, 1),
	}
	if rgSize > 0 && len(readSchema) > 0 {
		inner.pool = batch.NewBatchPool(readSchema, rgSize)
	}

	ctx := context.Background()
	inner.wg.Add(1)
	go inner.rgWorker(ctx)
	inner.wg.Wait()
	close(inner.batchCh)

	var gotRows int64
	var batchCount int
	for b := range inner.batchCh {
		if b != nil {
			gotRows += int64(b.ActiveLen())
			batchCount++
		}
	}

	if gotRows != totalRows {
		t.Errorf("total rows: got %d, want %d", gotRows, totalRows)
	}
	if batchCount == 0 {
		t.Error("no batches produced")
	}
}

func TestRGWorker_MultipleWorkers(t *testing.T) {
	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
		},
	}

	rowSets := make([][]map[string]any, 6)
	for s := 0; s < 6; s++ {
		rowSets[s] = make([]map[string]any, 5)
		for i := 0; i < 5; i++ {
			rowSets[s][i] = map[string]any{"id": int64(s*5 + i)}
		}
	}

	data := writeTestParquetMultiRG(t, schema, rowSets...)
	pqFile := openParquetFile(t, data)
	rgs := pqFile.RowGroups()
	if len(rgs) < 3 {
		t.Fatalf("expected at least 3 row groups, got %d", len(rgs))
	}

	units := make([]rgUnit, len(rgs))
	var totalRows int64
	for i, rg := range rgs {
		units[i] = rgUnit{pqFile: pqFile, rg: rg, rgRowOffset: totalRows}
		totalRows += rg.NumRows()
	}

	batchSchema := schema.Columns
	readSchema := buildReadSchema(batchSchema, nil)
	rgSize := int(rgs[0].NumRows())

	inner := &scanSourceInner{
		schema:  batchSchema,
		rgUnits: units,
		batchCh: make(chan *batch.RecordBatch, len(units)*2),
		errCh:   make(chan error, 1),
	}
	if rgSize > 0 && len(readSchema) > 0 {
		inner.pool = batch.NewBatchPool(readSchema, rgSize)
	}

	workers := 3
	if workers > len(units) {
		workers = len(units)
	}
	inner.wg.Add(workers)
	ctx := context.Background()
	for i := 0; i < workers; i++ {
		go inner.rgWorker(ctx)
	}
	inner.wg.Wait()
	close(inner.batchCh)

	var gotRows int64
	for b := range inner.batchCh {
		if b != nil {
			gotRows += int64(b.ActiveLen())
		}
	}

	if gotRows != totalRows {
		t.Errorf("total rows: got %d, want %d (with %d workers, %d RGs)",
			gotRows, totalRows, workers, len(rgs))
	}
}

func TestRGWorker_ContextCancellation(t *testing.T) {
	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
		},
	}

	rg1 := make([]map[string]any, 10)
	rg2 := make([]map[string]any, 10)
	for i := 0; i < 10; i++ {
		rg1[i] = map[string]any{"id": int64(i)}
		rg2[i] = map[string]any{"id": int64(10 + i)}
	}

	data := writeTestParquetMultiRG(t, schema, rg1, rg2)
	pqFile := openParquetFile(t, data)
	rgs := pqFile.RowGroups()

	units := make([]rgUnit, len(rgs))
	for i, rg := range rgs {
		units[i] = rgUnit{pqFile: pqFile, rg: rg}
	}

	inner := &scanSourceInner{
		schema:  schema.Columns,
		rgUnits: units,
		batchCh: make(chan *batch.RecordBatch, 1),
		errCh:   make(chan error, 1),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	inner.wg.Add(1)
	go inner.rgWorker(ctx)
	inner.wg.Wait()
	close(inner.batchCh)

	var count int
	for range inner.batchCh {
		count++
	}
	_ = count
}

func TestRGWorker_DeleteMarkers(t *testing.T) {
	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
		},
	}

	rg1 := make([]map[string]any, 10)
	rg2 := make([]map[string]any, 10)
	for i := 0; i < 10; i++ {
		rg1[i] = map[string]any{"id": int64(i)}
		rg2[i] = map[string]any{"id": int64(10 + i)}
	}

	data := writeTestParquetMultiRG(t, schema, rg1, rg2)
	pqFile := openParquetFile(t, data)
	rgs := pqFile.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups produced")
	}

	filePath := "test/file.parquet"
	deleteSet := map[int64]bool{0: true, 1: true}

	units := make([]rgUnit, len(rgs))
	var totalRows int64
	for i, rg := range rgs {
		units[i] = rgUnit{
			pqFile:      pqFile,
			fileEntry:   catalog.FileEntry{Path: filePath},
			rg:          rg,
			rgRowOffset: totalRows,
		}
		totalRows += rg.NumRows()
	}

	inner := &scanSourceInner{
		schema:        schema.Columns,
		rgUnits:       units,
		deleteMarkers: map[string]map[int64]bool{filePath: deleteSet},
		batchCh:       make(chan *batch.RecordBatch, len(units)+1),
		errCh:         make(chan error, 1),
	}

	ctx := context.Background()
	inner.wg.Add(1)
	go inner.rgWorker(ctx)
	inner.wg.Wait()
	close(inner.batchCh)

	var gotActiveRows int64
	for b := range inner.batchCh {
		if b != nil {
			gotActiveRows += int64(b.ActiveLen())
		}
	}

	expectedRows := totalRows - int64(len(deleteSet))
	if gotActiveRows != expectedRows {
		t.Errorf("active rows: got %d, want %d (deleted %d from %d total)",
			gotActiveRows, expectedRows, len(deleteSet), totalRows)
	}
}
