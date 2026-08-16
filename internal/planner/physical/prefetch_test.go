package physical

import (
	"bytes"
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// writeTestParquetMultiRG writes row sets to a parquet file with small row groups
// so that multiple row groups are produced.
func writeTestParquetMultiRG(t *testing.T, schema parquet.Schema, rowSets ...[]map[string]any) []byte {
	t.Helper()

	cfg := parquet.DefaultWriterConfig()
	cfg.Compression = parquet.CompressionNone
	// Set small row group size so each rowSet becomes its own row group.
	if len(rowSets) > 0 {
		cfg.RowGroupSize = len(rowSets[0])
	}

	var buf bytes.Buffer
	w, err := parquet.NewWriter(&buf, schema, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, rows := range rowSets {
		if err := w.WriteRows(rows); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func openNativeReader(t *testing.T, data []byte) *parquet.FileReader {
	t.Helper()
	fr, err := parquet.OpenFileReaderFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	return fr
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
	fr := openNativeReader(t, data)
	numRGs := fr.NumRowGroups()
	if numRGs < 2 {
		t.Fatalf("expected at least 2 row groups, got %d", numRGs)
	}

	units := make([]rgUnit, numRGs)
	var totalRows int64
	for i := 0; i < numRGs; i++ {
		rgRows := fr.RowGroupNumRows(i)
		units[i] = rgUnit{slot: newPreloadedFileSlot(catalog.FileEntry{}, fr), rgIndex: i, rgRowOffset: totalRows, numRows: rgRows}
		totalRows += rgRows
	}

	batchSchema := schema.Columns
	readSchema := buildReadSchema(batchSchema, nil)
	rgSize := int(units[0].numRows)

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
	fr := openNativeReader(t, data)
	numRGs := fr.NumRowGroups()
	if numRGs < 3 {
		t.Fatalf("expected at least 3 row groups, got %d", numRGs)
	}

	units := make([]rgUnit, numRGs)
	var totalRows int64
	for i := 0; i < numRGs; i++ {
		rgRows := fr.RowGroupNumRows(i)
		units[i] = rgUnit{slot: newPreloadedFileSlot(catalog.FileEntry{}, fr), rgIndex: i, rgRowOffset: totalRows, numRows: rgRows}
		totalRows += rgRows
	}

	batchSchema := schema.Columns
	readSchema := buildReadSchema(batchSchema, nil)
	rgSize := int(units[0].numRows)

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
			gotRows, totalRows, workers, numRGs)
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
	fr := openNativeReader(t, data)
	numRGs := fr.NumRowGroups()

	units := make([]rgUnit, numRGs)
	for i := 0; i < numRGs; i++ {
		units[i] = rgUnit{slot: newPreloadedFileSlot(catalog.FileEntry{}, fr), rgIndex: i, numRows: fr.RowGroupNumRows(i)}
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
	fr := openNativeReader(t, data)
	numRGs := fr.NumRowGroups()
	if numRGs == 0 {
		t.Fatal("no row groups produced")
	}

	filePath := "test/file.parquet"
	deleteSet := map[int64]bool{0: true, 1: true}

	units := make([]rgUnit, numRGs)
	var totalRows int64
	for i := 0; i < numRGs; i++ {
		rgRows := fr.RowGroupNumRows(i)
		units[i] = rgUnit{
			slot:        newPreloadedFileSlot(catalog.FileEntry{Path: filePath}, fr),
			rgIndex:     i,
			rgRowOffset: totalRows,
			numRows:     rgRows,
		}
		totalRows += rgRows
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
