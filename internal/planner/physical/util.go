package physical

import (
	"bytes"
	"io"
	"strings"

	goparquet "github.com/parquet-go/parquet-go"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/exec"
	"github.com/citc-tech/wadjet/internal/engine/scan"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

func readAll(rc io.ReadCloser) ([]byte, error) {
	return io.ReadAll(rc)
}

func bytesReader(data []byte) *bytes.Reader {
	return bytes.NewReader(data)
}

func fromRows(schema []parquet.Column, rows []map[string]any) *batch.RecordBatch {
	return batch.FromRows(schema, rows)
}

// readBatchDirect reads a parquet file directly into a RecordBatch using
// column-at-a-time page reading. This avoids the row-reconstruction overhead
// of parquet-go's ReadRows and eliminates the map[string]any intermediate.
// For schemas with nested types (ARRAY, ROW, MAP), falls back to row-level reading.
func readBatchDirect(pqReader *parquet.Reader, schema []parquet.Column, requiredCols []string, preds ...scanPredicate) *batch.RecordBatch {
	// Nested types require rep/def level decoding — fall back to row reader
	pqSchema := parquet.Schema{Columns: schema}
	if pqSchema.HasNestedColumns() {
		return readBatchViaRows(pqReader, schema, requiredCols)
	}

	file := pqReader.File()
	rowGroups := file.RowGroups()
	if len(rowGroups) == 0 {
		return nil
	}

	// Determine which columns to read and build the batch schema
	readSchema := schema
	if len(requiredCols) > 0 {
		needed := make(map[string]bool, len(requiredCols))
		for _, c := range requiredCols {
			needed[c] = true
		}
		filtered := make([]parquet.Column, 0, len(requiredCols))
		for _, col := range schema {
			if needed[col.Name] {
				filtered = append(filtered, col)
			}
		}
		if len(filtered) > 0 {
			readSchema = filtered
		}
	}

	// Map batch schema columns to parquet file column indices
	fileColumns := file.Schema().Columns() // [][]string
	type colMapping struct {
		fileIdx  int // index in parquet file's column chunks
		batchIdx int // index in our batch schema
	}
	mappings := make([]colMapping, 0, len(readSchema))
	for bi, sc := range readSchema {
		for fi, path := range fileColumns {
			name := path[len(path)-1]
			if name == sc.Name {
				mappings = append(mappings, colMapping{fileIdx: fi, batchIdx: bi})
				break
			}
		}
	}

	// Filter row groups using stats-based predicate pruning
	activeRGs := rowGroups
	if len(preds) > 0 {
		activeRGs = make([]goparquet.RowGroup, 0, len(rowGroups))
		for i, rg := range rowGroups {
			stats := pqReader.RowGroupStats(i)
			pruned := false
			for _, pred := range preds {
				op := mapPredOp(pred.Op)
				if op >= 0 {
					sp := scan.StatsPredicate{Column: pred.Column, Op: op, Value: pred.Value}
					if scan.CanPruneRowGroup(sp, stats) {
						pruned = true
						break
					}
				}
			}
			if !pruned {
				activeRGs = append(activeRGs, rg)
			}
		}
	}

	// Count total rows across active row groups
	var totalRows int64
	for _, rg := range activeRGs {
		totalRows += rg.NumRows()
	}
	if totalRows == 0 {
		return nil
	}

	b := batch.NewRecordBatch(readSchema, int(totalRows))
	valBuf := make([]goparquet.Value, 4096)

	// Read column-by-column across active row groups
	for _, m := range mappings {
		col := b.Columns[m.batchIdx]
		rowOffset := 0

		for _, rg := range activeRGs {
			chunks := rg.ColumnChunks()
			if m.fileIdx >= len(chunks) {
				continue
			}
			pages := chunks[m.fileIdx].Pages()
			for {
				page, err := pages.ReadPage()
				if err != nil || page == nil {
					break
				}

				vr := page.Values()
				for {
					n, err := vr.ReadValues(valBuf)
					for i := 0; i < n; i++ {
						setValueDirect(col, rowOffset+i, valBuf[i])
					}
					rowOffset += n
					if err != nil || n == 0 {
						break
					}
				}
				if cl, ok := vr.(io.Closer); ok {
					cl.Close()
				}
			}
			pages.Close()
		}
	}

	b.Len = int(totalRows)
	return b
}

// readBatchViaRows reads a parquet file using parquet-go's row-level reader,
// which handles nested types (LIST, MAP, STRUCT) natively. Used as fallback
// when the schema contains nested columns.
func readBatchViaRows(pqReader *parquet.Reader, schema []parquet.Column, requiredCols []string) *batch.RecordBatch {
	selectedCols := requiredCols
	if len(selectedCols) == 0 {
		selectedCols = make([]string, len(schema))
		for i, c := range schema {
			selectedCols[i] = c.Name
		}
	}
	rows, err := pqReader.ReadRows(selectedCols)
	if err != nil || len(rows) == 0 {
		return nil
	}

	// Build the read schema (only columns we're reading)
	readSchema := schema
	if len(requiredCols) > 0 {
		needed := make(map[string]bool, len(requiredCols))
		for _, c := range requiredCols {
			needed[c] = true
		}
		filtered := make([]parquet.Column, 0, len(requiredCols))
		for _, col := range schema {
			if needed[col.Name] {
				filtered = append(filtered, col)
			}
		}
		if len(filtered) > 0 {
			readSchema = filtered
		}
	}

	return fromRows(readSchema, rows)
}

// mapPredOp converts a logical predicate operator string to an exec.CompareOp.
func mapPredOp(op string) exec.CompareOp {
	switch strings.ToLower(op) {
	case "=":
		return exec.OpEq
	case "!=", "<>":
		return exec.OpNe
	case "<":
		return exec.OpLt
	case "<=":
		return exec.OpLe
	case ">":
		return exec.OpGt
	case ">=":
		return exec.OpGe
	default:
		return -1
	}
}

// buildProjectedParquetSchema creates a parquet-go schema with only the requested columns.
func buildProjectedParquetSchema(file *goparquet.File, selectedColumns []string) *goparquet.Schema {
	fileSchema := file.Schema()
	fields := fileSchema.Fields()

	needed := make(map[string]bool, len(selectedColumns))
	for _, c := range selectedColumns {
		needed[c] = true
	}

	group := make(goparquet.Group)
	for _, f := range fields {
		if needed[f.Name()] {
			group[f.Name()] = f
		}
	}

	if len(group) == 0 {
		return nil
	}
	return goparquet.NewSchema(fileSchema.Name(), group)
}

// setValueDirect writes a parquet Value directly to a batch Vector at the given index,
// using typed accessors to avoid the interface{} overhead of SetValue.
func setValueDirect(col *batch.Vector, idx int, val goparquet.Value) {
	if val.IsNull() {
		col.Nulls.SetNull(idx)
		switch col.Type {
		case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
			col.BytesData.Set(idx, nil)
		}
		return
	}
	col.Nulls.SetValid(idx)
	switch col.Type {
	case batch.TypeBool:
		col.BoolData[idx] = val.Boolean()
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		col.Int32Data[idx] = val.Int32()
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		col.Int64Data[idx] = val.Int64()
	case batch.TypeFloat32:
		col.Float32Data[idx] = val.Float()
	case batch.TypeFloat64:
		col.Float64Data[idx] = val.Double()
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
		col.BytesData.Set(idx, val.ByteArray())
	case batch.TypeDecimal:
		switch val.Kind() {
		case goparquet.Int32:
			col.DecimalData.Data[idx] = batch.Int128From(int64(val.Int32()))
		case goparquet.Int64:
			col.DecimalData.Data[idx] = batch.Int128From(val.Int64())
		case goparquet.FixedLenByteArray, goparquet.ByteArray:
			b := val.ByteArray()
			col.DecimalData.Data[idx] = decimalFromBytes(b)
		}
	}
}

// decimalFromBytes converts big-endian bytes to Int128.
func decimalFromBytes(b []byte) batch.Int128 {
	if len(b) == 0 {
		return batch.Int128{}
	}
	// Parquet stores decimals as big-endian two's complement
	var hi int64
	var lo uint64
	if len(b) <= 8 {
		// Fits in a single int64 — sign-extend
		if b[0]&0x80 != 0 {
			hi = -1
		}
		for _, c := range b {
			lo = (lo << 8) | uint64(c)
		}
		if hi < 0 {
			lo |= ^uint64(0) << (uint(len(b)) * 8)
		}
		return batch.Int128From(int64(lo))
	}
	// 9-16 bytes: split into hi/lo
	split := len(b) - 8
	hiBytes := b[:split]
	loBytes := b[split:]
	if hiBytes[0]&0x80 != 0 {
		hi = -1
	}
	for _, c := range hiBytes {
		hi = (hi << 8) | int64(c)
	}
	for _, c := range loBytes {
		lo = (lo << 8) | uint64(c)
	}
	return batch.Int128{Hi: hi, Lo: lo}
}
