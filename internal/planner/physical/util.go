package physical

import (
	"bytes"
	"io"

	goparquet "github.com/parquet-go/parquet-go"

	"github.com/derekmwright/caelum/internal/engine/batch"
	"github.com/derekmwright/caelum/internal/storage/parquet"
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

// readBatchDirect reads a parquet file directly into a RecordBatch, bypassing
// the expensive map[string]any intermediate. Uses parquet-go's batch Row API
// for efficient columnar decoding.
func readBatchDirect(pqReader *parquet.Reader, schema []parquet.Column, requiredCols []string) *batch.RecordBatch {
	file := pqReader.File()

	// Build projected schema for column pruning
	var opts []goparquet.ReaderOption
	readSchema := schema
	if len(requiredCols) > 0 {
		projected := buildProjectedParquetSchema(file, requiredCols)
		if projected != nil {
			opts = append(opts, projected)
		}
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

	pr := goparquet.NewReader(file, opts...)
	defer pr.Close()

	numRows := int(pr.NumRows())
	if numRows == 0 {
		return nil
	}

	// Map projected parquet columns to batch column indices
	pqCols := pr.Schema().Columns()
	colMap := make([]int, len(pqCols))
	for i, path := range pqCols {
		name := path[len(path)-1]
		colMap[i] = -1
		for j, sc := range readSchema {
			if sc.Name == name {
				colMap[i] = j
				break
			}
		}
	}

	b := batch.NewRecordBatch(readSchema, numRows)

	bufSize := 4096
	if numRows < bufSize {
		bufSize = numRows
	}
	buf := make([]goparquet.Row, bufSize)
	rowIdx := 0
	for rowIdx < numRows {
		remaining := numRows - rowIdx
		if remaining < len(buf) {
			buf = buf[:remaining]
		}
		n, err := pr.ReadRows(buf)
		for i := 0; i < n; i++ {
			for j, val := range buf[i] {
				if j >= len(colMap) {
					break
				}
				batchCol := colMap[j]
				if batchCol < 0 {
					continue
				}
				setValueDirect(b.Columns[batchCol], rowIdx, val)
			}
			rowIdx++
		}
		if err != nil || n == 0 {
			break
		}
	}

	b.Len = rowIdx
	return b
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
			// Sign-extend lo to handle negative values
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
