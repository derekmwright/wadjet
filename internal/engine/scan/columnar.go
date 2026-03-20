package scan

import (
	"encoding/binary"
	"fmt"
	"io"

	goparquet "github.com/parquet-go/parquet-go"
	pqencoding "github.com/parquet-go/parquet-go/encoding"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	pqt "github.com/citc-tech/wadjet/internal/storage/parquet"
)

// ReadFileBatches reads all row groups from a Parquet file into separate RecordBatches
// (one per row group). Supports column projection via selectedCols. Falls back to
// row-based reading for schemas containing Decimal, Array, Row, or Map types.
func ReadFileBatches(reader *pqt.Reader, schema []pqt.Column, selectedCols []string) ([]*batch.RecordBatch, error) {
	readSchema := schema
	if len(selectedCols) > 0 {
		readSchema = projectSchema(schema, selectedCols)
	}

	// Schemas with types unsupported by the columnar reader fall back to rows
	if hasUnsupportedColumnarTypes(readSchema) {
		return readFileBatchesViaRows(reader, readSchema, selectedCols)
	}

	pqFile := reader.File()
	rgs := pqFile.RowGroups()

	var batches []*batch.RecordBatch
	for _, rg := range rgs {
		b, err := readRowGroupColumnar(rg, readSchema, pqFile, nil)
		if err != nil {
			return nil, err
		}
		if b != nil {
			batches = append(batches, b)
		}
	}
	return batches, nil
}

// readFileBatchesViaRows falls back to row-based reading for unsupported types.
func readFileBatchesViaRows(reader *pqt.Reader, readSchema []pqt.Column, selectedCols []string) ([]*batch.RecordBatch, error) {
	rows, err := reader.ReadRows(selectedCols)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return []*batch.RecordBatch{batch.FromRows(readSchema, rows)}, nil
}

// projectSchema filters schema to only include columns in selectedCols.
func projectSchema(schema []pqt.Column, selectedCols []string) []pqt.Column {
	needed := make(map[string]bool, len(selectedCols))
	for _, c := range selectedCols {
		needed[c] = true
	}
	filtered := make([]pqt.Column, 0, len(selectedCols))
	for _, col := range schema {
		if needed[col.Name] {
			filtered = append(filtered, col)
		}
	}
	if len(filtered) > 0 {
		return filtered
	}
	return schema
}

// hasUnsupportedColumnarTypes returns true if any column uses a type the
// direct columnar reader cannot handle (Decimal, Array, Row, Map).
func hasUnsupportedColumnarTypes(schema []pqt.Column) bool {
	for _, col := range schema {
		switch col.Type {
		case pqt.TypeDecimal, pqt.TypeArray, pqt.TypeRow, pqt.TypeMap:
			return true
		}
	}
	return false
}

// ReadFileColumnar reads all row groups from a Parquet reader into a single RecordBatch.
// Used by the DML executor to read entire files for DELETE/UPDATE operations.
func ReadFileColumnar(reader *pqt.Reader, schema []pqt.Column) (*batch.RecordBatch, error) {
	pqFile := reader.File()
	rgs := pqFile.RowGroups()

	var batches []*batch.RecordBatch
	for _, rg := range rgs {
		b, err := readRowGroupColumnar(rg, schema, pqFile, nil)
		if err != nil {
			return nil, err
		}
		if b != nil {
			batches = append(batches, b)
		}
	}

	if len(batches) == 0 {
		return nil, nil
	}
	if len(batches) == 1 {
		return batches[0], nil
	}

	totalRows := 0
	for _, b := range batches {
		totalRows += b.Len
	}
	result := batch.NewRecordBatch(schema, totalRows)
	offset := 0
	for _, b := range batches {
		for j := range schema {
			copyVectorRange(result.Columns[j], offset, b.Columns[j], 0, b.Len)
		}
		offset += b.Len
	}
	return result, nil
}

// readRowGroupColumnar reads a parquet row group directly into a RecordBatch
// using typed column access — no map[string]any intermediate.
func readRowGroupColumnar(rg goparquet.RowGroup, schema []pqt.Column, pqFile *goparquet.File, pool *batch.BatchPool) (*batch.RecordBatch, error) {
	numRows := int(rg.NumRows())
	if numRows == 0 {
		return nil, nil
	}

	var b *batch.RecordBatch
	if pool != nil {
		b = pool.GetForSize(numRows)
	} else {
		b = batch.NewRecordBatch(schema, numRows)
	}
	chunks := rg.ColumnChunks()
	pqCols := pqFile.Schema().Columns()

	// Map our schema columns to parquet column indices
	for i, col := range schema {
		pqIdx := findParquetColumn(pqCols, col.Name)
		if pqIdx < 0 {
			// Column not in parquet file — leave as all nulls
			for j := 0; j < numRows; j++ {
				b.Columns[i].Nulls.SetNull(j)
			}
			continue
		}

		pqCol := pqFile.Schema().Columns()[pqIdx]
		maxDefLevel := pqCol[len(pqCol)-1]
		_ = maxDefLevel // We'll get it from the file schema's Column object

		chunk := chunks[pqIdx]
		if err := readColumnChunk(b.Columns[i], chunk, numRows, col.Type, pqFile, pqIdx); err != nil {
			return nil, fmt.Errorf("reading column %s: %w", col.Name, err)
		}
	}

	return b, nil
}

// findParquetColumn returns the index of the column with the given name
// in the parquet file's column paths, or -1 if not found.
func findParquetColumn(pqCols [][]string, name string) int {
	for i, path := range pqCols {
		if len(path) > 0 && path[len(path)-1] == name {
			return i
		}
	}
	return -1
}

// readColumnChunk reads all pages from a column chunk into a Vector.
func readColumnChunk(vec *batch.Vector, chunk goparquet.ColumnChunk, numRows int, typ pqt.TypeID, pqFile *goparquet.File, colIdx int) error {
	pages := chunk.Pages()
	defer pages.Close()

	// Get max definition level for null handling
	pqCol := FindColumnByIndex(pqFile.Root(), colIdx)
	maxDefLevel := 0
	if pqCol != nil {
		maxDefLevel = pqCol.MaxDefinitionLevel()
	}

	offset := 0
	for {
		page, err := pages.ReadPage()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading page: %w", err)
		}

		pageRows := int(page.NumRows())
		if pageRows == 0 {
			continue
		}

		defLevels := page.DefinitionLevels()
		data := page.Data()

		if defLevels == nil || page.NumNulls() == 0 {
			// Non-nullable column or nullable page with no actual nulls — direct copy.
			// Skips per-element definition level checks and uses bulk copy().
			if err := CopyTypedDataDirect(vec, offset, data, pageRows, typ); err != nil {
				return err
			}
		} else {
			// Nullable column with actual nulls — scatter using definition levels
			if err := CopyTypedDataScatter(vec, offset, data, defLevels, byte(maxDefLevel), pageRows, typ); err != nil {
				return err
			}
		}

		offset += pageRows
	}

	return nil
}

// FindColumnByIndex finds the leaf Column at the given index in the schema tree.
func FindColumnByIndex(root *goparquet.Column, idx int) *goparquet.Column {
	var found *goparquet.Column
	var walk func(col *goparquet.Column)
	walk = func(col *goparquet.Column) {
		if col.Leaf() {
			if col.Index() == idx {
				found = col
			}
			return
		}
		for _, child := range col.Columns() {
			walk(child)
			if found != nil {
				return
			}
		}
	}
	walk(root)
	return found
}

// CopyTypedDataDirect copies non-nullable page data directly into a Vector.
func CopyTypedDataDirect(vec *batch.Vector, offset int, data pqencoding.Values, n int, typ pqt.TypeID) error {
	switch typ {
	case pqt.TypeInt64, pqt.TypeTimestamp, pqt.TypeIPv4, pqt.TypeMAC, pqt.TypeDuration:
		src := data.Int64()
		copy(vec.Int64Data[offset:offset+n], src[:n])

	case pqt.TypeInt32, pqt.TypePort, pqt.TypeProtocol, pqt.TypeDate:
		src := data.Int32()
		copy(vec.Int32Data[offset:offset+n], src[:n])

	case pqt.TypeFloat64:
		src := data.Double()
		copy(vec.Float64Data[offset:offset+n], src[:n])

	case pqt.TypeFloat32:
		src := data.Float()
		copy(vec.Float32Data[offset:offset+n], src[:n])

	case pqt.TypeBool:
		boolBytes := data.Boolean()
		for i := 0; i < n; i++ {
			byteIdx := i / 8
			bitIdx := uint(i % 8)
			vec.BoolData[offset+i] = byteIdx < len(boolBytes) && (boolBytes[byteIdx]&(1<<bitIdx)) != 0
		}

	case pqt.TypeString, pqt.TypeBytes, pqt.TypeIPv6, pqt.TypeCIDR, pqt.TypeUUID:
		rawData, offsets := data.ByteArray()
		if offsets != nil {
			// Bulk copy: single append of contiguous data + offset arithmetic.
			// Replaces n individual Set calls, reducing memmove overhead.
			vec.BytesData.BulkSet(offset, rawData, offsets, n)
		} else {
			// PLAIN encoding: 4-byte length prefix per value
			pos := 0
			for i := 0; i < n; i++ {
				if pos+4 > len(rawData) {
					break
				}
				length := int(binary.LittleEndian.Uint32(rawData[pos:]))
				pos += 4
				if pos+length > len(rawData) {
					break
				}
				vec.BytesData.Set(offset+i, rawData[pos:pos+length])
				pos += length
			}
		}
	}
	return nil
}

// CopyTypedDataScatter copies nullable page data into a Vector, scattering
// values according to definition levels and setting nulls in the bitmap.
func CopyTypedDataScatter(vec *batch.Vector, offset int, data pqencoding.Values, defLevels []byte, maxDefLevel byte, n int, typ pqt.TypeID) error {
	switch typ {
	case pqt.TypeInt64, pqt.TypeTimestamp, pqt.TypeIPv4, pqt.TypeMAC, pqt.TypeDuration:
		src := data.Int64()
		valIdx := 0
		for i := 0; i < n; i++ {
			if defLevels[i] == maxDefLevel {
				vec.Int64Data[offset+i] = src[valIdx]
				valIdx++
			} else {
				vec.Nulls.SetNull(offset + i)
			}
		}

	case pqt.TypeInt32, pqt.TypePort, pqt.TypeProtocol, pqt.TypeDate:
		src := data.Int32()
		valIdx := 0
		for i := 0; i < n; i++ {
			if defLevels[i] == maxDefLevel {
				vec.Int32Data[offset+i] = src[valIdx]
				valIdx++
			} else {
				vec.Nulls.SetNull(offset + i)
			}
		}

	case pqt.TypeFloat64:
		src := data.Double()
		valIdx := 0
		for i := 0; i < n; i++ {
			if defLevels[i] == maxDefLevel {
				vec.Float64Data[offset+i] = src[valIdx]
				valIdx++
			} else {
				vec.Nulls.SetNull(offset + i)
			}
		}

	case pqt.TypeFloat32:
		src := data.Float()
		valIdx := 0
		for i := 0; i < n; i++ {
			if defLevels[i] == maxDefLevel {
				vec.Float32Data[offset+i] = src[valIdx]
				valIdx++
			} else {
				vec.Nulls.SetNull(offset + i)
			}
		}

	case pqt.TypeBool:
		boolBytes := data.Boolean()
		valIdx := 0
		for i := 0; i < n; i++ {
			if defLevels[i] == maxDefLevel {
				byteIdx := valIdx / 8
				bitIdx := uint(valIdx % 8)
				vec.BoolData[offset+i] = byteIdx < len(boolBytes) && (boolBytes[byteIdx]&(1<<bitIdx)) != 0
				valIdx++
			} else {
				vec.Nulls.SetNull(offset + i)
			}
		}

	case pqt.TypeString, pqt.TypeBytes, pqt.TypeIPv6, pqt.TypeCIDR, pqt.TypeUUID:
		rawData, offsets := data.ByteArray()
		valIdx := 0
		if offsets != nil {
			for i := 0; i < n; i++ {
				if defLevels[i] == maxDefLevel {
					start := offsets[valIdx]
					end := offsets[valIdx+1]
					vec.BytesData.Set(offset+i, rawData[start:end])
					valIdx++
				} else {
					vec.Nulls.SetNull(offset + i)
					vec.BytesData.Set(offset+i, nil)
				}
			}
		} else {
			// PLAIN encoding fallback
			pos := 0
			for i := 0; i < n; i++ {
				if defLevels[i] == maxDefLevel {
					if pos+4 > len(rawData) {
						break
					}
					length := int(binary.LittleEndian.Uint32(rawData[pos:]))
					pos += 4
					vec.BytesData.Set(offset+i, rawData[pos:pos+length])
					pos += length
					valIdx++
				} else {
					vec.Nulls.SetNull(offset + i)
					vec.BytesData.Set(offset+i, nil)
				}
			}
		}
	}
	return nil
}
