package scan

import (
	"encoding/binary"
	"fmt"
	"io"
	"strings"

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
// Handles qualified name mismatches in both directions:
//   - schema "n2.n_nationkey" matches needed "n_nationkey" (strip schema prefix)
//   - schema "n_name" matches needed "n1.n_name" (strip needed prefix)
func projectSchema(schema []pqt.Column, selectedCols []string) []pqt.Column {
	needed := make(map[string]bool, len(selectedCols))
	// Also index unqualified suffixes of qualified needed names so that
	// schema column "n_name" matches needed "n1.n_name".
	neededSuffix := make(map[string]bool, len(selectedCols))
	for _, c := range selectedCols {
		lc := strings.ToLower(c)
		needed[lc] = true
		if dotIdx := strings.LastIndex(lc, "."); dotIdx >= 0 {
			neededSuffix[lc[dotIdx+1:]] = true
		}
	}
	filtered := make([]pqt.Column, 0, len(selectedCols))
	for _, col := range schema {
		lname := strings.ToLower(col.Name)
		if needed[lname] {
			filtered = append(filtered, col)
		} else if dotIdx := strings.LastIndex(lname, "."); dotIdx >= 0 && needed[lname[dotIdx+1:]] {
			// schema "n2.n_nationkey" matches needed "n_nationkey"
			filtered = append(filtered, col)
		} else if neededSuffix[lname] {
			// schema "n_name" matches needed "n1.n_name"
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

// columnAliases maps catalog column names to known alternate names found in
// external Parquet datasets (e.g. Polars TPC-H uses "comments" for "l_comment").
var columnAliases = map[string][]string{
	"l_comment": {"comments"},
}

// findParquetColumn returns the index of the column with the given name
// in the parquet file's column paths, or -1 if not found. Falls back to
// known aliases when the exact name is absent.
func findParquetColumn(pqCols [][]string, name string) int {
	for i, path := range pqCols {
		if len(path) > 0 && path[len(path)-1] == name {
			return i
		}
	}
	// Try known aliases
	for _, alias := range columnAliases[name] {
		for i, path := range pqCols {
			if len(path) > 0 && path[len(path)-1] == alias {
				return i
			}
		}
	}
	return -1
}

// storageClass returns a normalized type representing the physical storage
// format used in a Vector. Types sharing a storage class have identical
// in-memory layout (e.g. TypeIPv4 and TypeInt64 both use Int64Data).
func storageClass(t pqt.TypeID) pqt.TypeID {
	switch t {
	case pqt.TypeInt64, pqt.TypeTimestamp, pqt.TypeIPv4, pqt.TypeMAC, pqt.TypeDuration:
		return pqt.TypeInt64
	case pqt.TypeInt32, pqt.TypePort, pqt.TypeProtocol, pqt.TypeDate:
		return pqt.TypeInt32
	case pqt.TypeFloat64:
		return pqt.TypeFloat64
	case pqt.TypeFloat32:
		return pqt.TypeFloat32
	case pqt.TypeBool:
		return pqt.TypeBool
	default: // String, Bytes, IPv6, CIDR, UUID
		return pqt.TypeString
	}
}

// readColumnChunk reads all pages from a column chunk into a Vector.
// When the file's physical type differs from the catalog type, values are
// coerced (e.g. INT64→INT32, INT64→Float64, DATE→String).
func readColumnChunk(vec *batch.Vector, chunk goparquet.ColumnChunk, numRows int, catalogType pqt.TypeID, pqFile *goparquet.File, colIdx int) error {
	pages := chunk.Pages()
	defer pages.Close()

	// Get max definition level and detect file-vs-catalog type mismatch
	pqCol := FindColumnByIndex(pqFile.Root(), colIdx)
	maxDefLevel := 0
	fileType := catalogType
	if pqCol != nil {
		maxDefLevel = pqCol.MaxDefinitionLevel()
		fileType = pqt.GoTypeToTypeID(pqCol)
	}
	// Only coerce when storage classes differ (e.g. INT64→INT32 needs narrowing).
	// Same-storage types (e.g. INT64 and IPv4) can use the catalog type directly.
	coerce := storageClass(fileType) != storageClass(catalogType)

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
			if coerce {
				if err := copyCoercedDirect(vec, offset, data, pageRows, fileType, catalogType); err != nil {
					return err
				}
			} else {
				if err := CopyTypedDataDirect(vec, offset, data, pageRows, catalogType); err != nil {
					return err
				}
			}
		} else {
			if coerce {
				if err := copyCoercedScatter(vec, offset, data, defLevels, byte(maxDefLevel), pageRows, fileType, catalogType); err != nil {
					return err
				}
			} else {
				if err := CopyTypedDataScatter(vec, offset, data, defLevels, byte(maxDefLevel), pageRows, catalogType); err != nil {
					return err
				}
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
//
// Optimized for Parquet's run-length encoded definition levels: detects
// contiguous runs of valid or null values and uses bulk copy()/SetNullRange
// instead of per-element checks. Falls back to per-element processing for
// bool and byte-array types where bulk copy is not straightforward.
func CopyTypedDataScatter(vec *batch.Vector, offset int, data pqencoding.Values, defLevels []byte, maxDefLevel byte, n int, typ pqt.TypeID) error {
	switch typ {
	case pqt.TypeInt64, pqt.TypeTimestamp, pqt.TypeIPv4, pqt.TypeMAC, pqt.TypeDuration:
		src := data.Int64()
		scatterRunsTyped(defLevels, maxDefLevel, n, func(dstStart, srcStart, count int) {
			copy(vec.Int64Data[offset+dstStart:offset+dstStart+count], src[srcStart:srcStart+count])
		}, func(dstStart, count int) {
			vec.Nulls.SetNullRange(offset+dstStart, count)
		})

	case pqt.TypeInt32, pqt.TypePort, pqt.TypeProtocol, pqt.TypeDate:
		src := data.Int32()
		scatterRunsTyped(defLevels, maxDefLevel, n, func(dstStart, srcStart, count int) {
			copy(vec.Int32Data[offset+dstStart:offset+dstStart+count], src[srcStart:srcStart+count])
		}, func(dstStart, count int) {
			vec.Nulls.SetNullRange(offset+dstStart, count)
		})

	case pqt.TypeFloat64:
		src := data.Double()
		scatterRunsTyped(defLevels, maxDefLevel, n, func(dstStart, srcStart, count int) {
			copy(vec.Float64Data[offset+dstStart:offset+dstStart+count], src[srcStart:srcStart+count])
		}, func(dstStart, count int) {
			vec.Nulls.SetNullRange(offset+dstStart, count)
		})

	case pqt.TypeFloat32:
		src := data.Float()
		scatterRunsTyped(defLevels, maxDefLevel, n, func(dstStart, srcStart, count int) {
			copy(vec.Float32Data[offset+dstStart:offset+dstStart+count], src[srcStart:srcStart+count])
		}, func(dstStart, count int) {
			vec.Nulls.SetNullRange(offset+dstStart, count)
		})

	case pqt.TypeBool:
		// Bool uses packed bit encoding — per-element is required
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

// scatterRunsTyped walks defLevels detecting contiguous runs of valid or null
// values and dispatches bulk operations. For valid runs, onValid receives the
// destination index, source value index, and run length so the caller can use
// copy() on the appropriate typed slice. For null runs, onNull receives the
// destination index and run length for bulk bitmap clearing.
func scatterRunsTyped(
	defLevels []byte, maxDefLevel byte, n int,
	onValid func(dstStart, srcStart, count int),
	onNull func(dstStart, count int),
) {
	valIdx := 0 // index into the non-null source values
	i := 0
	for i < n {
		if defLevels[i] == maxDefLevel {
			// Start of a valid run — find how far it extends
			runStart := i
			srcStart := valIdx
			for i < n && defLevels[i] == maxDefLevel {
				i++
				valIdx++
			}
			onValid(runStart, srcStart, i-runStart)
		} else {
			// Start of a null run — find how far it extends
			runStart := i
			for i < n && defLevels[i] != maxDefLevel {
				i++
			}
			onNull(runStart, i-runStart)
		}
	}
}

// copyCoercedDirect converts non-nullable page data from fileType to catalogType.
// Handles common Parquet type mismatches: INT64→INT32, INT64→Float64, Date→String.
func copyCoercedDirect(vec *batch.Vector, offset int, data pqencoding.Values, n int, fileType, catalogType pqt.TypeID) error {
	switch {
	case fileType == pqt.TypeInt64 && catalogType == pqt.TypeInt32:
		src := data.Int64()
		for i := 0; i < n; i++ {
			vec.Int32Data[offset+i] = int32(src[i])
		}
	case fileType == pqt.TypeInt64 && catalogType == pqt.TypeFloat64:
		src := data.Int64()
		for i := 0; i < n; i++ {
			vec.Float64Data[offset+i] = float64(src[i])
		}
	case (fileType == pqt.TypeDate || fileType == pqt.TypeInt32) && catalogType == pqt.TypeString:
		// DATE or INT32 (days since epoch) → "YYYY-MM-DD" string
		src := data.Int32()
		for i := 0; i < n; i++ {
			vec.BytesData.Set(offset+i, []byte(batch.FormatDate(src[i])))
		}
	default:
		return fmt.Errorf("unsupported type coercion: %v → %v", fileType, catalogType)
	}
	return nil
}

// copyCoercedScatter converts nullable page data from fileType to catalogType,
// respecting definition levels for null handling.
func copyCoercedScatter(vec *batch.Vector, offset int, data pqencoding.Values, defLevels []byte, maxDefLevel byte, n int, fileType, catalogType pqt.TypeID) error {
	switch {
	case fileType == pqt.TypeInt64 && catalogType == pqt.TypeInt32:
		src := data.Int64()
		scatterRunsTyped(defLevels, maxDefLevel, n, func(dstStart, srcStart, count int) {
			for i := 0; i < count; i++ {
				vec.Int32Data[offset+dstStart+i] = int32(src[srcStart+i])
			}
		}, func(dstStart, count int) {
			vec.Nulls.SetNullRange(offset+dstStart, count)
		})
	case fileType == pqt.TypeInt64 && catalogType == pqt.TypeFloat64:
		src := data.Int64()
		scatterRunsTyped(defLevels, maxDefLevel, n, func(dstStart, srcStart, count int) {
			for i := 0; i < count; i++ {
				vec.Float64Data[offset+dstStart+i] = float64(src[srcStart+i])
			}
		}, func(dstStart, count int) {
			vec.Nulls.SetNullRange(offset+dstStart, count)
		})
	case (fileType == pqt.TypeDate || fileType == pqt.TypeInt32) && catalogType == pqt.TypeString:
		src := data.Int32()
		valIdx := 0
		for i := 0; i < n; i++ {
			if defLevels[i] == maxDefLevel {
				vec.BytesData.Set(offset+i, []byte(batch.FormatDate(src[valIdx])))
				valIdx++
			} else {
				vec.Nulls.SetNull(offset + i)
				vec.BytesData.Set(offset+i, nil)
			}
		}
	default:
		return fmt.Errorf("unsupported type coercion: %v → %v", fileType, catalogType)
	}
	return nil
}
