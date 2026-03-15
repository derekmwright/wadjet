package scan

import (
	"encoding/binary"
	"fmt"
	"io"

	goparquet "github.com/parquet-go/parquet-go"
	pqencoding "github.com/parquet-go/parquet-go/encoding"

	"github.com/derekmwright/caelum/internal/engine/batch"
	pqt "github.com/derekmwright/caelum/internal/storage/parquet"
)

// readRowGroupColumnar reads a parquet row group directly into a RecordBatch
// using typed column access — no map[string]any intermediate.
func readRowGroupColumnar(rg goparquet.RowGroup, schema []pqt.Column, pqFile *goparquet.File) (*batch.RecordBatch, error) {
	numRows := int(rg.NumRows())
	if numRows == 0 {
		return nil, nil
	}

	b := batch.NewRecordBatch(schema, numRows)
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
	pqCol := findColumnByIndex(pqFile.Root(), colIdx)
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

		if defLevels == nil {
			// Non-nullable column — direct copy
			if err := copyTypedDataDirect(vec, offset, data, pageRows, typ); err != nil {
				return err
			}
		} else {
			// Nullable column — scatter using definition levels
			if err := copyTypedDataScatter(vec, offset, data, defLevels, byte(maxDefLevel), pageRows, typ); err != nil {
				return err
			}
		}

		offset += pageRows
	}

	return nil
}

// findColumnByIndex finds the leaf Column at the given index in the schema tree.
func findColumnByIndex(root *goparquet.Column, idx int) *goparquet.Column {
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

// copyTypedDataDirect copies non-nullable page data directly into a Vector.
func copyTypedDataDirect(vec *batch.Vector, offset int, data pqencoding.Values, n int, typ pqt.TypeID) error {
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
			// Offset-based layout
			for i := 0; i < n; i++ {
				start := offsets[i]
				end := offsets[i+1]
				vec.BytesData.Set(offset+i, rawData[start:end])
			}
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

// copyTypedDataScatter copies nullable page data into a Vector, scattering
// values according to definition levels and setting nulls in the bitmap.
func copyTypedDataScatter(vec *batch.Vector, offset int, data pqencoding.Values, defLevels []byte, maxDefLevel byte, n int, typ pqt.TypeID) error {
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
