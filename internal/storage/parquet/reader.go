package parquet

import (
	"encoding/binary"
	"fmt"
	"io"
)

// RowGroupStats contains min/max statistics for a row group.
type RowGroupStats struct {
	NumRows int64
	Columns map[string]ColumnStats
}

// ColumnStats contains min/max statistics for a single column in a row group.
type ColumnStats struct {
	HasStats  bool
	MinValue  any
	MaxValue  any
	NullCount int64
}

// Reader reads rows from a Parquet file.
type Reader struct {
	fr     *FileReader
	schema Schema
}

// NewReader creates a Parquet reader from an io.ReaderAt.
func NewReader(r io.ReaderAt, size int64) (*Reader, error) {
	fr, err := OpenFileReader(r, size)
	if err != nil {
		return nil, fmt.Errorf("opening parquet file: %w", err)
	}
	return &Reader{fr: fr, schema: fr.Schema()}, nil
}

// Schema returns the schema of the Parquet file.
func (r *Reader) Schema() Schema { return r.schema }

// FileReader returns the underlying native FileReader.
func (r *Reader) FileReader() *FileReader { return r.fr }

// NumRowGroups returns the number of row groups in the file.
func (r *Reader) NumRowGroups() int { return r.fr.NumRowGroups() }

// NumRows returns the total number of rows across all row groups.
func (r *Reader) NumRows() int64 { return r.fr.NumRows() }

// RowGroupStats returns statistics for a row group.
func (r *Reader) RowGroupStats(index int) RowGroupStats {
	return r.fr.RowGroupStats(index)
}

// ReadRows reads all rows from the Parquet file, optionally selecting only
// specific columns. Uses our native page reader — no parquet-go dependency.
func (r *Reader) ReadRows(selectedColumns []string) ([]map[string]any, error) {
	readCols := r.schema.Columns
	if len(selectedColumns) > 0 {
		readCols = filterSchemaColumns(r.schema.Columns, selectedColumns)
	}

	leaves := r.fr.Leaves()
	leafByName := make(map[string]int, len(leaves))
	for i, l := range leaves {
		leafByName[l.Name] = i
	}

	var allRows []map[string]any
	for rgIdx := 0; rgIdx < r.fr.NumRowGroups(); rgIdx++ {
		numRows := int(r.fr.RowGroupNumRows(rgIdx))
		if numRows == 0 {
			continue
		}

		colValues := make([][]any, len(readCols))
		for i, col := range readCols {
			leafIdx, ok := leafByName[col.Name]
			if !ok {
				colValues[i] = make([]any, numRows)
				continue
			}
			vals, err := readColumnToAny(r.fr, rgIdx, leafIdx, numRows, col.Type)
			if err != nil {
				return nil, fmt.Errorf("reading column %s: %w", col.Name, err)
			}
			colValues[i] = vals
		}

		for row := 0; row < numRows; row++ {
			m := make(map[string]any, len(readCols))
			for i, col := range readCols {
				if row < len(colValues[i]) {
					if v := colValues[i][row]; v != nil {
						m[col.Name] = v
					}
				}
			}
			allRows = append(allRows, m)
		}
	}
	return allRows, nil
}

// ReadRowGroup reads all rows from a specific row group.
func (r *Reader) ReadRowGroup(index int, selectedColumns []string) ([]map[string]any, error) {
	if index < 0 || index >= r.fr.NumRowGroups() {
		return nil, fmt.Errorf("row group index %d out of range [0, %d)", index, r.fr.NumRowGroups())
	}

	readCols := r.schema.Columns
	if len(selectedColumns) > 0 {
		readCols = filterSchemaColumns(r.schema.Columns, selectedColumns)
	}

	leaves := r.fr.Leaves()
	leafByName := make(map[string]int, len(leaves))
	for i, l := range leaves {
		leafByName[l.Name] = i
	}

	numRows := int(r.fr.RowGroupNumRows(index))
	colValues := make([][]any, len(readCols))
	for i, col := range readCols {
		leafIdx, ok := leafByName[col.Name]
		if !ok {
			colValues[i] = make([]any, numRows)
			continue
		}
		vals, err := readColumnToAny(r.fr, index, leafIdx, numRows, col.Type)
		if err != nil {
			return nil, fmt.Errorf("reading column %s: %w", col.Name, err)
		}
		colValues[i] = vals
	}

	rows := make([]map[string]any, numRows)
	for row := 0; row < numRows; row++ {
		m := make(map[string]any, len(readCols))
		for i, col := range readCols {
			if row < len(colValues[i]) {
				if v := colValues[i][row]; v != nil {
					m[col.Name] = v
				}
			}
		}
		rows[row] = m
	}
	return rows, nil
}

func filterSchemaColumns(cols []Column, selected []string) []Column {
	need := make(map[string]bool, len(selected))
	for _, s := range selected {
		need[s] = true
	}
	var result []Column
	for _, c := range cols {
		if need[c.Name] {
			result = append(result, c)
		}
	}
	return result
}

// readColumnToAny reads all pages for a column and returns values as []any.
func readColumnToAny(fr *FileReader, rgIdx, colIdx, numRows int, typeID TypeID) ([]any, error) {
	pr := fr.ColumnPages(rgIdx, colIdx)
	if pr == nil {
		return make([]any, numRows), nil
	}
	defer pr.Close()

	leaves := fr.Leaves()
	maxDef := int32(0)
	if colIdx < len(leaves) {
		maxDef = int32(leaves[colIdx].MaxDefLevel)
	}

	dict, err := pr.NextDictionary()
	if err != nil {
		return nil, fmt.Errorf("reading dictionary: %w", err)
	}

	values := make([]any, numRows)
	offset := 0

	for {
		page, err := pr.NextPage()
		if err != nil {
			return nil, fmt.Errorf("reading page: %w", err)
		}
		if page == nil {
			break
		}

		data := page.Data
		if dict != nil {
			data = resolveDictForRows(dict, data, typeID)
		}

		n := page.NumValues
		if offset+n > numRows {
			n = numRows - offset
		}

		if page.NumNulls == 0 || page.DefinitionLevels == nil {
			unpackAllPresent(values, offset, data, n, typeID)
		} else {
			unpackWithNulls(values, offset, data, page.DefinitionLevels, maxDef, n, typeID)
		}
		offset += n
	}
	return values, nil
}

func unpackAllPresent(dst []any, offset int, data Values, n int, typeID TypeID) {
	switch typeID {
	case TypeInt64, TypeTimestamp, TypeDuration:
		src := data.Int64()
		for i := 0; i < n && i < len(src); i++ {
			dst[offset+i] = src[i]
		}
	case TypeIPv4, TypeMAC:
		src := data.Int64()
		for i := 0; i < n && i < len(src); i++ {
			dst[offset+i] = src[i]
		}
	case TypeInt32, TypePort, TypeProtocol:
		src := data.Int32()
		for i := 0; i < n && i < len(src); i++ {
			dst[offset+i] = int64(src[i])
		}
	case TypeDate:
		src := data.Int32()
		for i := 0; i < n && i < len(src); i++ {
			dst[offset+i] = int64(src[i])
		}
	case TypeFloat64:
		src := data.Double()
		for i := 0; i < n && i < len(src); i++ {
			dst[offset+i] = src[i]
		}
	case TypeFloat32:
		src := data.Float()
		for i := 0; i < n && i < len(src); i++ {
			dst[offset+i] = float64(src[i])
		}
	case TypeBool:
		boolBytes := data.Boolean()
		for i := 0; i < n; i++ {
			byteIdx := i / 8
			bitIdx := uint(i % 8)
			dst[offset+i] = byteIdx < len(boolBytes) && (boolBytes[byteIdx]&(1<<bitIdx)) != 0
		}
	case TypeString, TypeCIDR:
		rawData, offsets := data.ByteArray()
		if offsets != nil {
			for i := 0; i < n && i+1 < len(offsets); i++ {
				dst[offset+i] = string(rawData[offsets[i]:offsets[i+1]])
			}
		}
	case TypeBytes, TypeIPv6, TypeUUID:
		rawData, offsets := data.ByteArray()
		if offsets != nil {
			for i := 0; i < n && i+1 < len(offsets); i++ {
				b := make([]byte, offsets[i+1]-offsets[i])
				copy(b, rawData[offsets[i]:offsets[i+1]])
				dst[offset+i] = b
			}
		}
	case TypeDecimal:
		unpackDecimalAllPresent(dst, offset, data, n)
	}
}

func unpackWithNulls(dst []any, offset int, data Values, defLevels []int32, maxDef int32, n int, typeID TypeID) {
	switch typeID {
	case TypeInt64, TypeTimestamp, TypeIPv4, TypeMAC, TypeDuration:
		src := data.Int64()
		vi := 0
		for i := 0; i < n; i++ {
			if i < len(defLevels) && defLevels[i] == maxDef {
				if vi < len(src) {
					dst[offset+i] = src[vi]
				}
				vi++
			}
		}
	case TypeInt32, TypePort, TypeProtocol, TypeDate:
		src := data.Int32()
		vi := 0
		for i := 0; i < n; i++ {
			if i < len(defLevels) && defLevels[i] == maxDef {
				if vi < len(src) {
					dst[offset+i] = int64(src[vi])
				}
				vi++
			}
		}
	case TypeFloat64:
		src := data.Double()
		vi := 0
		for i := 0; i < n; i++ {
			if i < len(defLevels) && defLevels[i] == maxDef {
				if vi < len(src) {
					dst[offset+i] = src[vi]
				}
				vi++
			}
		}
	case TypeFloat32:
		src := data.Float()
		vi := 0
		for i := 0; i < n; i++ {
			if i < len(defLevels) && defLevels[i] == maxDef {
				if vi < len(src) {
					dst[offset+i] = float64(src[vi])
				}
				vi++
			}
		}
	case TypeBool:
		boolBytes := data.Boolean()
		vi := 0
		for i := 0; i < n; i++ {
			if i < len(defLevels) && defLevels[i] == maxDef {
				byteIdx := vi / 8
				bitIdx := uint(vi % 8)
				dst[offset+i] = byteIdx < len(boolBytes) && (boolBytes[byteIdx]&(1<<bitIdx)) != 0
				vi++
			}
		}
	case TypeString, TypeCIDR:
		rawData, offsets := data.ByteArray()
		vi := 0
		if offsets != nil {
			for i := 0; i < n; i++ {
				if i < len(defLevels) && defLevels[i] == maxDef {
					if vi+1 < len(offsets) {
						dst[offset+i] = string(rawData[offsets[vi]:offsets[vi+1]])
					}
					vi++
				}
			}
		}
	case TypeBytes, TypeIPv6, TypeUUID:
		rawData, offsets := data.ByteArray()
		vi := 0
		if offsets != nil {
			for i := 0; i < n; i++ {
				if i < len(defLevels) && defLevels[i] == maxDef {
					if vi+1 < len(offsets) {
						b := make([]byte, offsets[vi+1]-offsets[vi])
						copy(b, rawData[offsets[vi]:offsets[vi+1]])
						dst[offset+i] = b
					}
					vi++
				}
			}
		}
	case TypeDecimal:
		unpackDecimalWithNulls(dst, offset, data, defLevels, maxDef, n)
	}
}

func unpackDecimalAllPresent(dst []any, offset int, data Values, n int) {
	switch data.PhysType() {
	case PhysicalInt64:
		src := data.Int64()
		for i := 0; i < n && i < len(src); i++ {
			dst[offset+i] = src[i]
		}
	case PhysicalInt32:
		src := data.Int32()
		for i := 0; i < n && i < len(src); i++ {
			dst[offset+i] = int64(src[i])
		}
	default:
		rawData, offsets := data.ByteArray()
		if offsets != nil {
			for i := 0; i < n && i+1 < len(offsets); i++ {
				b := rawData[offsets[i]:offsets[i+1]]
				dst[offset+i] = decimalBytesToInt64(b)
			}
		}
	}
}

func unpackDecimalWithNulls(dst []any, offset int, data Values, defLevels []int32, maxDef int32, n int) {
	switch data.PhysType() {
	case PhysicalInt64:
		src := data.Int64()
		vi := 0
		for i := 0; i < n; i++ {
			if i < len(defLevels) && defLevels[i] == maxDef {
				if vi < len(src) {
					dst[offset+i] = src[vi]
				}
				vi++
			}
		}
	case PhysicalInt32:
		src := data.Int32()
		vi := 0
		for i := 0; i < n; i++ {
			if i < len(defLevels) && defLevels[i] == maxDef {
				if vi < len(src) {
					dst[offset+i] = int64(src[vi])
				}
				vi++
			}
		}
	default:
		rawData, offsets := data.ByteArray()
		vi := 0
		if offsets != nil {
			for i := 0; i < n; i++ {
				if i < len(defLevels) && defLevels[i] == maxDef {
					if vi+1 < len(offsets) {
						b := rawData[offsets[vi]:offsets[vi+1]]
						dst[offset+i] = decimalBytesToInt64(b)
					}
					vi++
				}
			}
		}
	}
}

// decimalBytesToInt64 converts big-endian two's complement bytes to int64.
func decimalBytesToInt64(b []byte) int64 {
	if len(b) == 0 {
		return 0
	}
	// Sign-extend from MSB.
	negative := b[0]&0x80 != 0
	var v int64
	if negative {
		v = -1
	}
	for _, c := range b {
		v = (v << 8) | int64(c)
	}
	return v
}

// resolveDictForRows resolves dictionary indices to actual values.
func resolveDictForRows(dict *DictionaryData, page Values, typeID TypeID) Values {
	indices := page.Int32()
	n := page.Count()

	switch typeID {
	case TypeInt64, TypeTimestamp, TypeIPv4, TypeMAC, TypeDuration:
		src := dict.Data.Int64()
		dst := make([]int64, n)
		for i := 0; i < n; i++ {
			dst[i] = src[indices[i]]
		}
		return PlainInt64Values(dst)
	case TypeInt32, TypePort, TypeProtocol, TypeDate:
		src := dict.Data.Int32()
		dst := make([]int32, n)
		for i := 0; i < n; i++ {
			dst[i] = src[indices[i]]
		}
		return PlainInt32Values(dst)
	case TypeFloat64:
		src := dict.Data.Double()
		dst := make([]float64, n)
		for i := 0; i < n; i++ {
			dst[i] = src[indices[i]]
		}
		return PlainFloat64Values(dst)
	case TypeFloat32:
		src := dict.Data.Float()
		dst := make([]float32, n)
		for i := 0; i < n; i++ {
			dst[i] = src[indices[i]]
		}
		return PlainFloat32Values(dst)
	default:
		dictData, dictOffsets := dict.Data.ByteArray()
		var buf []byte
		offsets := make([]uint32, n+1)
		for i := 0; i < n; i++ {
			idx := indices[i]
			offsets[i] = uint32(len(buf))
			buf = append(buf, dictData[dictOffsets[idx]:dictOffsets[idx+1]]...)
		}
		offsets[n] = uint32(len(buf))
		return ByteArrayValues(buf, offsets)
	}
}

// CompareNative compares two native Go values for ordering.
func CompareNative(a, b any) int {
	switch av := a.(type) {
	case int64:
		if bv, ok := b.(int64); ok {
			if av < bv {
				return -1
			}
			if av > bv {
				return 1
			}
			return 0
		}
	case float64:
		if bv, ok := b.(float64); ok {
			if av < bv {
				return -1
			}
			if av > bv {
				return 1
			}
			return 0
		}
	case string:
		if bv, ok := b.(string); ok {
			if av < bv {
				return -1
			}
			if av > bv {
				return 1
			}
			return 0
		}
	case []byte:
		if bv, ok := b.([]byte); ok {
			la, lb := len(av), len(bv)
			for i := 0; i < la && i < lb; i++ {
				if av[i] < bv[i] {
					return -1
				}
				if av[i] > bv[i] {
					return 1
				}
			}
			if la < lb {
				return -1
			}
			if la > lb {
				return 1
			}
			return 0
		}
	}
	return 0
}

// RowGroupNumRows returns the row count for a row group (convenience for callers
// that don't want to go through FileReader).
func (r *Reader) RowGroupNumRows(index int) int64 {
	return r.fr.RowGroupNumRows(index)
}

// decimalFromBytes converts big-endian two's complement bytes to Int128.
// Exported for use by the physical planner's decimal page reader.
func DecimalFromBytes(b []byte) [2]uint64 {
	n := len(b)
	if n == 0 {
		return [2]uint64{}
	}

	negative := b[0]&0x80 != 0
	var hi, lo uint64
	if negative {
		hi = ^uint64(0)
		lo = ^uint64(0)
	}

	for i := 0; i < n; i++ {
		if i < n-8 {
			hi = (hi << 8) | uint64(b[i])
		} else {
			lo = (lo << 8) | uint64(b[i])
		}
	}

	// For short byte arrays (<= 8 bytes), hi should be sign-extended
	if n <= 8 {
		hi = 0
		if negative {
			hi = ^uint64(0)
		}
		lo = 0
		if negative {
			lo = ^uint64(0)
		}
		for _, c := range b {
			lo = (lo << 8) | uint64(c)
		}
	}

	return [2]uint64{hi, lo}
}

// decimalFromBytesRaw converts big-endian byte array to Int128 (hi, lo).
// Used internally by readDecimalPage.
func decimalFromBytesRaw(b []byte) (int64, uint64) {
	n := len(b)
	if n == 0 {
		return 0, 0
	}
	negative := b[0]&0x80 != 0
	var hi int64
	var lo uint64
	if negative {
		hi = -1
		lo = ^uint64(0)
	}
	if n <= 8 {
		lo = 0
		if negative {
			lo = ^uint64(0)
		}
		for _, c := range b {
			lo = (lo << 8) | uint64(c)
		}
		if negative {
			hi = -1
		}
	} else {
		for i := 0; i < n-8; i++ {
			hi = (hi << 8) | int64(b[i])
		}
		lo = 0
		for i := n - 8; i < n; i++ {
			lo = (lo << 8) | uint64(b[i])
		}
	}
	return hi, lo
}

// Deprecated compatibility aliases — exported functions that some callers may
// still reference. These delegate to TypeIDFromSchemaNode which is the canonical
// path now.

// TypeIDFromParquetPhysical maps a native physical type to TypeID.
func TypeIDFromParquetPhysical(pt PhysicalType) TypeID {
	switch pt {
	case PhysicalBoolean:
		return TypeBool
	case PhysicalInt32:
		return TypeInt32
	case PhysicalInt64:
		return TypeInt64
	case PhysicalFloat:
		return TypeFloat32
	case PhysicalDouble:
		return TypeFloat64
	case PhysicalByteArray, PhysicalFixedLenByteArray:
		return TypeString
	default:
		return TypeString
	}
}

// decimalFromBytesLE interprets LE decimal bytes for statistics.
func decimalFromBytesLE(b []byte, size int) int64 {
	switch size {
	case 4:
		if len(b) >= 4 {
			return int64(int32(binary.LittleEndian.Uint32(b)))
		}
	case 8:
		if len(b) >= 8 {
			return int64(binary.LittleEndian.Uint64(b))
		}
	}
	return 0
}
