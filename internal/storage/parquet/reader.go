package parquet

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strings"
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

// NewReaderAt creates a Parquet reader in staged (pread) mode: no
// whole-file buffer is ever held — each column-chunk read stages its
// byte range from r into a pooled buffer on demand (OpenFileReaderAt).
// Use this over NewReader when r is a local file: NewReader eagerly
// copies the entire file to heap, NewReaderAt reads footer-only.
func NewReaderAt(r io.ReaderAt, size int64) (*Reader, error) {
	fr, err := OpenFileReaderAt(r, size)
	if err != nil {
		return nil, fmt.Errorf("opening parquet file: %w", err)
	}
	return &Reader{fr: fr, schema: fr.Schema()}, nil
}

// NewReaderFromBytes creates a Parquet reader from a byte slice.
// Zero-copy: the slice is used directly by the underlying FileReader
// without allocating a separate copy. Use this when the data is already
// in memory (e.g. from readAllSized or a cache) to avoid the O(n) copy
// that NewReader performs via OpenFileReader.
func NewReaderFromBytes(data []byte) (*Reader, error) {
	fr, err := OpenFileReaderFromBytes(data)
	if err != nil {
		return nil, fmt.Errorf("opening parquet file: %w", err)
	}
	return &Reader{fr: fr, schema: fr.Schema()}, nil
}

// NewReaderFromBytesCached is NewReaderFromBytes with the footer decode
// served from the process footer cache when identity is non-empty (see
// footer_cache.go). data is still used zero-copy for every page read; only
// the Thrift footer decode is elided.
func NewReaderFromBytesCached(data []byte, identity string) (*Reader, error) {
	fr, err := OpenFileReaderFromBytesCached(data, identity)
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
// Supports nested types (ARRAY, MAP, ROW) by assembling leaf-level data
// using definition and repetition levels.
func (r *Reader) ReadRows(selectedColumns []string) ([]map[string]any, error) {
	return r.ReadRowsAs(nil, selectedColumns)
}

// ReadRowsAs is ReadRows with the CALLER's types: every column that schema
// names is decoded as the type the caller declares for it, and the rest as
// the type recovered from the file.
//
// The difference is not cosmetic. A parquet file cannot describe eight of
// this engine's types: buildLeafSchemaElement writes no logical annotation
// for IPv4, IPv6, MAC, PORT, PROTOCOL, DURATION, BYTES or UUID, so
// TypeIDFromSchemaNode recovers them as plain INT64/BYTE_ARRAY columns.
// Decoded that way an IPv6 came back as a 16-byte Go STRING, which
// Vector.SetValue then handed net.ParseIP and stored as NULL — so every
// query that falls back to the row reader (which is every query on a table
// carrying a nested column) silently lost the value. The catalog knows what
// the file cannot say; this is how it says so.
func (r *Reader) ReadRowsAs(schema []Column, selectedColumns []string) ([]map[string]any, error) {
	readCols := r.schema.Columns
	if len(selectedColumns) > 0 {
		readCols = filterSchemaColumns(r.schema.Columns, selectedColumns)
	}
	readCols = retypeFromCatalog(readCols, schema)

	// Check if we have nested columns that need assembly.
	hasNested := false
	for _, col := range readCols {
		if col.Type == TypeArray || col.Type == TypeMap || col.Type == TypeRow {
			hasNested = true
			break
		}
	}

	if !hasNested {
		return r.readRowsFlat(readCols)
	}
	return r.readRowsNested(readCols)
}

// readRowsFlat is the original flat-schema read path (unchanged behavior).
func (r *Reader) readRowsFlat(readCols []Column) ([]map[string]any, error) {
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

// readRowsNested handles schemas with ARRAY, MAP, or ROW columns by reading
// leaf-level pages with def/rep levels and assembling nested structures.
func (r *Reader) readRowsNested(readCols []Column) ([]map[string]any, error) {
	leaves := r.fr.Leaves()

	// Build a leaf index map by path for nested lookups.
	leafByPath := make(map[string]int, len(leaves))
	leafByName := make(map[string]int, len(leaves))
	for i, l := range leaves {
		leafByPath[pathKey(l.Path)] = i
		leafByName[l.Name] = i
	}

	var allRows []map[string]any
	for rgIdx := 0; rgIdx < r.fr.NumRowGroups(); rgIdx++ {
		numRows := int(r.fr.RowGroupNumRows(rgIdx))
		if numRows == 0 {
			continue
		}

		// Read all leaf column pages with def/rep levels.
		leafPages := make([]leafColumnData, len(leaves))
		for i := range leaves {
			lcd, err := readLeafColumn(r.fr, rgIdx, i)
			if err != nil {
				return nil, fmt.Errorf("reading leaf column %v: %w", leaves[i].Path, err)
			}
			leafPages[i] = lcd
		}

		// Assemble rows.
		rows := make([]map[string]any, numRows)
		for i := range rows {
			rows[i] = make(map[string]any, len(readCols))
		}

		for _, col := range readCols {
			switch col.Type {
			case TypeArray:
				assembleArrayColumn(col, leaves, leafByPath, leafPages, rows)
			case TypeMap:
				assembleMapColumn(col, leaves, leafByPath, leafPages, rows)
			case TypeRow:
				assembleRowColumn(col, leaves, leafByPath, leafPages, rows)
			default:
				// Flat column — use the simple path.
				leafIdx, ok := leafByName[col.Name]
				if !ok {
					continue
				}
				vals, err := readColumnToAny(r.fr, rgIdx, leafIdx, numRows, col.Type)
				if err != nil {
					return nil, fmt.Errorf("reading column %s: %w", col.Name, err)
				}
				for row := 0; row < numRows; row++ {
					if row < len(vals) && vals[row] != nil {
						rows[row][col.Name] = vals[row]
					}
				}
			}
		}

		allRows = append(allRows, rows...)
	}
	return allRows, nil
}

// leafColumnData holds all values and levels for a single leaf column page.
type leafColumnData struct {
	values    []any   // decoded values (only non-null entries)
	defLevels []int32 // definition levels for each entry
	repLevels []int32 // repetition levels for each entry
	maxDef    int32
	maxRep    int32
	typeID    TypeID
}

// readLeafColumn reads all pages for a leaf column and returns raw values with levels.
func readLeafColumn(fr *FileReader, rgIdx, colIdx int) (leafColumnData, error) {
	leaves := fr.Leaves()
	leaf := leaves[colIdx]
	typeID := TypeIDFromSchemaNode(leaf)

	lcd := leafColumnData{
		maxDef: int32(leaf.MaxDefLevel),
		maxRep: int32(leaf.MaxRepLevel),
		typeID: typeID,
	}

	pr := fr.ColumnPages(rgIdx, colIdx)
	if pr == nil {
		return lcd, nil
	}
	defer pr.Close()

	dict, err := pr.NextDictionary()
	if err != nil {
		return lcd, fmt.Errorf("reading dictionary: %w", err)
	}

	for {
		page, err := pr.NextPage()
		if err != nil {
			return lcd, fmt.Errorf("reading page: %w", err)
		}
		if page == nil {
			break
		}

		data := page.Data
		if page.IsDictEncoded() {
			if dict == nil {
				return lcd, fmt.Errorf("dictionary-encoded page but chunk has no dictionary page")
			}
			data, err = resolveDictForRows(dict, data, typeID)
			if err != nil {
				return lcd, fmt.Errorf("resolving dictionary page: %w", err)
			}
		}

		// Collect definition and repetition levels.
		if page.DefinitionLevels != nil {
			lcd.defLevels = append(lcd.defLevels, page.DefinitionLevels...)
		} else {
			// All values present — fill with maxDef.
			for i := 0; i < page.NumValues; i++ {
				lcd.defLevels = append(lcd.defLevels, lcd.maxDef)
			}
		}
		if page.RepetitionLevels != nil {
			lcd.repLevels = append(lcd.repLevels, page.RepetitionLevels...)
		} else {
			for i := 0; i < page.NumValues; i++ {
				lcd.repLevels = append(lcd.repLevels, 0)
			}
		}

		// Decode non-null values.
		pageVals := decodePageValues(data, lcd.maxDef, page.DefinitionLevels, typeID)
		lcd.values = append(lcd.values, pageVals...)
		page.Release()
	}

	// Nested (ARRAY/MAP/ROW) leaves come through here too, so this is the
	// scaling point for a timestamp buried inside a struct or list.
	if typeID == TypeTimestamp {
		if div := TimestampDivisorFromSchemaNode(leaf); div != 1 {
			for i, v := range lcd.values {
				if iv, ok := v.(int64); ok {
					lcd.values[i] = TimestampToEngineMillis(iv, div)
				}
			}
		}
	}

	return lcd, nil
}

// decodePageValues decodes values from a page, returning only non-null values.
func decodePageValues(data Values, maxDef int32, defLevels []int32, typeID TypeID) []any {
	if defLevels == nil {
		// All values present.
		return decodeAllValues(data, typeID)
	}

	// Count non-null values.
	nonNull := 0
	for _, d := range defLevels {
		if d == maxDef {
			nonNull++
		}
	}

	allVals := decodeAllValues(data, typeID)
	if len(allVals) > nonNull {
		allVals = allVals[:nonNull]
	}
	return allVals
}

// decodeAllValues decodes all values from a Values buffer into []any.
func decodeAllValues(data Values, typeID TypeID) []any {
	switch typeID {
	case TypeInt64, TypeTimestamp, TypeDuration, TypeIPv4, TypeMAC:
		src := data.Int64()
		vals := make([]any, len(src))
		for i, v := range src {
			vals[i] = v
		}
		return vals
	case TypeInt32, TypePort, TypeProtocol, TypeDate:
		src := data.Int32()
		vals := make([]any, len(src))
		for i, v := range src {
			vals[i] = int64(v)
		}
		return vals
	case TypeFloat64:
		src := data.Double()
		vals := make([]any, len(src))
		for i, v := range src {
			vals[i] = v
		}
		return vals
	case TypeFloat32:
		src := data.Float()
		vals := make([]any, len(src))
		for i, v := range src {
			vals[i] = float64(v)
		}
		return vals
	case TypeString, TypeCIDR:
		rawData, offsets := data.ByteArray()
		if offsets == nil {
			return nil
		}
		vals := make([]any, 0, len(offsets)-1)
		for i := 0; i+1 < len(offsets); i++ {
			vals = append(vals, string(rawData[offsets[i]:offsets[i+1]]))
		}
		return vals
	case TypeBool:
		boolBytes := data.Boolean()
		n := data.Count()
		vals := make([]any, n)
		for i := 0; i < n; i++ {
			byteIdx := i / 8
			bitIdx := uint(i % 8)
			vals[i] = byteIdx < len(boolBytes) && (boolBytes[byteIdx]&(1<<bitIdx)) != 0
		}
		return vals
	case TypeBytes, TypeIPv6, TypeUUID:
		rawData, offsets := data.ByteArray()
		if offsets == nil {
			return nil
		}
		vals := make([]any, 0, len(offsets)-1)
		for i := 0; i+1 < len(offsets); i++ {
			b := make([]byte, offsets[i+1]-offsets[i])
			copy(b, rawData[offsets[i]:offsets[i+1]])
			vals = append(vals, b)
		}
		return vals
	default:
		return nil
	}
}

// pathKey creates a lookup key from a path slice.
func pathKey(path []string) string {
	if len(path) == 0 {
		return ""
	}
	result := path[0]
	for _, p := range path[1:] {
		result += "." + p
	}
	return result
}

// findLeafByPrefix finds a leaf column whose path starts with the given prefix.
func findLeafByPrefix(leaves []*SchemaNode, leafByPath map[string]int, prefix []string) (int, bool) {
	prefixKey := pathKey(prefix)
	for i, l := range leaves {
		lk := pathKey(l.Path)
		if lk == prefixKey || (len(lk) > len(prefixKey) && lk[:len(prefixKey)] == prefixKey && lk[len(prefixKey)] == '.') {
			return i, true
		}
		_ = leafByPath // use the map to silence linter
	}
	return -1, false
}

// assembleArrayColumn reads an ARRAY column's leaves and assembles []any values per row.
func assembleArrayColumn(col Column, leaves []*SchemaNode, leafByPath map[string]int, leafPages []leafColumnData, rows []map[string]any) {
	elemCol := Column{Name: "element", Type: TypeString}
	if col.ElementType != nil {
		elemCol = *col.ElementType
	}

	// For simple element types, find the single leaf.
	elemPath := []string{col.Name, "list", elemCol.Name}
	leafIdx, ok := leafByPath[pathKey(elemPath)]
	if !ok {
		// Try finding by prefix for nested element types.
		leafIdx2, ok2 := findLeafByPrefix(leaves, leafByPath, []string{col.Name, "list"})
		if !ok2 {
			return
		}
		leafIdx = leafIdx2
	}

	lcd := leafPages[leafIdx]
	if len(lcd.defLevels) == 0 {
		return
	}

	// The outer group adds 1 def level if nullable.
	// The list (repeated) group adds 1 def level and 1 rep level.
	// The element adds 1 def level if optional.
	// So: def=0 -> null array, def=1 -> empty array, def=2 -> list present but element null,
	//     def=3 -> value present (for optional group + repeated list + optional element).
	// For non-nullable outer: def=0 -> empty array, def=1 -> list present but elem null, def=2 -> value present.
	outerGroupDef := int32(0)
	if col.Nullable {
		outerGroupDef = 1
	}
	emptyArrayDef := outerGroupDef // list group absent means empty

	rowIdx := 0
	valIdx := 0

	for i := 0; i < len(lcd.defLevels); i++ {
		def := lcd.defLevels[i]
		rep := int32(0)
		if i < len(lcd.repLevels) {
			rep = lcd.repLevels[i]
		}

		if rep == 0 && i > 0 {
			// New row boundary (rep=0 means start of new top-level value).
			rowIdx++
		}
		if rowIdx >= len(rows) {
			break
		}

		if def < outerGroupDef {
			// Null array — leave as nil in the row.
			continue
		}
		if def == emptyArrayDef {
			// Empty array — only set if not already set.
			if _, exists := rows[rowIdx][col.Name]; !exists {
				rows[rowIdx][col.Name] = []any{}
			}
			continue
		}

		// Value present in the list.
		var elemVal any
		if def == lcd.maxDef {
			if valIdx < len(lcd.values) {
				elemVal = lcd.values[valIdx]
			}
			valIdx++
		}

		existing, exists := rows[rowIdx][col.Name]
		if !exists {
			rows[rowIdx][col.Name] = []any{elemVal}
		} else {
			arr := existing.([]any)
			rows[rowIdx][col.Name] = append(arr, elemVal)
		}
	}
}

// assembleMapColumn reads a MAP column's leaves and assembles map values per row.
func assembleMapColumn(col Column, leaves []*SchemaNode, leafByPath map[string]int, leafPages []leafColumnData, rows []map[string]any) {
	if col.ElementType == nil || len(col.ElementType.Fields) != 2 {
		return
	}

	keyCol := col.ElementType.Fields[0]
	valCol := col.ElementType.Fields[1]

	keyPath := []string{col.Name, "key_value", keyCol.Name}
	valPath := []string{col.Name, "key_value", valCol.Name}

	keyLeafIdx, ok := leafByPath[pathKey(keyPath)]
	if !ok {
		return
	}
	valLeafIdx, ok := leafByPath[pathKey(valPath)]
	if !ok {
		return
	}

	keyLCD := leafPages[keyLeafIdx]
	valLCD := leafPages[valLeafIdx]

	if len(keyLCD.defLevels) == 0 {
		return
	}

	outerGroupDef := int32(0)
	if col.Nullable {
		outerGroupDef = 1
	}

	rowIdx := 0
	keyValIdx := 0
	valValIdx := 0

	for i := 0; i < len(keyLCD.defLevels); i++ {
		def := keyLCD.defLevels[i]
		rep := int32(0)
		if i < len(keyLCD.repLevels) {
			rep = keyLCD.repLevels[i]
		}

		if rep == 0 && i > 0 {
			rowIdx++
		}
		if rowIdx >= len(rows) {
			break
		}

		if def < outerGroupDef {
			continue
		}
		if def == outerGroupDef {
			// MAP present, key_value group absent: an EMPTY map, which is
			// not the same value as a NULL map and must not read back as
			// one. assembleArrayColumn has always drawn this distinction
			// for the zero-length ARRAY; MAP simply fell through to "no
			// entries", which the caller cannot tell from absent.
			if _, exists := rows[rowIdx][col.Name]; !exists {
				rows[rowIdx][col.Name] = map[string]any{}
			}
			continue
		}

		// Key present (key is required so if kv group present, key is present).
		if def >= keyLCD.maxDef {
			var k any
			if keyValIdx < len(keyLCD.values) {
				k = keyLCD.values[keyValIdx]
			}
			keyValIdx++

			var v any
			if i < len(valLCD.defLevels) && valLCD.defLevels[i] == valLCD.maxDef {
				if valValIdx < len(valLCD.values) {
					v = valLCD.values[valValIdx]
				}
				valValIdx++
			}

			existing, exists := rows[rowIdx][col.Name]
			if !exists {
				rows[rowIdx][col.Name] = map[string]any{fmt.Sprint(k): v}
			} else {
				m := existing.(map[string]any)
				m[fmt.Sprint(k)] = v
			}
		}
	}
}

// assembleRowColumn reads a ROW/STRUCT column's leaves and assembles map values per row.
func assembleRowColumn(col Column, leaves []*SchemaNode, leafByPath map[string]int, leafPages []leafColumnData, rows []map[string]any) {
	outerGroupDef := int32(0)
	if col.Nullable {
		outerGroupDef = 1
	}

	for _, field := range col.Fields {
		fieldPath := []string{col.Name, field.Name}
		leafIdx, ok := leafByPath[pathKey(fieldPath)]
		if !ok {
			continue
		}

		lcd := leafPages[leafIdx]
		if len(lcd.defLevels) == 0 {
			continue
		}

		valIdx := 0
		for rowIdx := 0; rowIdx < len(rows) && rowIdx < len(lcd.defLevels); rowIdx++ {
			def := lcd.defLevels[rowIdx]

			if def < outerGroupDef {
				// Outer struct is null.
				continue
			}

			// Struct is present. Check if field value is present.
			if def == lcd.maxDef {
				var fieldVal any
				if valIdx < len(lcd.values) {
					fieldVal = lcd.values[valIdx]
				}
				valIdx++

				existing, exists := rows[rowIdx][col.Name]
				if !exists {
					rows[rowIdx][col.Name] = map[string]any{field.Name: fieldVal}
				} else {
					m := existing.(map[string]any)
					m[field.Name] = fieldVal
				}
			} else if def >= outerGroupDef {
				// Struct present but field is null. Still need to ensure struct exists.
				if _, exists := rows[rowIdx][col.Name]; !exists {
					rows[rowIdx][col.Name] = map[string]any{}
				}
			}
		}
	}
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

// float32sFromBytes decodes a VECTOR leaf's fixed-length bytes back into the
// []float32 the engine's row shape carries — the inverse of the writer's
// toBytes, little-endian, four bytes per dimension. Without it neither
// unpack function had a VECTOR arm at all, so Reader.ReadRows answered NULL
// for every VECTOR column it was asked for.
func float32sFromBytes(b []byte) []float32 {
	out := make([]float32, len(b)/4)
	for i := range out {
		out[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[i*4:]))
	}
	return out
}

// retypeFromCatalog replaces each read column's type with the catalog's,
// where the catalog names it and both sides are leaves.
//
// Leaves only, deliberately: a nested column's read plan is built from the
// FILE's shape (assembleMapColumn walks name/"key_value"/field paths), and
// substituting a catalog Column whose children were resolved differently
// would look up leaves that do not exist. A lossy leaf INSIDE a container
// stays lossy — that is the same annotation gap, one level down, and it
// needs the annotations, not a substitution.
func retypeFromCatalog(readCols, catalog []Column) []Column {
	if len(catalog) == 0 || len(readCols) == 0 {
		return readCols
	}
	byName := make(map[string]Column, len(catalog))
	for _, c := range catalog {
		byName[strings.ToLower(c.Name)] = c
	}
	out := make([]Column, len(readCols))
	copy(out, readCols)
	for i, c := range out {
		want, ok := byName[strings.ToLower(c.Name)]
		if !ok || want.Type == c.Type {
			continue
		}
		if isNestedType(c.Type) || isNestedType(want.Type) {
			continue
		}
		out[i].Type = want.Type
		out[i].Precision, out[i].Scale = want.Precision, want.Scale
		out[i].Dimension = want.Dimension
	}
	return out
}

func isNestedType(t TypeID) bool {
	return t == TypeArray || t == TypeRow || t == TypeMap
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
		if page.IsDictEncoded() {
			if dict == nil {
				return nil, fmt.Errorf("dictionary-encoded page but chunk has no dictionary page")
			}
			data, err = resolveDictForRows(dict, data, typeID)
			if err != nil {
				return nil, fmt.Errorf("resolving dictionary page: %w", err)
			}
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
		page.Release()
	}

	// A micro/nano TIMESTAMP column decodes to the file's unit; the engine
	// speaks milliseconds. See TimestampDivisorFromSchemaNode.
	if typeID == TypeTimestamp && colIdx < len(leaves) {
		if div := TimestampDivisorFromSchemaNode(leaves[colIdx]); div != 1 {
			for i, v := range values {
				if iv, ok := v.(int64); ok {
					values[i] = TimestampToEngineMillis(iv, div)
				}
			}
		}
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
	case TypeVector:
		rawData, offsets := data.ByteArray()
		if offsets != nil {
			for i := 0; i < n && i+1 < len(offsets); i++ {
				dst[offset+i] = float32sFromBytes(rawData[offsets[i]:offsets[i+1]])
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
	case TypeVector:
		rawData, offsets := data.ByteArray()
		vi := 0
		if offsets != nil {
			for i := 0; i < n; i++ {
				if i < len(defLevels) && defLevels[i] == maxDef {
					if vi+1 < len(offsets) {
						dst[offset+i] = float32sFromBytes(rawData[offsets[vi]:offsets[vi+1]])
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

// ValidateDictIndices checks that a page's dictionary indices are all
// within [0, numValues) before any gather dereferences them. Corrupt or
// hostile files can carry out-of-range indices; without this pass the
// typed gather loops panic (or silently read the wrong entry).
func ValidateDictIndices(indices []int32, n, numValues int) error {
	if len(indices) < n {
		return fmt.Errorf("dictionary page: %d indices for %d values", len(indices), n)
	}
	for i := 0; i < n; i++ {
		if idx := indices[i]; idx < 0 || int(idx) >= numValues {
			return fmt.Errorf("dictionary index %d out of range [0, %d)", idx, numValues)
		}
	}
	return nil
}

// resolveDictForRows resolves dictionary indices to actual values.
func resolveDictForRows(dict *DictionaryData, page Values, typeID TypeID) (Values, error) {
	indices := page.Int32()
	n := page.Count()
	if err := ValidateDictIndices(indices, n, dict.NumValues); err != nil {
		return Values{}, err
	}

	switch typeID {
	case TypeInt64, TypeTimestamp, TypeIPv4, TypeMAC, TypeDuration:
		src := dict.Data.Int64()
		dst := make([]int64, n)
		for i := 0; i < n; i++ {
			dst[i] = src[indices[i]]
		}
		return PlainInt64Values(dst), nil
	case TypeInt32, TypePort, TypeProtocol, TypeDate:
		src := dict.Data.Int32()
		dst := make([]int32, n)
		for i := 0; i < n; i++ {
			dst[i] = src[indices[i]]
		}
		return PlainInt32Values(dst), nil
	case TypeFloat64:
		src := dict.Data.Double()
		dst := make([]float64, n)
		for i := 0; i < n; i++ {
			dst[i] = src[indices[i]]
		}
		return PlainFloat64Values(dst), nil
	case TypeFloat32:
		src := dict.Data.Float()
		dst := make([]float32, n)
		for i := 0; i < n; i++ {
			dst[i] = src[indices[i]]
		}
		return PlainFloat32Values(dst), nil
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
		return ByteArrayValues(buf, offsets), nil
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

