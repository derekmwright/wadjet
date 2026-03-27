package parquet

import (
	"fmt"
	"io"

	goparquet "github.com/parquet-go/parquet-go"
)

// RowGroupStats contains min/max statistics for a row group.
type RowGroupStats struct {
	NumRows int64
	Columns map[string]ColumnStats
}

// ColumnStats contains min/max statistics for a single column in a row group.
type ColumnStats struct {
	HasStats bool
	MinValue any
	MaxValue any
	NullCount int64
}

// Reader reads rows from a Parquet file.
type Reader struct {
	file   *goparquet.File
	schema Schema
}

// NewReader creates a Parquet reader from an io.ReaderAt.
func NewReader(r io.ReaderAt, size int64) (*Reader, error) {
	f, err := goparquet.OpenFile(r, size)
	if err != nil {
		return nil, fmt.Errorf("opening parquet file: %w", err)
	}

	schema := extractSchema(f)

	return &Reader{
		file:   f,
		schema: schema,
	}, nil
}

// Schema returns the schema of the Parquet file.
func (r *Reader) Schema() Schema {
	return r.schema
}

// File returns the underlying parquet-go File for direct columnar access.
func (r *Reader) File() *goparquet.File {
	return r.file
}

// NumRowGroups returns the number of row groups in the file.
func (r *Reader) NumRowGroups() int {
	return len(r.file.RowGroups())
}

// NumRows returns the total number of rows across all row groups.
func (r *Reader) NumRows() int64 {
	return r.file.NumRows()
}

// RowGroupStats returns statistics for a row group.
func (r *Reader) RowGroupStats(index int) RowGroupStats {
	rgs := r.file.RowGroups()
	if index < 0 || index >= len(rgs) {
		return RowGroupStats{}
	}

	rg := rgs[index]
	stats := RowGroupStats{
		NumRows: rg.NumRows(),
		Columns: make(map[string]ColumnStats),
	}

	columnPaths := r.file.Schema().Columns()
	for i, cc := range rg.ColumnChunks() {
		if i >= len(columnPaths) {
			break
		}
		colPath := columnPaths[i]
		colName := colPath[len(colPath)-1]

		ci, err := cc.ColumnIndex()
		if err != nil {
			stats.Columns[colName] = ColumnStats{HasStats: false}
			continue
		}

		numPages := ci.NumPages()
		cs := ColumnStats{HasStats: numPages > 0}
		if cs.HasStats {
			var totalNulls int64
			for p := 0; p < numPages; p++ {
				totalNulls += ci.NullCount(p)
			}
			cs.NullCount = totalNulls

			// Compute row-group min/max from page-level stats
			for p := 0; p < numPages; p++ {
				if ci.NullPage(p) {
					continue
				}
				pageMin := ci.MinValue(p)
				pageMax := ci.MaxValue(p)
				if !pageMin.IsNull() {
					native := parquetValueToNative(pageMin)
					if cs.MinValue == nil || CompareNative(native, cs.MinValue) < 0 {
						cs.MinValue = native
					}
				}
				if !pageMax.IsNull() {
					native := parquetValueToNative(pageMax)
					if cs.MaxValue == nil || CompareNative(native, cs.MaxValue) > 0 {
						cs.MaxValue = native
					}
				}
			}
		}
		stats.Columns[colName] = cs
	}

	return stats
}

// ReadRows reads all rows from the Parquet file, optionally selecting only specific columns.
// When selectedColumns is non-empty, true column projection is used at the parquet level
// so that only the requested columns are decoded from disk.
func (r *Reader) ReadRows(selectedColumns []string) ([]map[string]any, error) {
	var opts []goparquet.ReaderOption
	if len(selectedColumns) > 0 {
		if projected := r.buildProjectedSchema(selectedColumns); projected != nil {
			opts = append(opts, projected)
		}
	}

	pr := goparquet.NewReader(r.file, opts...)
	defer pr.Close()

	numRows := pr.NumRows()
	rows := make([]map[string]any, 0, numRows)
	for {
		row := make(map[string]any)
		err := pr.Read(&row)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("reading rows: %w", err)
		}
		rows = append(rows, row)
	}

	return rows, nil
}

// buildProjectedSchema creates a parquet schema containing only the requested columns.
// Returns nil if projection cannot be built (falls back to reading all columns).
func (r *Reader) buildProjectedSchema(selectedColumns []string) *goparquet.Schema {
	fileSchema := r.file.Schema()
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

// ReadRowGroup reads all rows from a specific row group.
func (r *Reader) ReadRowGroup(index int, selectedColumns []string) ([]map[string]any, error) {
	rgs := r.file.RowGroups()
	if index < 0 || index >= len(rgs) {
		return nil, fmt.Errorf("row group index %d out of range [0, %d)", index, len(rgs))
	}

	rg := rgs[index]
	var opts []goparquet.ReaderOption
	if len(selectedColumns) > 0 {
		if projected := r.buildProjectedSchema(selectedColumns); projected != nil {
			opts = append(opts, projected)
		}
	}

	pr := goparquet.NewRowGroupReader(rg, opts...)
	defer pr.Close()

	numRows := rg.NumRows()
	rows := make([]map[string]any, 0, numRows)
	for {
		row := make(map[string]any)
		err := pr.Read(&row)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("reading row group: %w", err)
		}
		rows = append(rows, row)
	}

	return rows, nil
}

func extractSchema(f *goparquet.File) Schema {
	var columns []Column
	for _, child := range f.Root().Columns() {
		columns = append(columns, extractColumn(child))
	}
	return Schema{Columns: columns}
}

// extractColumn converts a parquet-go Column into our Column definition,
// detecting LIST, MAP, and STRUCT patterns from the Parquet schema structure.
func extractColumn(col *goparquet.Column) Column {
	if col.Leaf() {
		return Column{
			Name:     col.Name(),
			Type:     GoTypeToTypeID(col),
			Nullable: col.Optional(),
		}
	}

	children := col.Columns()

	// Detect LIST pattern: group with one repeated child group
	// Standard: optional group NAME (LIST) { repeated group list { element } }
	// Also handles: optional group NAME { repeated group bag { element } }
	if len(children) == 1 && children[0].Repeated() {
		repeatedChild := children[0]
		innerChildren := repeatedChild.Columns()

		if len(innerChildren) == 1 {
			// ARRAY: single element column
			elemCol := extractColumn(innerChildren[0])
			return Column{
				Name:        col.Name(),
				Type:        TypeArray,
				Nullable:    col.Optional(),
				ElementType: &elemCol,
			}
		}
		if len(innerChildren) == 2 {
			// MAP: key-value pair
			keyCol := extractColumn(innerChildren[0])
			valCol := extractColumn(innerChildren[1])
			entryCol := Column{
				Name:   "entry",
				Type:   TypeRow,
				Fields: []Column{keyCol, valCol},
			}
			return Column{
				Name:        col.Name(),
				Type:        TypeMap,
				Nullable:    col.Optional(),
				ElementType: &entryCol,
			}
		}
	}

	// ROW/STRUCT: group with named children (none repeated at this level)
	fields := make([]Column, len(children))
	for i, child := range children {
		fields[i] = extractColumn(child)
	}
	return Column{
		Name:     col.Name(),
		Type:     TypeRow,
		Nullable: col.Optional(),
		Fields:   fields,
	}
}

// collectLeafColumns flattens a Parquet column tree into leaf-only columns.
// Used by legacy code paths that don't support nested types.
func collectLeafColumns(col *goparquet.Column, columns *[]Column) {
	if col.Leaf() {
		name := flattenColumnPath(col)
		typ := GoTypeToTypeID(col)
		nullable := col.Optional()
		*columns = append(*columns, Column{
			Name:     name,
			Type:     typ,
			Nullable: nullable,
		})
		return
	}
	for _, child := range col.Columns() {
		collectLeafColumns(child, columns)
	}
}

func flattenColumnPath(col *goparquet.Column) string {
	path := col.Path()
	if len(path) == 0 {
		return col.Name()
	}
	return path[len(path)-1]
}

// parquetValueToNative converts a parquet-go Value to a Go native type.
func parquetValueToNative(v goparquet.Value) any {
	switch v.Kind() {
	case goparquet.Boolean:
		return v.Boolean()
	case goparquet.Int32:
		return int64(v.Int32())
	case goparquet.Int64:
		return v.Int64()
	case goparquet.Float:
		return float64(v.Float())
	case goparquet.Double:
		return v.Double()
	case goparquet.ByteArray:
		return string(v.ByteArray())
	case goparquet.FixedLenByteArray:
		return string(v.ByteArray())
	default:
		return nil
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
	}
	return 0
}

// GoTypeToTypeID maps a parquet-go column to a Wadjet TypeID based on the
// column's logical and physical type. Exported for use by the scan package.
func GoTypeToTypeID(col *goparquet.Column) TypeID {
	return TypeIDFromParquetType(col.Type())
}

// TypeIDFromParquetType maps a parquet Type (from a Column or ColumnChunk) to
// our TypeID. This is the canonical type-detection path — it handles logical
// type annotations (DATE, TIMESTAMP, DECIMAL, INTEGER) and falls back to the
// physical kind.
func TypeIDFromParquetType(t goparquet.Type) TypeID {
	lt := t.LogicalType()
	if lt != nil {
		if lt.UTF8 != nil {
			return TypeString
		}
		if lt.Timestamp != nil {
			return TypeTimestamp
		}
		if lt.Date != nil {
			return TypeDate
		}
		if lt.Decimal != nil {
			return TypeDecimal
		}
		if lt.Integer != nil {
			if lt.Integer.BitWidth <= 32 {
				return TypeInt32
			}
			return TypeInt64
		}
	}

	switch t.Kind() {
	case goparquet.Boolean:
		return TypeBool
	case goparquet.Int32:
		return TypeInt32
	case goparquet.Int64:
		return TypeInt64
	case goparquet.Float:
		return TypeFloat32
	case goparquet.Double:
		return TypeFloat64
	case goparquet.ByteArray, goparquet.FixedLenByteArray:
		return TypeString
	default:
		return TypeString
	}
}

