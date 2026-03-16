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

		cs := ColumnStats{HasStats: ci.NumPages() > 0}
		if cs.HasStats {
			cs.NullCount = int64(ci.NullCount(0))
		}
		stats.Columns[colName] = cs
	}

	return stats
}

// ReadRows reads all rows from the Parquet file, optionally selecting only specific columns.
func (r *Reader) ReadRows(selectedColumns []string) ([]map[string]any, error) {
	pr := goparquet.NewReader(r.file)
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

	if len(selectedColumns) > 0 {
		selected := make(map[string]bool, len(selectedColumns))
		for _, c := range selectedColumns {
			selected[c] = true
		}
		for i, row := range rows {
			filtered := make(map[string]any, len(selectedColumns))
			for k, v := range row {
				if selected[k] {
					filtered[k] = v
				}
			}
			rows[i] = filtered
		}
	}

	return rows, nil
}

// ReadRowGroup reads all rows from a specific row group.
func (r *Reader) ReadRowGroup(index int, selectedColumns []string) ([]map[string]any, error) {
	rgs := r.file.RowGroups()
	if index < 0 || index >= len(rgs) {
		return nil, fmt.Errorf("row group index %d out of range [0, %d)", index, len(rgs))
	}

	rg := rgs[index]
	pr := goparquet.NewRowGroupReader(rg)
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

	if len(selectedColumns) > 0 {
		selected := make(map[string]bool, len(selectedColumns))
		for _, c := range selectedColumns {
			selected[c] = true
		}
		for i, row := range rows {
			filtered := make(map[string]any, len(selectedColumns))
			for k, v := range row {
				if selected[k] {
					filtered[k] = v
				}
			}
			rows[i] = filtered
		}
	}

	return rows, nil
}

func extractSchema(f *goparquet.File) Schema {
	var columns []Column
	collectLeafColumns(f.Root(), &columns)
	return Schema{Columns: columns}
}

func collectLeafColumns(col *goparquet.Column, columns *[]Column) {
	if col.Leaf() {
		name := flattenColumnPath(col)
		typ := goTypeToTypeID(col)
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

func goTypeToTypeID(col *goparquet.Column) TypeID {
	lt := col.Type().LogicalType()
	if lt != nil {
		if lt.UTF8 != nil {
			return TypeString
		}
		if lt.Timestamp != nil {
			return TypeTimestamp
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

	switch col.Type().Kind() {
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

