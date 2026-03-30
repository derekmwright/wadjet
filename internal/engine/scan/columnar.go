package scan

import (
	"strings"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	pqt "github.com/citc-tech/wadjet/internal/storage/parquet"
)

// ReadFileBatches reads all row groups from a Parquet file into separate RecordBatches
// (one per row group). Supports column projection via selectedCols. Falls back to
// row-based reading for schemas containing Array or Map types.
func ReadFileBatches(reader *pqt.Reader, schema []pqt.Column, selectedCols []string) ([]*batch.RecordBatch, error) {
	readSchema := schema
	if len(selectedCols) > 0 {
		readSchema = projectSchema(schema, selectedCols)
	}

	// Schemas with types unsupported by the native columnar reader fall back to rows.
	if HasUnsupportedColumnarTypes(readSchema) {
		return readFileBatchesViaRows(reader, readSchema, selectedCols)
	}

	fr := reader.FileReader()
	return ReadFileBatchesNative(fr, schema, selectedCols)
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

// HasUnsupportedColumnarTypes returns true if any column uses a type the
// native columnar reader cannot handle (Array, Map).
// TypeRow is supported: child fields are read as separate leaf column chunks.
// TypeDecimal is supported by the native reader.
func HasUnsupportedColumnarTypes(schema []pqt.Column) bool {
	for _, col := range schema {
		switch col.Type {
		case pqt.TypeArray, pqt.TypeMap:
			return true
		}
	}
	return false
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

// findParquetColumnByPath finds a leaf column whose path ends with
// [..., parentName, childName]. Used to locate ROW child fields in
// the flattened Parquet column list.
func findParquetColumnByPath(pqCols [][]string, parentName, childName string) int {
	for i, path := range pqCols {
		if len(path) >= 2 && path[len(path)-2] == parentName && path[len(path)-1] == childName {
			return i
		}
	}
	return -1
}

// ReadFileColumnar reads all row groups from a Parquet reader into a single RecordBatch.
// Used by the DML executor to read entire files for DELETE/UPDATE operations.
func ReadFileColumnar(reader *pqt.Reader, schema []pqt.Column) (*batch.RecordBatch, error) {
	fr := reader.FileReader()

	var batches []*batch.RecordBatch
	for rgIdx := 0; rgIdx < fr.NumRowGroups(); rgIdx++ {
		b, err := ReadRowGroupNative(fr, rgIdx, schema, nil)
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
