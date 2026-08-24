package scan

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// ReadFileBatches reads all row groups from a Parquet file into separate RecordBatches
// (one per row group). Supports column projection via selectedCols. Falls back to
// row-based reading for schemas containing Array or Map types.
func ReadFileBatches(reader *pqt.Reader, schema []pqt.Column, selectedCols []string) ([]*batch.RecordBatch, error) {
	return ReadFileBatchesShard(reader, schema, selectedCols, 0, 1)
}

// ReadFileBatchesShard is like ReadFileBatches but reads only the row-group
// slice assigned to one shard of a multi-task scan. With shardCount=1 the
// behavior is identical to ReadFileBatches (whole file). With shardCount>1
// the file's row groups are split evenly into shardCount disjoint ranges and
// shardIdx selects which range to read; the union over all shards equals the
// whole file.
//
// This is the primitive that lets a single compacted parquet file (e.g. SF10
// partsupp = 691 MB single file) fan out across N tasks without requiring
// the file to be physically chunked. The downstream broadcast-join chain then
// inherits parallelism through probe-split (which checks `len(probeFiles) >=
// 2`) because each shard task emits its own output file.
func ReadFileBatchesShard(reader *pqt.Reader, schema []pqt.Column, selectedCols []string, shardIdx, shardCount int) ([]*batch.RecordBatch, error) {
	if shardCount < 1 {
		shardCount = 1
	}
	if shardIdx < 0 || shardIdx >= shardCount {
		return nil, nil
	}

	readSchema := schema
	if len(selectedCols) > 0 {
		readSchema = projectSchema(schema, selectedCols)
	}

	// The row-based fallback path doesn't yet support row-group sharding —
	// callers needing Array/Map types must accept whole-file reads on shard 0
	// only, with empty results from other shards. None of our TPC-H scan
	// stages use these types, so this is safe in practice; revisit if a
	// future workload introduces sharded reads of Array/Map data.
	if HasUnsupportedColumnarTypes(readSchema) {
		if shardIdx != 0 {
			return nil, nil
		}
		return readFileBatchesViaRows(reader, readSchema, selectedCols)
	}

	fr := reader.FileReader()
	return ReadFileBatchesNativeShard(fr, schema, selectedCols, shardIdx, shardCount)
}

// readFileBatchesViaRows falls back to row-based reading for unsupported types.
func readFileBatchesViaRows(reader *pqt.Reader, readSchema []pqt.Column, selectedCols []string) ([]*batch.RecordBatch, error) {
	// Typed by the CALLER's schema: the file's own footer cannot express
	// the network types, BYTES or UUID, and the row reader would otherwise
	// hand FromRows a shape those vectors store as NULL.
	rows, err := reader.ReadRowsAs(readSchema, selectedCols)
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
// TypeDecimal is supported by the native reader.
//
// TypeRow is supported only when every field is a DIRECT PRIMITIVE LEAF.
// readRowGroupNative's leafByPath is keyed on a leaf's FULL path, and the
// ROW arm can only ever build a two-element one — {column, field}. That is
// the whole of what it can address: a field that is itself a ROW, ARRAY or
// MAP is a GROUP in the file, its leaves live one or more levels below that
// path, and the lookup missed — taking the "column absent from the file →
// all nulls" branch and answering `{a:"x", inner:{x:7}}` as `inner:<nil>`
// with no error (#448, the same silent-wrong-answer class as #425 one level
// down).
//
// Refusing the shape here is the fix rather than teaching the ROW arm to
// recurse, because the recursion is not one level of lookup: assembling a
// container needs the def/rep level walk the row reader's record assembler
// already does (nested_assembly.go), and the native reader has no level
// machinery at all — which is why ARRAY and MAP are refused outright. A ROW
// of ROW alone could be addressed by path, but ROW of ARRAY and ROW of MAP
// could not, so the predicate would still have to exist and the reader would
// gain a second partial assembler to keep in agreement with the first
// (ADR-0018 §3). The row reader has been correct for these shapes since the
// #409 nested-assembly rewrite; this routes to it.
//
// Nothing loses a read path by this. The planner decides between the eager
// native scan and the row fallback on parquet.Schema.HasNestedColumns, which
// refuses a table with ANY ROW column one layer earlier (plan.go Init,
// buildRGUnits, readBatchDirect), and the top-N late-materialization rewrite
// refuses one on the full table schema (topn_late_mat.go) — so
// ReadRowGroupNativeShaped, the late-mat decode, never sees a ROW at all.
// The worker's cachedFileStreamSource is the path that does, and it tests
// this predicate before opening a row-group iterator and falls back to
// ReadFileBatchesShard, which reads through the row reader.
func HasUnsupportedColumnarTypes(schema []pqt.Column) bool {
	for _, col := range schema {
		switch col.Type {
		case pqt.TypeArray, pqt.TypeMap:
			return true
		case pqt.TypeRow:
			for _, f := range col.Fields {
				switch f.Type {
				case pqt.TypeArray, pqt.TypeMap, pqt.TypeRow:
					return true
				}
			}
		}
	}
	return false
}

// StorageClass exposes storageClass to other packages. It is the file-vs-
// catalog compatibility relation the columnar decoder uses: when the file's
// type and the catalog's type share a storage class the page values are
// copied into the vector VERBATIM, and only a mismatch routes through
// copyNativeCoerced* (which converts values). Callers that want to reason
// about a parquet value without reading it — the planner's footer-statistics
// MIN/MAX path — need exactly this test.
func StorageClass(t pqt.TypeID) pqt.TypeID { return storageClass(t) }

// storageClass returns a normalized type representing the physical storage
// format used in a Vector. Types sharing a storage class have identical
// in-memory layout (e.g. TypeIPv4 and TypeInt64 both use Int64Data).
//
// It delegates to parquet.StorageClassOf because the ROW reader gates the
// same admission on the same relation (retypeFromCatalog), and two copies of
// it drift: which read path a query takes is decided by the SHAPE of the
// table's schema, not by the query, so a pairing one path copies and the
// other refuses is a divergence waiting for a schema change to expose it.
//
// The version that used to live here had a `default:` arm, and that arm is
// what made the pairing unsafe: DECIMAL (Int128 array), VECTOR (Float32Data,
// VectorDim wide) and the five bytes-backed types all landed in one class.
// A catalog STRING over a file DECIMAL therefore "matched", coerce stayed
// false, and copyNativeDataDirect switched on the FILE's type — indexing
// DecimalData on a vector that had only ever allocated a bytes arena. Sixteen
// of the 361 (catalog, file) pairs panicked that way, inside the scan
// errgroup, which in a worker process is the worker.
func storageClass(t pqt.TypeID) pqt.TypeID { return pqt.StorageClassOf(t) }

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
//
// A schema with an Array/Map column, or a ROW whose field is itself a
// container, is refused by ReadRowGroupNative (HasUnsupportedColumnarTypes,
// #448/#449) — routing here instead of erroring would either fail every
// DELETE/UPDATE against such a table or, before that guard existed, silently
// null out the nested column on readback. The row reader has no such
// restriction, and readFileBatchesViaRows(reader, schema, nil) reads the
// whole file into exactly one batch in file order, so the row indices the
// DML callers compute against it (delete markers, scalar UPDATE rewrites)
// line up the same way ReadRowGroupNative's batches would have.
func ReadFileColumnar(reader *pqt.Reader, schema []pqt.Column) (*batch.RecordBatch, error) {
	if HasUnsupportedColumnarTypes(schema) {
		batches, err := readFileBatchesViaRows(reader, schema, nil)
		if err != nil {
			return nil, err
		}
		if len(batches) == 0 {
			return nil, nil
		}
		return batches[0], nil
	}

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
