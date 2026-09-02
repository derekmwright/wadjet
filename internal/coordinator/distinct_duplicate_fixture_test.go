package coordinator

import (
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The DUPLICATE-SPREAD fixture (#703): 7 500 rows, three distinct values of
// every column, every value present in every BATCH.
//
// It exists because none of #703's first five gate cells could tell dedup from
// no dedup under morsel parallelism. `Pipeline.runParallel` takes one warm-up
// batch and returns serially if the source is then exhausted, so a 9-row or
// 40-row fixture NEVER CLONES; and the one multi-batch cell in the first cut
// (`SUM(DISTINCT c_i32)` over typemx) has no duplicates at all — count(c_i32)
// and count(distinct c_i32) are both 4828, so the distinct and the plain sum
// are the same number and the cell cannot fail either way. That is
// correctness-fix protocol rule 2 exactly: a fixture whose values cannot
// distinguish the two rules passes for the wrong reason.
//
// What the shape has to produce is the SAME value arriving in more than one
// clone, which needs duplicates on both sides of a split the scheduler makes.
// Four batches cycling through three values gives that on every run, and two
// group keys (n % 2) give it for the grouped form too, where the partitioned
// path (WADJET_PARTITIONED_AGG=1) would otherwise hide it by giving each key
// one owner.
//
// PostgreSQL 17 over the same rows (oracle container, --locale=C):
//
//	SUM(DISTINCT a)   16.24        AVG(DISTINCT a)   5.4133333333333333
//	COUNT(DISTINCT a) 3            MIN(DISTINCT a)   -0.01
//	SUM(DISTINCT i)   33           SUM(DISTINCT f)   4.5
//	per group (g = 0, 1): 16.24 / 5.4133333333333333 / 3
//	SUM(i) = 82500 over 7 500 rows
const rvdTable = "revdup"

const (
	// 7 500 rows is four 2048-row batches, which is what the clone split
	// needs; tmdWriteTables spreads them over its usual four files for the
	// DAG arms.
	rvdRows     = 7500
	rvdGroups   = 2
	rvdDistinct = 3
)

func rvdSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "g", Type: parquet.TypeInt32},
		{Name: "a", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
		{Name: "i", Type: parquet.TypeInt64, Nullable: true},
		{Name: "f", Type: parquet.TypeFloat64, Nullable: true},
	}}
}

// rvdData returns every row. The three values cycle, so every batch and every
// file carries all of them and a clone that receives any slice sees the
// duplicates.
func rvdData() []map[string]any {
	decs := []int64{1275, 350, -1} // 12.75, 3.50, -0.01 at scale 2
	ints := []int64{10, 20, 3}
	flts := []float64{1.5, 2.0, 1.0}
	rows := make([]map[string]any, 0, rvdRows)
	for n := 0; n < rvdRows; n++ {
		k := n % rvdDistinct
		rows = append(rows, map[string]any{
			"g": int32(n % rvdGroups),
			"a": dbpDec(decs[k]),
			"i": ints[k],
			"f": flts[k],
		})
	}
	return rows
}
