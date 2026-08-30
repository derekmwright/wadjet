package coordinator

import "github.com/derekmwright/wadjet/internal/storage/parquet"

// The GROUPING-COVERAGE fixture: a table whose columns are DELIMITED
// identifiers that read back as something other than a name.
//
// SQL's grouping rule is that every column a SELECT item reads must itself be
// grouped, and the check that enforces it compares a select item against the
// GROUP BY terms. Both sides are expressions, and a delimited identifier is
// the one spelling where the TEXT of a term and the MEANING of it come apart:
// `GROUP BY "g + 1"` groups by one column and says nothing about `g`, so
// `SELECT g + 1` beside it is ungrouped and PostgreSQL refuses it with 42803.
//
// gcovArith is that column. gcovWords is the same trap without arithmetic —
// `"g plus 1"` is not an expression in any dialect — so a fix that only
// special-cases operators is visibly not enough.
//
// g and h are separately groupable and j is never grouped, so each direction
// of the check has a column to be right or wrong about, and the delimited
// columns' values are far from any group key's: a query that answers where it
// should refuse answers a WRONG NUMBER, not a plausible one.
const (
	gcovTable = "gcov"
	gcovArith = "g + 1"
	gcovWords = "g plus 1"
)

func gcovSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "g", Type: parquet.TypeInt32, Nullable: true},
		{Name: "h", Type: parquet.TypeInt32, Nullable: true},
		{Name: "j", Type: parquet.TypeInt32, Nullable: true},
		{Name: gcovArith, Type: parquet.TypeInt64, Nullable: true},
		{Name: gcovWords, Type: parquet.TypeInt64, Nullable: true},
	}}
}

// gcovData is 60 rows: g cycles 0..2 (so `g + 1` is 1..3 and the delimited
// column, which counts by hundreds, can never be mistaken for it), h cycles
// 0..1, j is distinct per row.
func gcovData() []map[string]any {
	rows := make([]map[string]any, 0, 60)
	for i := 0; i < 60; i++ {
		rows = append(rows, map[string]any{
			"id":      int64(i),
			"g":       int32(i % 3),
			"h":       int32(i % 2),
			"j":       int32(i),
			gcovArith: int64(100 + i),
			gcovWords: int64(500 + i),
		})
	}
	return rows
}
