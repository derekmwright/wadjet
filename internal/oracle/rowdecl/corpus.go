// Package rowdecl provides the fixed-ROW declaration position corpus.
package rowdecl

import (
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func Fields() []parquet.Column {
	return []parquet.Column{{Name: "major", Type: parquet.TypeInt64}, {Name: "minor", Type: parquet.TypeInt64}, {Name: "patch", Type: parquet.TypeInt64}, {Name: "prerelease", Type: parquet.TypeString}, {Name: "build", Type: parquet.TypeString}}
}
func Value(id int64) map[string]any {
	return map[string]any{"major": id, "minor": int64(2), "patch": int64(3), "prerelease": "", "build": ""}
}
func Schema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{{Name: "id", Type: parquet.TypeInt64}, {Name: "v", Type: parquet.TypeString}, {Name: "r", Type: parquet.TypeRow, Fields: Fields()}}}
}
func Data() []map[string]any {
	var out []map[string]any
	for i := int64(1); i <= 3; i++ {
		out = append(out, map[string]any{"id": i, "v": fmt.Sprintf("%d.2.3", i), "r": Value(i)})
	}
	return out
}

type Cell struct {
	Name, SQL string
	Want      []string
	Row       bool
}

// Cells uses one producer spelling in every position. Expected strings are
// PostgreSQL 17.11 measurements; ROW values use Go's map spelling for the
// embedded adapter only. Wire assertions render the same measured composites.
func Cells(producer string) []Cell {
	row := func(i int64) string { return fmt.Sprint(Value(i)) }
	c := []Cell{
		{"projection", `SELECT $ AS p FROM a3b_rows WHERE id=1`, []string{row(1)}, true},
		{"join", `SELECT $ AS p FROM a3b_rows a JOIN a3b_rows b ON a.id=b.id WHERE a.id=1`, []string{row(1)}, true},
		{"cte", `WITH c AS (SELECT $ AS p FROM a3b_rows WHERE id=1) SELECT p FROM c`, []string{row(1)}, true},
		{"derived", `SELECT p FROM (SELECT $ AS p FROM a3b_rows WHERE id=1) d`, []string{row(1)}, true},
		{"lateral", `SELECT p FROM a3b_rows a CROSS JOIN LATERAL (SELECT $ AS p FROM a3b_rows WHERE id=1) d WHERE a.id=1`, []string{row(1)}, true},
		{"scalar_subquery", `SELECT (SELECT $ FROM a3b_rows WHERE id=1) AS p FROM a3b_rows WHERE id=2`, []string{row(1)}, true},
		{"window_partition", `SELECT $ AS p, COUNT(*) OVER (PARTITION BY $) AS n FROM a3b_rows ORDER BY id`, []string{row(1), row(2), row(3)}, true},
		{"window_order", `SELECT $ AS p, ROW_NUMBER() OVER (ORDER BY $) AS n FROM a3b_rows ORDER BY id`, []string{row(1), row(2), row(3)}, true},
		{"limit", `SELECT $ AS p FROM a3b_rows ORDER BY id LIMIT 1`, []string{row(1)}, true},
		{"case", `SELECT CASE WHEN id=1 THEN $ ELSE NULL END AS p FROM a3b_rows WHERE id=1`, []string{row(1)}, true},
		{"distinct", `SELECT DISTINCT $ AS p FROM a3b_rows`, []string{row(1), row(2), row(3)}, true},
		{"group", `SELECT $ AS p, COUNT(*) AS n FROM a3b_rows GROUP BY 1`, []string{row(1), row(2), row(3)}, true},
		{"union", `SELECT $ AS p FROM a3b_rows UNION SELECT $ AS p FROM a3b_rows`, []string{row(1), row(2), row(3)}, true},
		{"union_all", `SELECT $ AS p FROM a3b_rows WHERE id=1 UNION ALL SELECT $ AS p FROM a3b_rows WHERE id=2`, []string{row(1), row(2)}, true},
		{"intersect", `SELECT $ AS p FROM a3b_rows INTERSECT SELECT $ AS p FROM a3b_rows WHERE id=1`, []string{row(1)}, true},
		{"except", `SELECT $ AS p FROM a3b_rows EXCEPT SELECT $ AS p FROM a3b_rows WHERE id=1`, []string{row(2), row(3)}, true},
		{"count_distinct", `SELECT COUNT(*) AS p FROM (SELECT DISTINCT $ AS p FROM a3b_rows) d`, []string{"3"}, false},
		{"window_over_distinct", `SELECT p, COUNT(*) OVER () AS n FROM (SELECT DISTINCT $ AS p FROM a3b_rows) d`, []string{row(1), row(2), row(3)}, true},
		{"field_group", `SELECT ($).major AS p, COUNT(*) AS n FROM a3b_rows GROUP BY 1`, []string{"1", "2", "3"}, false},
		{"field_order", `SELECT ($).major AS p FROM a3b_rows ORDER BY ($).major`, []string{"1", "2", "3"}, false},
		{"field_filter", `SELECT ($).major AS p FROM a3b_rows WHERE ($).major=2`, []string{"2"}, false},
		{"derived_field_group", `SELECT (s).major AS p, COUNT(*) AS n FROM (SELECT $ AS s FROM a3b_rows) d GROUP BY 1`, []string{"1", "2", "3"}, false},
		{"derived_field_sum", `SELECT SUM((s).major) AS p FROM (SELECT $ AS s FROM a3b_rows) d`, []string{"6"}, false},
		{"hidden_row_order", `SELECT id AS p FROM a3b_rows ORDER BY $ DESC`, []string{"3", "2", "1"}, false},
		{"derived_row_order", `SELECT p FROM (SELECT $ AS p FROM a3b_rows) d ORDER BY p`, []string{row(1), row(2), row(3)}, true},
		{"derived_field_order", `SELECT (s).major AS p FROM (SELECT $ AS s FROM a3b_rows) d ORDER BY 1 DESC`, []string{"3", "2", "1"}, false},
		{"cte_field_group", `WITH d AS (SELECT $ AS s FROM a3b_rows) SELECT (s).major AS p,COUNT(*) AS n FROM d GROUP BY 1`, []string{"1", "2", "3"}, false},
		{"setop_field_group", `SELECT (s).major AS p,COUNT(*) AS n FROM (SELECT $ AS s FROM a3b_rows UNION SELECT $ AS s FROM a3b_rows) d GROUP BY 1`, []string{"1", "2", "3"}, false},
		{"derived_window_partition", `SELECT p,COUNT(*) OVER (PARTITION BY p) AS n FROM (SELECT $ AS p FROM a3b_rows) d`, []string{row(1), row(2), row(3)}, true},
	}
	// Function-argument scalar subqueries retain their existing local route:
	// the scalar resolver does not descend into function arguments.
	scalarName := "scalar_argument"
	scalarSQL := `SELECT r AS p FROM a3b_rows WHERE id=(SELECT MAX(id) FROM a3b_rows)`
	if producer != "r" {
		scalarName = "scalar_argument_routed"
		open := strings.IndexByte(producer, '(')
		arg := strings.TrimSuffix(producer[open+1:], ")")
		scalarSQL = "SELECT " + producer[:open] + "((SELECT MAX(" + arg + ") FROM a3b_rows)) AS p FROM a3b_rows WHERE id=1"
	}
	c = append(c, Cell{scalarName, scalarSQL, []string{row(3)}, true})
	for i := range c {
		p := producer
		if c[i].Name == "join" {
			if p == "r" {
				p = "a.r"
			} else {
				p = strings.ReplaceAll(strings.ReplaceAll(p, "(id)", "(a.id)"), "(v)", "(a.v)")
			}
		}
		c[i].SQL = strings.ReplaceAll(c[i].SQL, "$", p)
	}
	return c
}
