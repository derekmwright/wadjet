package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// #416 through the embedded API. A zero-row result took its schema from the
// first batch it consumed — of which there is none — so `SELECT a, b FROM t
// WHERE false` declared STRING for every column it could not find in the
// catalog by name, and any column the catalog fallback missed (an aggregate's
// output, an alias) came back as text.
//
// The assertion is the invariant, not a table: the same statement with a
// predicate that matches and one that matches nothing must declare the same
// columns, names and types alike. A client asking for the shape of a result
// gets one answer whether or not there are rows in it.
func TestEmptyResultDeclaresSameColumnsAsNonEmpty(t *testing.T) {
	ctx := context.Background()
	db := mbOpen(t)

	cases := []struct {
		name string
		tmpl string // one %s: the WHERE predicate
	}{
		{"projection", "SELECT id, c_str, c_dec, c_ipv6 FROM mbtypes WHERE %s"},
		{"projection_ordered", "SELECT id, c_str, c_dec FROM mbtypes WHERE %s ORDER BY id DESC"},
		{"aliases", "SELECT id AS the_id, c_i32 AS small, c_bool AS flag FROM mbtypes WHERE %s"},
		{"grouped_aggregate", "SELECT g AS grp, COUNT(*) AS n, MIN(c_ts) AS lo, MAX(c_str) AS hi " +
			"FROM mbtypes WHERE %s GROUP BY g ORDER BY grp"},
		{"computed", "SELECT id + 1 AS next, UPPER(c_str) AS up FROM mbtypes WHERE %s"},
		{"limit", "SELECT id, c_str FROM mbtypes WHERE %s ORDER BY id LIMIT 5"},

		// Widened during review (#416): expression-heavy and mixed-type
		// shapes the cases above did not exercise.
		{"agg_sum_avg", "SELECT g, SUM(id) AS s, AVG(id) AS a FROM mbtypes WHERE %s GROUP BY g"},
		{"agg_count_distinct", "SELECT g, COUNT(*) AS n, COUNT(DISTINCT c_str) AS d FROM mbtypes WHERE %s GROUP BY g"},
		{"agg_minmax_mixed", "SELECT g, MIN(c_str) AS lo, MAX(c_ipv6) AS hi, MIN(c_dur) AS d, " +
			"MAX(c_uuid) AS u, MIN(c_bool) AS b FROM mbtypes WHERE %s GROUP BY g"},
		{"agg_minmax_mixed2", "SELECT g, MIN(c_mac) AS m, MAX(c_port) AS p, MIN(c_proto) AS pr, " +
			"MAX(c_dec) AS de, MIN(c_bytes) AS by FROM mbtypes WHERE %s GROUP BY g"},
		{"cast", "SELECT CAST(id AS BIGINT) AS ci, CAST(c_str AS VARCHAR) AS cs, CAST(c_f64 AS DOUBLE) AS cf " +
			"FROM mbtypes WHERE %s"},
		{"arith", "SELECT id + 1 AS pi, id * 2 AS mi, c_f64 / 2 AS df, id - c_i32 AS sub FROM mbtypes WHERE %s"},
		{"concat", "SELECT c_str || '-x' AS cat, c_str || c_str AS cat2 FROM mbtypes WHERE %s"},
		{"case_numeric", "SELECT CASE WHEN id > 10 THEN id ELSE 0 END AS cn, " +
			"CASE WHEN id > 10 THEN c_f64 ELSE 0.0 END AS cf FROM mbtypes WHERE %s"},
		{"coalesce", "SELECT COALESCE(c_str, 'x') AS co, COALESCE(c_i32, 0) AS ci FROM mbtypes WHERE %s"},
		{"predicate_bool", "SELECT id > 3 AS gt, id IS NULL AS isn, c_str LIKE 'str%%' AS lk FROM mbtypes WHERE %s"},
		{"container_passthrough", "SELECT id, c_arr FROM mbtypes WHERE %s"},

		// F3: a Window node had no arm in emittedColTypes, so EVERY column of
		// a zero-row window query — including a plain INT64 passthrough —
		// declared STRING. ROW_NUMBER exercises the name-list branch,
		// SUM OVER exercises the aggregate-window float64 default, and LAG
		// with a default exercises the value-function branch that copies its
		// argument column's type.
		{"window_row_number", "SELECT id, ROW_NUMBER() OVER (PARTITION BY g ORDER BY id) AS rn FROM mbtypes WHERE %s"},
		{"window_sum_over", "SELECT id, SUM(id) OVER (PARTITION BY g) AS sw FROM mbtypes WHERE %s"},
		{"window_lag_default", "SELECT id, LAG(id, 1, -1) OVER (ORDER BY id) AS lg FROM mbtypes WHERE %s"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			full, err := db.Query(ctx, fmt.Sprintf(tc.tmpl, "id < 100"))
			if err != nil {
				t.Fatalf("non-empty arm: %v", err)
			}
			if len(full.Rows) == 0 {
				t.Fatal("the non-empty arm returned no rows; the reference is meaningless")
			}
			empty, err := db.Query(ctx, fmt.Sprintf(tc.tmpl, "id < 0"))
			if err != nil {
				t.Fatalf("empty arm: %v", err)
			}
			if len(empty.Rows) != 0 {
				t.Fatalf("the empty arm returned %d rows", len(empty.Rows))
			}
			if strings.Join(empty.Columns, ",") != strings.Join(full.Columns, ",") {
				t.Errorf("column NAMES differ:\n empty %v\n full  %v", empty.Columns, full.Columns)
			}
			if got, want := describeMetas(empty), describeMetas(full); got != want {
				t.Errorf("column TYPES differ between the empty and non-empty arms:\n"+
					" empty %s\n full  %s", got, want)
			}
		})
	}
}

func describeMetas(r *QueryResult) string {
	parts := make([]string, len(r.ColumnMetas))
	for i, m := range r.ColumnMetas {
		parts[i] = m.Name + ":" + m.TypeID.String()
	}
	return strings.Join(parts, ",")
}
