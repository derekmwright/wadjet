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
