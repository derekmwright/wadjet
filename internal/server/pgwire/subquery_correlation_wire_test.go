package pgwire

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// WHAT THE WIRE CARRIES FOR #1044 AND #1045, IN BOTH RESULT FORMATS.
//
// #1044 is an OID as much as a value: the filing's own shape answered the
// EMPTY STRING under OID 701 (float8) where PostgreSQL 17.11 answers 18 under
// OID 20 (bigint) and 1026 under OID 1700 (numeric). A value oracle cannot see
// a right value under a wrong OID, which is why this gate is here beside the
// five-arm one in internal/coordinator — the coordinator gate reads Go boxes,
// and the declaration a client acts on is the RowDescription's.
//
// Both format codes are exercised because the declaration decides how the
// BINARY encoding is written: an int4 under OID 20 is four bytes a client
// reads as eight.
//
// Every want was measured on live PostgreSQL 17.11 over the same three rows
// setupRealDB writes (id 1,2,3; visits 100,42,200), in this arc's ROUND0.
func TestASubqueryReadsTheRowItIsCorrelatedOnOnTheWire(t *testing.T) {
	_, srv := setupRealDB(t)
	conn := connectPgconn(t, srv.Addr())
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		sql  string
		// oids is PostgreSQL 17.11's RowDescription, column for column.
		oids []uint32
		// text is the result in the TEXT format, rows joined by "|" and
		// columns by ",". NULL is spelled <null> so an empty string — the
		// value #1044 answered — cannot pass for one.
		text string
		// wantErr names a substring of the refusal every format must raise.
		wantErr string
		// pinOIDs records a DECLARATION this engine gets wrong, where the
		// VALUE is right. The cell then asserts the divergence: the day it
		// declares oids this FAILS and the pin is deleted.
		pinOIDs []uint32
		pinWhy  string
	}{
		// --- #1044: the filing's own shape and its CTE twin ---------------
		{name: "the_filing_shape_derived_twins",
			sql: `SELECT SUM(a.v) AS a, SUM(b.v) AS b ` +
				`FROM (SELECT (SELECT u.x) AS v FROM (SELECT id AS x FROM users) u) a ` +
				`CROSS JOIN (SELECT (SELECT u.x) AS v FROM (SELECT visits AS x FROM users) u) b`,
			oids: []uint32{20, 1700}, text: `18,1026`},
		{name: "the_filing_shape_cte_twins",
			sql: `WITH a AS (SELECT id AS x FROM users), b AS (SELECT visits AS x FROM users) ` +
				`SELECT SUM(p.v) AS a, SUM(q.v) AS b ` +
				`FROM (SELECT (SELECT u.x) AS v FROM a u) p ` +
				`CROSS JOIN (SELECT (SELECT u.x) AS v FROM b u) q`,
			oids: []uint32{20, 1700}, text: `18,1026`},
		{name: "a_derived_tables_own_output_alias_declares_int4",
			sql:  `SELECT (SELECT u.x) AS v FROM (SELECT id AS x FROM users) u ORDER BY 1`,
			oids: []uint32{23}, text: `1|2|3`},
		{name: "a_bigint_column_declares_int8",
			sql:  `SELECT (SELECT u.x) AS v FROM (SELECT visits AS x FROM users) u ORDER BY 1`,
			oids: []uint32{20}, text: `42|100|200`},
		{name: "a_string_column_declares_text",
			sql:  `SELECT (SELECT u.nm) AS v FROM (SELECT name AS nm FROM users) u ORDER BY 1`,
			oids: []uint32{25}, text: `alice|bob|carol`},
		{name: "aggregated_over_the_alias",
			sql:  `SELECT SUM((SELECT u.x)) AS v FROM (SELECT id AS x FROM users) u`,
			oids: []uint32{20}, text: `6`},
		// The boundary, on the wire: an empty result is the scalar NULL, and
		// NULL is what the wire must carry — not the empty string that made
		// #1044 silent.
		{name: "boundary_a_false_where_is_null",
			sql:  `SELECT (SELECT u.id WHERE 1=0) AS v FROM users u ORDER BY 1`,
			oids: []uint32{23}, text: `<null>|<null>|<null>`,
			pinOIDs: []uint32{25},
			pinWhy: "a WHERE clause keeps this a subquery (#1044's boundary), and a " +
				"subquery whose body names only the OUTER query cannot be planned " +
				"standalone, so its declaration falls to the TEXT fallback. The VALUE " +
				"is right on every arm; only the OID diverges, as it did before #1044. " +
				"Closing it needs the declaration walk to type a subquery against the " +
				"enclosing scope"},
		{name: "boundary_two_columns_is_refused",
			sql:     `SELECT (SELECT u.id, u.visits) AS v FROM users u`,
			wantErr: `only one column`},
		// --- #1045: refused, with PostgreSQL's answer recorded beside it ---
		{name: "a_window_over_the_outer_row_is_refused", // PostgreSQL: 2, 3, 4
			sql: `SELECT id,(SELECT 1+SUM(u.id) OVER () FROM users x WHERE x.id=1) AS v ` +
				`FROM users u ORDER BY id`,
			wantErr: `correlated on u.id`},
		{name: "an_uncorrelated_window_in_a_correlated_subquery_is_refused", // PostgreSQL: 1, 2, 3
			sql: `SELECT id,(SELECT SUM(x.id) OVER () FROM users x WHERE x.id = u.id) AS v ` +
				`FROM users u ORDER BY id`,
			wantErr: `holds a window function`},
		// --- the controls -------------------------------------------------
		{name: "ctl_an_uncorrelated_window_in_a_subquery",
			sql: `SELECT id,(SELECT 1+SUM(x.id) OVER () FROM users x WHERE x.id=1) AS v ` +
				`FROM users u ORDER BY id`,
			oids: []uint32{23, 20}, text: `1,2|2,2|3,2`},
		{name: "ctl_a_subquery_with_its_own_from",
			sql: `SELECT (SELECT MAX(y.visits) FROM users y WHERE y.id = u.x) AS v ` +
				`FROM (SELECT id AS x FROM users) u ORDER BY 1`,
			oids: []uint32{20}, text: `42|100|200`},
	} {
		for _, format := range []int16{0, 1} {
			t.Run(fmt.Sprintf("%s/format%d", tc.name, format), func(t *testing.T) {
				res := conn.ExecParams(ctx, tc.sql, nil, nil, nil, []int16{format}).Read()
				if tc.wantErr != "" {
					if res.Err == nil {
						t.Fatalf("ANSWERED %v where the wire must carry a refusal containing %q\n  SQL: %s",
							res.Rows, tc.wantErr, tc.sql)
					}
					if !strings.Contains(res.Err.Error(), tc.wantErr) {
						t.Errorf("refusal\n  got  %v\n  want a sentence containing %q\n  SQL: %s",
							res.Err, tc.wantErr, tc.sql)
					}
					if state := pgErrCode(res.Err); state != "0A000" && state != "42601" {
						t.Errorf("refusal carries SQLSTATE %q, want 0A000 (or 42601 for the "+
							"column-count refusal)\n  SQL: %s", state, tc.sql)
					}
					return
				}
				if res.Err != nil {
					t.Fatalf("%v\n  SQL: %s\n  PostgreSQL 17.11 answers %s", res.Err, tc.sql, tc.text)
				}
				var oids []uint32
				for _, f := range res.FieldDescriptions {
					oids = append(oids, f.DataTypeOID)
				}
				switch {
				case tc.pinOIDs != nil && sameOIDs(oids, tc.oids):
					t.Errorf("RowDescription now declares PostgreSQL 17.11's %v, so this pin is "+
						"FIXED: delete pinOIDs from the cell.\n  pinned reason: %s\n  SQL: %s",
						tc.oids, tc.pinWhy, tc.sql)
				case tc.pinOIDs != nil && !sameOIDs(oids, tc.pinOIDs):
					t.Errorf("RowDescription declares %v, which is neither PostgreSQL 17.11's %v "+
						"nor the pinned %v\n  SQL: %s", oids, tc.oids, tc.pinOIDs, tc.sql)
				case tc.pinOIDs == nil && !sameOIDs(oids, tc.oids):
					t.Errorf("RowDescription declares %v, want %v (PostgreSQL 17.11)\n  SQL: %s",
						oids, tc.oids, tc.sql)
				}
				if format == 0 {
					if got := renderTextRows(res.Rows); got != tc.text {
						t.Errorf("text format\n  got  %s\n  want %s (PostgreSQL 17.11)\n  SQL: %s",
							got, tc.text, tc.sql)
					}
					return
				}
				// BINARY. The bytes are the declaration's, so the width is the
				// assertion: an int4 answered under OID 20 would arrive as
				// four bytes where a client reads eight.
				for _, row := range res.Rows {
					for i, cell := range row {
						if cell == nil {
							continue
						}
						declared := tc.oids
						if tc.pinOIDs != nil {
							declared = tc.pinOIDs
						}
						if want := binaryWidth(declared[i]); want > 0 && len(cell) != want {
							t.Errorf("binary column %d is %d bytes under OID %d, want %d\n  SQL: %s",
								i, len(cell), tc.oids[i], want, tc.sql)
						}
					}
				}
			})
		}
	}
}

// binaryWidth is the fixed binary width of the OIDs this gate declares, or 0
// for a variable-width one (numeric and text).
func binaryWidth(oid uint32) int {
	switch oid {
	case 23:
		return 4
	case 20:
		return 8
	}
	return 0
}

func sameOIDs(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// renderTextRows joins a text-format result, spelling NULL as <null> so the
// EMPTY STRING #1044 answered cannot pass for one.
func renderTextRows(rows [][][]byte) string {
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		cols := make([]string, 0, len(row))
		for _, c := range row {
			if c == nil {
				cols = append(cols, "<null>")
				continue
			}
			cols = append(cols, string(c))
		}
		out = append(out, strings.Join(cols, ","))
	}
	return strings.Join(out, "|")
}
