package tpch

import (
	"context"
	"testing"
)

// PostgreSQL answers `SELECT upper(a), upper(b)` with two columns both called
// `upper`, and #513 made this engine agree. A map keyed by column name cannot
// hold both, so the embedded API carries a POSITIONAL form alongside it and
// the pgwire DataRow path reads that. This test is the embedded half of the
// contract; the wire half is the PostgreSQL oracle's DuplicateName* entries,
// which compare cells positionally against a live server.
//
// The values below were read off postgres:17-alpine over the same fixture:
// the two columns are the two DIFFERENT source columns, never one twice.
func TestDuplicateOutputNamesKeepBothValues(t *testing.T) {
	ctx := context.Background()
	db := setupTPCH(t, SF001)

	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"two calls of one function",
			`SELECT UPPER(n_name), UPPER(n_comment) FROM nation ORDER BY n_nationkey`},
		// The ORDER BY term is not a select item, so the hidden-sort-key trim
		// runs between the projection and the result — the projection that
		// used to copy its columns by NAME, which is where the loss was.
		{"under a hidden sort key",
			`SELECT UPPER(n_name), UPPER(n_comment) FROM nation ORDER BY n_regionkey, n_nationkey`},
		{"a computed column and an alias",
			`SELECT UPPER(n_name), n_comment AS upper FROM nation ORDER BY n_nationkey`},
		{"an explicit duplicate alias",
			`SELECT UPPER(n_name) AS u, UPPER(n_comment) AS u FROM nation ORDER BY n_nationkey`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := db.Query(ctx, tc.sql)
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			if len(res.Columns) != 2 {
				t.Fatalf("got %d columns, want 2: %v", len(res.Columns), res.Columns)
			}
			if res.Columns[0] != res.Columns[1] {
				t.Fatalf("columns %v are not the duplicate-name shape this test is about", res.Columns)
			}
			// The map cannot represent the answer, so the positional form
			// MUST be present — that is the whole contract.
			if len(res.RowValues) != len(res.Rows) {
				t.Fatalf("RowValues has %d rows and Rows has %d — a result whose column names "+
					"collide must carry the positional form", len(res.RowValues), len(res.Rows))
			}
			if len(res.RowValues) == 0 {
				t.Fatalf("no rows")
			}
			same := 0
			for i, cells := range res.RowValues {
				if len(cells) != 2 {
					t.Fatalf("row %d has %d cells, want 2", i, len(cells))
				}
				if cells[0] == cells[1] {
					same++
				}
			}
			if same == len(res.RowValues) {
				t.Errorf("every row has cell 0 == cell 1 (%d rows) — the second column carries "+
					"the first one's value, which is what a name-keyed copy does", same)
			}
		})
	}
}

// TestUniqueOutputNamesNeedNoPositionalForm is the other half of the rule, and
// the reason it costs nothing: where the names are unique the map IS the
// positional form, so nothing extra is materialized. CollectSink has been
// measured at 68% of inuse_space on large results — a []any per row for every
// query would not be free.
func TestUniqueOutputNamesNeedNoPositionalForm(t *testing.T) {
	ctx := context.Background()
	db := setupTPCH(t, SF001)
	res, err := db.Query(ctx, `SELECT n_name, n_comment FROM nation ORDER BY n_nationkey`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(res.RowValues) != 0 {
		t.Errorf("RowValues materialized %d rows for a result with unique column names %v",
			len(res.RowValues), res.Columns)
	}
	// Cells still answers positionally, from the map.
	got := res.Cells(0)
	if len(got) != 2 || got[0] == got[1] {
		t.Errorf("Cells(0) = %v, want two distinct cells", got)
	}
}
