package wadjet

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A DML statement that FAILS must leave the row set exactly as it found it.
//
// executeUpdate wrote a file's delete markers and re-ingested the updated rows
// afterwards. That was harmless while every SET conversion succeeded, and
// became data loss the moment one could refuse: with the #647 ingest check in
// place, `UPDATE u SET d = 99999999999999999999.99` answered 22003 with the
// matched rows ALREADY DELETED, so three failed UPDATEs emptied a three-row
// table. The parent defect stored the wrong value; this one lost the row.
//
// The fix has two halves and this test sees both: the SET values are resolved
// against the column's declared (p, s) BEFORE the loop touches a file, and the
// re-ingest now precedes the marker commit so no future failable conversion
// can reopen the window.
func TestFailedUpdateLeavesTheRowSetUnchanged(t *testing.T) {
	cases := []struct {
		name  string
		set   string
		state string
	}{
		{name: "decimal past the declared precision", set: "d = 99999999999999999999.99", state: "22003"},
		{name: "decimal exponent past the precision", set: "d = 1e40", state: "22003"},
		{name: "decimal rounding into the overflow", set: "d = 9999999.999", state: "22003"},
		{name: "decimal text naming no number", set: "d = 'abc'", state: "22P02"},
		{name: "decimal NaN", set: "d = 'NaN'", state: "22003"},
		{name: "decimal Infinity", set: "d = 'Infinity'", state: "22003"},
		// Not a DECIMAL at all: the same window, reached through the DATE
		// converter that has always been able to refuse (#560).
		{name: "unparseable date", set: "dt = 'not-a-date'"},
		{name: "nonexistent calendar date", set: "dt = '2026-02-30'"},
		// And a conversion convertValue itself refuses.
		{name: "port out of range", set: "p = 99999"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			schema := parquet.Schema{Columns: []parquet.Column{
				{Name: "id", Type: parquet.TypeInt64},
				{Name: "d", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
				{Name: "dt", Type: parquet.TypeDate, Nullable: true},
				{Name: "p", Type: parquet.TypePort, Nullable: true},
			}}
			if err := db.CreateTable(ctx, "u", schema, nil); err != nil {
				t.Fatal(err)
			}
			for i := 1; i <= 3; i++ {
				sql := fmt.Sprintf("INSERT INTO u (id, d, dt, p) VALUES (%d, %d.50, '2026-01-0%d', %d)", i, i, i, 80+i)
				if _, err := db.Execute(ctx, sql); err != nil {
					t.Fatalf("seeding row %d: %v", i, err)
				}
			}
			before := decimalRowSet(t, db, ctx)
			if len(before) != 3 {
				t.Fatalf("seeded %d rows, want 3", len(before))
			}

			// Three failed UPDATEs in a row: one is a lost row, three used to
			// be the whole table.
			for attempt := 1; attempt <= 3; attempt++ {
				_, err := db.Execute(ctx, "UPDATE u SET "+tc.set+" WHERE id = 1")
				if err == nil {
					t.Fatalf("attempt %d: UPDATE SET %s succeeded; want a refusal", attempt, tc.set)
				}
				if tc.state != "" {
					if got := sqlerr.StateOf(err); got != tc.state {
						t.Fatalf("attempt %d: SQLSTATE %q, want %q (err: %v)", attempt, got, tc.state, err)
					}
				}
				after := decimalRowSet(t, db, ctx)
				if len(after) != len(before) {
					t.Fatalf("attempt %d: %d rows after a REFUSED UPDATE, want %d — the statement "+
						"deleted what it would not replace\n  before: %v\n  after:  %v",
						attempt, len(after), len(before), before, after)
				}
				for i := range before {
					if after[i] != before[i] {
						t.Fatalf("attempt %d: row %d is %q after a REFUSED UPDATE, want %q",
							attempt, i, after[i], before[i])
					}
				}
			}

			// And the table still takes a good UPDATE afterwards.
			if _, err := db.Execute(ctx, "UPDATE u SET d = 7.25 WHERE id = 1"); err != nil {
				t.Fatalf("a valid UPDATE after the refused ones: %v", err)
			}
			after := decimalRowSet(t, db, ctx)
			if len(after) != 3 {
				t.Fatalf("%d rows after the successful UPDATE, want 3: %v", len(after), after)
			}
		})
	}
}

// decimalRowSet reads the whole table as sorted "id=<id> d=<d>" strings, so a
// lost or duplicated row is visible in the comparison.
func decimalRowSet(t *testing.T, db *DB, ctx context.Context) []string {
	t.Helper()
	res, err := db.Query(ctx, "SELECT id, d FROM u")
	if err != nil {
		t.Fatalf("reading the table back: %v", err)
	}
	out := make([]string, 0, len(res.Rows))
	for _, r := range res.Rows {
		out = append(out, fmt.Sprintf("id=%v d=%v", r["id"], r["d"]))
	}
	sort.Strings(out)
	return out
}

// The same window in MERGE: its delete markers for the matched target rows
// committed before the replacement rows were ingested.
func TestFailedMergeLeavesTheTargetUnchanged(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	target := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "u", target, nil); err != nil {
		t.Fatal(err)
	}
	src := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeString, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "s", src, nil); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		if _, err := db.Execute(ctx, fmt.Sprintf("INSERT INTO u (id, d) VALUES (%d, %d.50)", i, i)); err != nil {
			t.Fatal(err)
		}
	}
	// The source carries a value the target's DECIMAL(9,2) cannot hold.
	if _, err := db.Execute(ctx, "INSERT INTO s (id, v) VALUES (1, '99999999999999999999.99')"); err != nil {
		t.Fatal(err)
	}
	before := decimalRowSet(t, db, ctx)

	_, err = db.Execute(ctx,
		"MERGE INTO u USING s ON u.id = s.id WHEN MATCHED THEN UPDATE SET d = s.v")
	if err == nil {
		t.Fatal("MERGE of a value the target cannot hold succeeded; want a refusal")
	}
	after := decimalRowSet(t, db, ctx)
	if len(after) != len(before) {
		t.Fatalf("%d rows after a REFUSED MERGE, want %d — it deleted what it would not replace\n"+
			"  before: %v\n  after:  %v", len(after), len(before), before, after)
	}
	for i := range before {
		if after[i] != before[i] {
			t.Fatalf("row %d is %q after a REFUSED MERGE, want %q", i, after[i], before[i])
		}
	}
}
