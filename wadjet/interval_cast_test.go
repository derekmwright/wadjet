// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestTextCastToIntervalIsTheLiteralsInterval pins round-3 review N2: a CAST
// of text to INTERVAL — `CAST('1 day' AS INTERVAL)`, `'1 day'::interval`, the
// shape a bound parameter takes — passed the text through uncast, so the
// TIMESTAMP shift it declares added ONE MILLISECOND. It now reads the text
// through the INTERVAL literal's own grammar and unit table; PostgreSQL 17.11
// values. And round-3 review N4: `ts - ts` (no INTERVAL type here: the
// documented milliseconds) declares double precision, not text.
func TestTextCastToIntervalIsTheLiteralsInterval(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.CreateTable(ctx, "iv", parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "d", Type: parquet.TypeDate, Nullable: true},
		{Name: "ts", Type: parquet.TypeTimestamp, Nullable: true},
	}}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, "INSERT INTO iv (id, d, ts) VALUES (1, '2026-03-03', '2026-03-03 10:00:00')"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ sql, want, state string }{
		{"SELECT CAST(ts + CAST('1 day' AS INTERVAL) AS TEXT) AS v FROM iv", "2026-03-04 10:00:00", ""},
		{"SELECT CAST(ts + '1 day'::interval AS TEXT) AS v FROM iv", "2026-03-04 10:00:00", ""},
		{"SELECT CAST(ts - CAST('2 hours' AS INTERVAL) AS TEXT) AS v FROM iv", "2026-03-03 08:00:00", ""},
		{"SELECT CAST(d + CAST('3 days' AS INTERVAL) AS TEXT) AS v FROM iv", "2026-03-06 00:00:00", ""},
		{"SELECT CAST(ts - CAST('90' AS INTERVAL) AS TEXT) AS v FROM iv", "2026-03-03 09:58:30", ""},
		{"SELECT CAST(ts + CAST('abc' AS INTERVAL) AS TEXT) AS v FROM iv", "", "22007"},
		{"SELECT CAST(5 AS INTERVAL) AS v", "", "42846"},
	} {
		res, err := db.Query(ctx, tc.sql)
		if tc.state != "" {
			if got := sqlerr.StateOf(err); got != tc.state {
				t.Errorf("%s: got %q (%v), want %s", tc.sql, got, err, tc.state)
			}
			continue
		}
		if err != nil || len(res.Rows) != 1 || fmt.Sprint(res.Rows[0]["v"]) != tc.want {
			t.Errorf("%s = %v (%v), PostgreSQL 17.11 %s", tc.sql, res, err, tc.want)
		}
	}
	if _, err := db.Execute(ctx, "UPDATE iv SET ts = ts + CAST('1 day' AS INTERVAL)"); err != nil {
		t.Fatal(err)
	}
	res, err := db.Query(ctx, "SELECT CAST(ts AS TEXT) AS v, ts - ts AS z FROM iv")
	if err != nil || len(res.Rows) != 1 || fmt.Sprint(res.Rows[0]["v"]) != "2026-03-04 10:00:00" {
		t.Fatalf("after UPDATE: %v %v", res, err)
	}
	if len(res.OutputSchema) != 2 || res.OutputSchema[1].Type != parquet.TypeFloat64 {
		t.Errorf("ts - ts declared %v, want double precision (the documented milliseconds)", res.OutputSchema)
	}
}
