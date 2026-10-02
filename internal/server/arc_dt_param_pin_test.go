// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/server/pgwire"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// dtParamRows runs sql with text-format parameters of the given OIDs and
// renders the sorted rows ("<0 rows>" when none).
func dtParamRows(ctx context.Context, c *pgconn.PgConn, sql string, vals []string, oids []uint32) string {
	pv := make([][]byte, len(vals))
	for i, v := range vals {
		pv[i] = []byte(v)
	}
	rr := c.ExecParams(ctx, sql, pv, oids, nil, nil)
	var rows []string
	for rr.NextRow() {
		var parts []string
		for _, b := range rr.Values() {
			if b == nil {
				parts = append(parts, "NULL")
			} else {
				parts = append(parts, string(b))
			}
		}
		rows = append(rows, strings.Join(parts, ","))
	}
	if _, err := rr.Close(); err != nil {
		return "ERR " + err.Error()
	}
	sort.Strings(rows)
	if len(rows) == 0 {
		return "<0 rows>"
	}
	return strings.Join(rows, " | ")
}

// A TIMESTAMP-TYPED PARAMETER AGAINST A DATE COLUMN (#1426). The wire spliced
// a bound parameter into the statement as a bare quoted literal, so the DATE
// operand read it with its own input function, which dropped the time: the
// parameter's declared OID (1114, timestamp) never reached the DATE /
// TIMESTAMP rule (batch.TemporalCommonType). PostgreSQL 17.11 compares
// `date = timestamp` at the DATE's midnight (measured through
// pgconn.ExecParams, text format, OID 1114).
//
// These cells were pinned at today's wrong answer (eq/nonMidnight 3,
// in/nonMidnight 2 | 3, lt/afterMidnight none) until arc PW spliced every
// parameter as a literal of its type; they now assert PostgreSQL's answer.
// The controls (a midnight, an untyped and a DATE-typed parameter, and an
// explicit CAST) agreed with PostgreSQL before and after.
func TestArcDTTimestampParameterAgainstDate(t *testing.T) {
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "d", Type: parquet.TypeDate, Nullable: true},
	}}
	var rows []map[string]any
	for i, v := range []any{"2024-01-02", "2024-03-04", "1969-12-31", "1970-01-01", nil} {
		rows = append(rows, map[string]any{"id": int64(i + 1), "d": v})
	}
	if err := db.CreateTable(ctx, "dt_param_d", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("dt_param_d", schema, nil, ingest.Config{MaxBufferRows: 10, RowGroupSize: 2})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	pg := pgwire.NewServer(db, pgwire.Config{}, nil)
	if err := pg.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("start pgwire: %v", err)
	}
	t.Cleanup(pg.Shutdown)
	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://wadjet@%s/wadjet?sslmode=disable", pg.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	const ts, date, unknown = 1114, 1082, 0
	cells := []struct {
		name   string
		sql    string
		vals   []string
		oids   []uint32
		pg     string // PostgreSQL 17.11
		pinned string // today's answer when it differs (#1426)
	}{
		{"eq/nonMidnight", "SELECT id FROM dt_param_d WHERE d = $1",
			[]string{"1969-12-31 23:59:59.999"}, []uint32{ts}, "<0 rows>", ""},
		{"in/nonMidnight", "SELECT id FROM dt_param_d WHERE d IN ($1, $2)",
			[]string{"1969-12-31 00:00:00.001", "2024-03-04 12:00:00"}, []uint32{ts, ts}, "<0 rows>", ""},
		{"lt/afterMidnight", "SELECT id FROM dt_param_d WHERE d < $1",
			[]string{"1969-12-31 00:00:00.001"}, []uint32{ts}, "3", ""},
		{"ctrl/midnight", "SELECT id FROM dt_param_d WHERE d = $1",
			[]string{"1969-12-31 00:00:00"}, []uint32{ts}, "3", ""},
		{"ctrl/untyped", "SELECT id FROM dt_param_d WHERE d IN ($1, $2)",
			[]string{"1969-12-31", "2024-01-02"}, []uint32{unknown, unknown}, "1 | 3", ""},
		{"ctrl/dateTyped", "SELECT id FROM dt_param_d WHERE d IN ($1, $2)",
			[]string{"1969-12-31", "2024-01-02"}, []uint32{date, date}, "1 | 3", ""},
		{"ctrl/cast", "SELECT id FROM dt_param_d WHERE d IN (CAST($1 AS TIMESTAMP))",
			[]string{"2024-03-04 12:00:00"}, []uint32{unknown}, "<0 rows>", ""},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			got := dtParamRows(ctx, conn.PgConn(), c.sql, c.vals, c.oids)
			switch {
			case c.pinned == "":
				if got != c.pg {
					t.Errorf("%s %v (OIDs %v)\n  got  %s\n  want %s (PostgreSQL 17.11)", c.sql, c.vals, c.oids, got, c.pg)
				}
			case got == c.pg:
				t.Errorf("%s %v (OIDs %v) now answers PostgreSQL's %s: #1426 is fixed — delete this pin "+
					"and assert PostgreSQL's answer", c.sql, c.vals, c.oids, got)
			case got != c.pinned:
				t.Errorf("%s %v (OIDs %v)\n  got  %s\n  PINNED %s (#1426), PostgreSQL 17.11 %s", c.sql, c.vals, c.oids, got, c.pinned, c.pg)
			}
		})
	}
}
