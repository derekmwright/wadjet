// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The embedded arm of arc SS (#1428 #1431 #1427 #1422): `DB.Query` over the
// issues' own statements, each result read positionally (QueryResult.Cells)
// by its DECLARED type — a TIMESTAMP cell is its epoch milliseconds, rendered
// here as the instant — with the declared type beside it, and the values an
// INSERT … SELECT STORES when the scalar subquery is the source. Every want is
// PostgreSQL 17.11's, measured over the same DDL. At v0.25.3 the TIMESTAMP
// subquery answered its epoch milliseconds to CAST and extract (-1,
// -968030), `+ INTERVAL` dropped the hour, the 9999-12-31 DATE equality
// answered no row, and the correlated `c.f + x.v` declared integer: 6 for 6.5,
// and 7 / 7 stored for 8 / 7.5.
func TestArcSSEmbeddedScalarSubqueryTypedOperand(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, ddl := range []string{
		"CREATE TABLE ss_e (id BIGINT, f DOUBLE, n NUMERIC(10,2), d DATE, ts TIMESTAMP)",
		"INSERT INTO ss_e VALUES (1, 1.5, 2.25, DATE '2024-03-04', TIMESTAMP '2024-03-04 12:00:00'), " +
			"(3, 0.25, 10.00, DATE '9999-12-31', TIMESTAMP '1969-12-31 23:59:59.999'), " +
			"(4, NULL, NULL, DATE '1000-01-01', TIMESTAMP '1000-01-01 00:00:00')",
		"CREATE TABLE ss_ei (id BIGINT, v INT)",
		"INSERT INTO ss_ei VALUES (1, 5), (2, 6)",
		"CREATE TABLE ss_tgt (k VARCHAR, i INT, n NUMERIC(10,2), f DOUBLE, ts TIMESTAMP, v VARCHAR)",
	} {
		if _, err := db.Query(ctx, ddl); err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
	}
	cases := []struct{ name, sql, want string }{
		{"1428/castVarchar", "SELECT CAST((SELECT ts FROM ss_e WHERE id = 3) AS VARCHAR) AS x",
			"{text} 1969-12-31 23:59:59.999"},
		// PostgreSQL declares extract numeric; this engine declares double
		// precision for it over any operand (ADR-0024 §2c, catalogued).
		{"1428/extractYear", "SELECT extract(year FROM (SELECT min(ts) FROM ss_e)) AS x",
			"{float} 1000"},
		{"1431/plusHour", "SELECT (SELECT max(ts) FROM ss_e WHERE id = 1) + INTERVAL '1 hour' AS x",
			"{timestamp} 2024-03-04 13:00:00"},
		{"1431/minusDay", "SELECT (SELECT max(ts) FROM ss_e WHERE id = 1) - INTERVAL '1 day' AS x",
			"{timestamp} 2024-03-03 12:00:00"},
		{"1427/eq", "SELECT id FROM ss_e WHERE d = (SELECT d FROM ss_e WHERE id = 3)",
			"{int} 3"},
		{"1422/outerFirst", "SELECT c.id, (SELECT c.f + x.v FROM ss_ei x WHERE x.id = 1) AS x FROM ss_e c WHERE c.id = 1",
			"{int,float} 1,6.5"},
		{"1422/innerFirst", "SELECT c.id, (SELECT x.v + c.f FROM ss_ei x WHERE x.id = 1) AS x FROM ss_e c WHERE c.id = 1",
			"{int,float} 1,6.5"},
	}
	for _, c := range cases {
		res, err := db.Query(ctx, c.sql)
		if err != nil {
			t.Errorf("%s: %s\n  refused: %v\n  want %s", c.name, c.sql, err, c.want)
			continue
		}
		if got := ssEmbeddedRender(res); got != c.want {
			t.Errorf("%s: %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.name, c.sql, got, c.want)
		}
	}
	// WHAT IS STORED: the scalar subquery as an INSERT … SELECT source, one
	// target type each.
	for _, ins := range []string{
		"INSERT INTO ss_tgt (k, i) SELECT 'i', (SELECT c.f + x.v FROM ss_ei x WHERE x.id = 2) FROM ss_e c WHERE c.id = 1",
		"INSERT INTO ss_tgt (k, n) SELECT 'n', (SELECT c.n + x.v FROM ss_ei x WHERE x.id = 2) FROM ss_e c WHERE c.id = 1",
		"INSERT INTO ss_tgt (k, f) SELECT 'f', (SELECT c.f + x.v FROM ss_ei x WHERE x.id = 2) FROM ss_e c WHERE c.id = 1",
		"INSERT INTO ss_tgt (k, ts) SELECT 'ts', (SELECT max(ts) FROM ss_e WHERE id = 1) + INTERVAL '1 hour'",
		"INSERT INTO ss_tgt (k, v) SELECT 'v', (SELECT ts FROM ss_e WHERE id = 3)",
		"INSERT INTO ss_tgt (k, v) SELECT 'vd', (SELECT d FROM ss_e WHERE id = 3)",
	} {
		if _, err := db.Query(ctx, ins); err != nil {
			t.Errorf("%s\n  refused: %v", ins, err)
		}
	}
	const stored = "SELECT k, i, n, f, CAST(ts AS VARCHAR) AS ts, v FROM ss_tgt ORDER BY k"
	want := "{text,int,numeric,float,text,text} " +
		"f,NULL,NULL,7.5,NULL,NULL | i,8,NULL,NULL,NULL,NULL | n,NULL,8.25,NULL,NULL,NULL | " +
		"ts,NULL,NULL,NULL,2024-03-04 13:00:00,NULL | v,NULL,NULL,NULL,NULL,1969-12-31 23:59:59.999 | " +
		"vd,NULL,NULL,NULL,NULL,9999-12-31"
	res, err := db.Query(ctx, stored)
	if err != nil {
		t.Fatalf("%s: %v", stored, err)
	}
	if got := ssEmbeddedRender(res); got != want {
		t.Errorf("stored values\n  got  %s\n  want %s (PostgreSQL 17.11)", got, want)
	}
}

// ssEmbeddedRender is `{classes} row | row`, each cell read by its declared
// type and printed as PostgreSQL's text output prints it; the class is the
// declared type's family (an integer's width is not this gate's question).
func ssEmbeddedRender(res *QueryResult) string {
	classes := make([]string, len(res.ColumnMetas))
	for i, m := range res.ColumnMetas {
		switch m.TypeID {
		case parquet.TypeInt32, parquet.TypeInt64:
			classes[i] = "int"
		case parquet.TypeFloat64, parquet.TypeFloat32:
			classes[i] = "float"
		case parquet.TypeDecimal:
			classes[i] = "numeric"
		case parquet.TypeString:
			classes[i] = "text"
		default:
			classes[i] = strings.ToLower(m.TypeID.String())
		}
	}
	rows := make([]string, 0, len(res.Rows))
	for r := range res.Rows {
		cells := res.Cells(r)
		parts := make([]string, len(cells))
		for i, v := range cells {
			switch x := v.(type) {
			case nil:
				parts[i] = "NULL"
			case int64:
				if res.ColumnMetas[i].TypeID == parquet.TypeTimestamp {
					parts[i] = batch.FormatTimestamp(x)
				} else {
					parts[i] = strconv.FormatInt(x, 10)
				}
			case float64:
				parts[i] = strconv.FormatFloat(x, 'f', -1, 64)
			default:
				parts[i] = fmt.Sprint(v)
			}
		}
		rows = append(rows, strings.Join(parts, ","))
	}
	return "{" + strings.Join(classes, ",") + "} " + strings.Join(rows, " | ")
}
