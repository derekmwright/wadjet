// SPDX-License-Identifier: MIT

package pgwire

import (
	"bufio"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// Arc BY round 2 (#1501, review r1 B2): a bound bytea parameter is the typed
// value `CAST('\x<hex>' AS BYTES)`, and that spelling survives EVERY position
// the parameter can sit in — the LAG / LEAD default and a MERGE value
// included, where Bind used to strip the CAST and leave the hex SPELLING. The
// position table is coordinator.TestArcBYR2ReentryPositionsEveryArm's, over
// `$1` in text and binary format, plus the write positions (VALUES, UPDATE
// SET, MERGE INSERT / UPDATE) into a BYTES and a TEXT column. BY_PG_DSN=<dsn>
// prints PostgreSQL 17.11's answers (testdata/arc_by_r2_reentry_wire_pg17.tsv).

var byR2WireValues = []string{"6869", "610062", "27", "5c", ""}

var byR2WirePositions = []struct{ name, q string }{
	{"select", `SELECT encode($1, 'hex') FROM by_one`},
	{"where-eq", `SELECT count(*) FROM by_v WHERE b = $1`},
	{"where-rev", `SELECT count(*) FROM by_v WHERE $1 = b`},
	{"in-list", `SELECT count(*) FROM by_v WHERE b IN ($1)`},
	{"between", `SELECT count(*) FROM by_v WHERE b BETWEEN $1 AND $1`},
	{"case", `SELECT encode(CASE WHEN id = 1 THEN $1 ELSE b END, 'hex') FROM by_one`},
	{"coalesce", `SELECT encode(COALESCE($1, b), 'hex') FROM by_one`},
	{"nullif", `SELECT encode(NULLIF($1, b), 'hex') FROM by_one`},
	{"greatest", `SELECT encode(GREATEST($1, decode('', 'hex')), 'hex') FROM by_one`},
	{"length", `SELECT length($1) FROM by_one`},
	{"concat", `SELECT encode($1 || b, 'hex') FROM by_one`},
	{"substring", `SELECT encode(substring($1 from 1 for 2), 'hex') FROM by_one`},
	{"like", `SELECT count(*) FROM by_v WHERE b LIKE $1`},
	{"lag-value", `SELECT encode(lag($1, 0) OVER (ORDER BY id), 'hex') FROM by_one`},
	{"lag-offset", `SELECT encode(lag(b, $1) OVER (ORDER BY id), 'hex') FROM by_one`},
	{"lag-default", `SELECT encode(lag(b, 1, $1) OVER (ORDER BY id), 'hex') FROM by_one`},
	{"lead-default", `SELECT encode(lead(b, 1, $1) OVER (ORDER BY id), 'hex') FROM by_one`},
	{"nth-value", `SELECT encode(nth_value($1, 1) OVER (ORDER BY id), 'hex') FROM by_one`},
	{"agg", `SELECT count(DISTINCT $1) FROM by_one`},
	{"group-by", `SELECT count(*) FROM by_one GROUP BY $1`},
	{"order-by", `SELECT id FROM by_one ORDER BY $1`},
	{"join-on", `SELECT count(*) FROM by_v a JOIN by_one c ON a.b = $1`},
	{"limit", `SELECT id FROM by_one LIMIT $1`},
	{"table-fn", `SELECT count(*) FROM generate_series(1, length($1) + 1)`},
	{"subquery-body", `SELECT (SELECT encode($1, 'hex'))`},
	{"cte", `WITH c AS (SELECT $1 AS v) SELECT encode(v, 'hex') FROM c`},
	{"scalar-sub", `SELECT count(*) FROM by_v WHERE b = (SELECT $1)`},
}

// The write positions: each runs on its own table w_<n> (TBL in the text) (id, b BYTES, s TEXT)
// seeded with row 0, then reads the table back.
var byR2WireWrites = []struct{ name, q string }{
	{"ins-bytes", `INSERT INTO TBL (id, b) VALUES (1, $1)`},
	{"ins-text", `INSERT INTO TBL (id, s) VALUES (1, $1)`},
	{"upd-bytes", `UPDATE TBL SET b = $1 WHERE id = 0`},
	{"upd-text", `UPDATE TBL SET s = $1 WHERE id = 0`},
	{"merge-ins-bytes", `MERGE INTO TBL USING (SELECT 1 AS id) x ON TBL.id = x.id WHEN NOT MATCHED THEN INSERT (id, b) VALUES (x.id, $1)`},
	{"merge-ins-text", `MERGE INTO TBL USING (SELECT 1 AS id) x ON TBL.id = x.id WHEN NOT MATCHED THEN INSERT (id, s) VALUES (x.id, $1)`},
	{"merge-upd-bytes", `MERGE INTO TBL USING (SELECT 0 AS id) x ON TBL.id = x.id WHEN MATCHED THEN UPDATE SET b = $1`},
	{"merge-upd-text", `MERGE INTO TBL USING (SELECT 0 AS id) x ON TBL.id = x.id WHEN MATCHED THEN UPDATE SET s = $1`},
}

// byR2WireKept: the cells this engine answers differently from PostgreSQL
// 17.11 at c67ebf5b and at the round-2 tip alike (arc BY filing candidates).
// A kept cell that starts agreeing FAILS: delete its line.
func byR2WireKept(name string) (string, bool) {
	switch {
	case strings.HasPrefix(name, "limit/"):
		// LIMIT reads a constant: a bytea there is 42601 here, 42804 on
		// PostgreSQL (BY-C12).
		return "ERR 42601", true
	case strings.HasPrefix(name, "like/") && strings.HasSuffix(name, "/5c"):
		// A bytea LIKE pattern ending in its escape answers here; PostgreSQL
		// raises 22025 (BY-C10).
		return "rows=1 1", true
	}
	return "", false
}

func byR2WireRender(res *pgconn.Result) string {
	var rows []string
	for _, r := range res.Rows {
		var cells []string
		for _, c := range r {
			if c == nil {
				cells = append(cells, "NULL")
			} else {
				cells = append(cells, string(c))
			}
		}
		rows = append(rows, strings.Join(cells, ","))
	}
	sort.Strings(rows)
	return fmt.Sprintf("rows=%d %s", len(rows), strings.Join(rows, " | "))
}

func byR2WireErr(err error) string {
	if pe, ok := err.(*pgconn.PgError); ok {
		return "ERR " + pe.Code
	}
	return "ERR " + err.Error()
}

type byR2WireCell struct {
	name, sql string
	setup     []string
	read      string
	param     []byte
	format    int16
}

func byR2WireCells(pg bool) []byR2WireCell {
	bytesType := "BYTES"
	if pg {
		bytesType = "bytea"
	}
	var out []byR2WireCell
	n := 0
	for _, h := range byR2WireValues {
		raw, _ := hex.DecodeString(h)
		label := h
		if label == "" {
			label = "empty"
		}
		for _, f := range []struct {
			name   string
			format int16
			param  []byte
		}{{"bin", 1, raw}, {"text", 0, []byte(`\x` + h)}} {
			for _, p := range byR2WirePositions {
				out = append(out, byR2WireCell{name: p.name + "/" + f.name + "/" + label, sql: p.q, param: f.param, format: f.format})
			}
			for _, w := range byR2WireWrites {
				n++
				tbl := fmt.Sprintf("w_%d", n)
				out = append(out, byR2WireCell{
					name:   w.name + "/" + f.name + "/" + label,
					sql:    strings.ReplaceAll(w.q, "TBL", tbl),
					setup:  []string{"CREATE TABLE " + tbl + " (id BIGINT, b " + bytesType + ", s TEXT)", "INSERT INTO " + tbl + " VALUES (0, NULL, NULL)"},
					read:   "SELECT id, encode(b, 'hex'), s FROM " + tbl,
					param:  f.param,
					format: f.format,
				})
			}
		}
	}
	return out
}

func byR2WireRun(ctx context.Context, conn *pgconn.PgConn, c byR2WireCell) string {
	for _, q := range c.setup {
		if _, err := conn.Exec(ctx, q).ReadAll(); err != nil {
			return "setup " + byR2WireErr(err)
		}
	}
	res := conn.ExecParams(ctx, c.sql, [][]byte{c.param}, []uint32{17}, []int16{c.format}, nil).Read()
	got := ""
	if res.Err != nil {
		got = byR2WireErr(res.Err)
	} else if c.read == "" {
		got = byR2WireRender(res)
	}
	if c.read != "" {
		r := conn.ExecParams(ctx, c.read, nil, nil, nil, nil).Read()
		if r.Err != nil {
			return got + " ; read " + byR2WireErr(r.Err)
		}
		if got != "" {
			got += " ; "
		}
		got += byR2WireRender(r)
	}
	return got
}

func TestArcBYR2ReentryPositionsOnTheWire(t *testing.T) {
	ctx := context.Background()
	if dsn := os.Getenv("BY_PG_DSN"); dsn != "" {
		conn, err := pgconn.Connect(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close(ctx)
		if _, err := conn.Exec(ctx, `SET statement_timeout = '30s'; DROP SCHEMA IF EXISTS byr2 CASCADE; CREATE SCHEMA byr2; SET search_path = byr2;
CREATE TABLE by_one (id bigint, b bytea, s text); INSERT INTO by_one VALUES (1, '\xff', 'z');
CREATE TABLE by_v (k bigint, b bytea);
INSERT INTO by_v VALUES (1, '\x6869'), (2, '\x610062'), (3, '\x27'), (4, '\x5c'), (5, '\x');`).ReadAll(); err != nil {
			t.Fatal(err)
		}
		for _, c := range byR2WireCells(true) {
			fmt.Printf("%s\t%s\n", c.name, byR2WireRun(ctx, conn, c))
		}
		return
	}
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, tb := range []struct {
		name string
		sc   parquet.Schema
		rows []map[string]any
	}{
		{"by_one", parquet.Schema{Columns: []parquet.Column{{Name: "id", Type: parquet.TypeInt64},
			{Name: "b", Type: parquet.TypeBytes, Nullable: true}, {Name: "s", Type: parquet.TypeString, Nullable: true}}},
			[]map[string]any{{"id": int64(1), "b": []byte{0xff}, "s": "z"}}},
		{"by_v", parquet.Schema{Columns: []parquet.Column{{Name: "k", Type: parquet.TypeInt64},
			{Name: "b", Type: parquet.TypeBytes, Nullable: true}}}, nil},
	} {
		if tb.name == "by_v" {
			for i, h := range byR2WireValues {
				raw, _ := hex.DecodeString(h)
				tb.rows = append(tb.rows, map[string]any{"k": int64(i + 1), "b": raw})
			}
		}
		if err := db.CreateTable(ctx, tb.name, tb.sc, nil); err != nil {
			t.Fatal(err)
		}
		ing := db.NewIngester(tb.name, tb.sc, nil, ingest.Config{MaxBufferRows: 16})
		if err := ing.Ingest(ctx, tb.rows); err != nil {
			t.Fatal(err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	srv := NewServer(db, Config{}, nil)
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	conn := connectPgconn(t, srv.Addr())
	f, err := os.Open("testdata/arc_by_r2_reentry_wire_pg17.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	answers := map[string]string{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if line := sc.Text(); line != "" && !strings.HasPrefix(line, "#") {
			name, want, _ := strings.Cut(line, "\t")
			answers[name] = want
		}
	}
	equal := 0
	for _, c := range byR2WireCells(false) {
		want, ok := answers[c.name]
		if !ok {
			t.Fatalf("%s: no PostgreSQL answer", c.name)
		}
		got := byR2WireRun(ctx, conn, c)
		if kept, ok := byR2WireKept(c.name); ok {
			if got != kept {
				t.Errorf("%s: kept cell moved\n  got  %s\n  kept %s (PostgreSQL 17.11: %s)", c.name, got, kept, want)
			}
			continue
		}
		if got != want {
			t.Errorf("%s: %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.name, c.sql, got, want)
			continue
		}
		equal++
	}
	t.Logf("%d cells equal PostgreSQL", equal)
}
