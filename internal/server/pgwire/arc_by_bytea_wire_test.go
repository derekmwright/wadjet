// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

// byWireStep is one statement of TestArcBYByteaWireDoorsMatchPostgres: the
// extended protocol with typed parameters in a chosen format and a chosen
// result format, the simple protocol, or COPY FROM STDIN.
type byWireStep struct {
	name    string
	sql     string
	params  [][]byte
	oids    []uint32
	formats []int16 // parameter formats
	result  int16   // result format
	simple  bool
	copyIn  string
}

// byWireSteps run in order on one connection; every answer is PostgreSQL
// 17.11's (BY_PG_DSN=<dsn> runs the same steps there and prints them, with
// BYTES spelled bytea).
var byWireSteps = []byWireStep{
	{name: "ddl", sql: `CREATE TABLE pb (id BIGINT, b BYTES)`, simple: true},
	// Parameters into an assignment: text and binary bytea, an undeclared
	// text one (PostgreSQL types it from the column), and two refusals.
	{name: "ins/text-bytea", sql: `INSERT INTO pb (id, b) VALUES ($1, $2)`, params: [][]byte{[]byte("1"), []byte(`\x6869`)}, oids: []uint32{20, 17}, formats: []int16{0, 0}},
	{name: "ins/binary-bytea", sql: `INSERT INTO pb (id, b) VALUES ($1, $2)`, params: [][]byte{[]byte("2"), {0x00, 0xff, 0x5c}}, oids: []uint32{20, 17}, formats: []int16{0, 1}},
	{name: "ins/text-undeclared", sql: `INSERT INTO pb (id, b) VALUES ($1, $2)`, params: [][]byte{[]byte("3"), []byte(`\x6869`)}, oids: []uint32{0, 0}, formats: []int16{0, 0}},
	{name: "ins/text-bytea-escape", sql: `INSERT INTO pb (id, b) VALUES ($1, $2)`, params: [][]byte{[]byte("4"), []byte(`a\\b\000`)}, oids: []uint32{20, 17}, formats: []int16{0, 0}},
	{name: "ins/text-bytea-lone-backslash", sql: `INSERT INTO pb (id, b) VALUES ($1, $2)`, params: [][]byte{[]byte("90"), []byte(`a\b`)}, oids: []uint32{20, 17}, formats: []int16{0, 0}},
	{name: "ins/text-bytea-odd", sql: `INSERT INTO pb (id, b) VALUES ($1, $2)`, params: [][]byte{[]byte("91"), []byte(`\x686`)}, oids: []uint32{20, 17}, formats: []int16{0, 0}},
	{name: "ins/simple", sql: `INSERT INTO pb VALUES (5, '\x6869'), (6, 'a\\b')`, simple: true},
	{name: "copy", sql: `COPY pb (id, b) FROM STDIN`, copyIn: "7\t\\\\x6869\n8\thi\n"},
	{name: "upd/binary-bytea", sql: `UPDATE pb SET b = $1 WHERE id = 6`, params: [][]byte{{0x5c}}, oids: []uint32{17}, formats: []int16{1}},
	// Parameters into a comparison: parameters-pgwire#r11's binary bytes
	// holding a backslash, and the text forms.
	{name: "cmp/binary-backslash", sql: `SELECT id FROM pb WHERE b = $1 ORDER BY id`, params: [][]byte{{0x00, 0xff, 0x5c}}, oids: []uint32{17}, formats: []int16{1}},
	{name: "cmp/binary-hi", sql: `SELECT id FROM pb WHERE b = $1 ORDER BY id`, params: [][]byte{[]byte("hi")}, oids: []uint32{17}, formats: []int16{1}},
	{name: "cmp/text-bytea", sql: `SELECT id FROM pb WHERE b = $1 ORDER BY id`, params: [][]byte{[]byte(`\x6869`)}, oids: []uint32{17}, formats: []int16{0}},
	{name: "cmp/text-undeclared", sql: `SELECT id FROM pb WHERE b = $1 ORDER BY id`, params: [][]byte{[]byte(`\x6869`)}, oids: []uint32{0}, formats: []int16{0}},
	// A bytea parameter is a bytea value: parameters-pgwire#r10.
	{name: "select/text-param-text-result", sql: `SELECT $1 AS v`, params: [][]byte{[]byte(`\x6869`)}, oids: []uint32{17}, formats: []int16{0}},
	{name: "select/binary-param-binary-result", sql: `SELECT $1 AS v`, params: [][]byte{{0x00, 0x5c}}, oids: []uint32{17}, formats: []int16{1}, result: 1},
	// Results: the column in both formats.
	{name: "read/text", sql: `SELECT id, b FROM pb ORDER BY id`},
	{name: "read/binary", sql: `SELECT id, b FROM pb ORDER BY id`, result: 1},
	{name: "read/hex", sql: `SELECT id, encode(b, 'hex') AS h FROM pb ORDER BY id`},
}

// byWireAnswers is PostgreSQL 17.11's transcript of byWireSteps
// (BY_PG_DSN mode, by_author/wire_pg.out): per step the error's SQLSTATE, or
// the result's column OIDs and rows. A binary cell is printed as hex.
var byWireAnswers = map[string]string{
	"ddl":                               "ok",
	"ins/text-bytea":                    "ok",
	"ins/binary-bytea":                  "ok",
	"ins/text-undeclared":               "ok",
	"ins/text-bytea-escape":             "ok",
	"ins/text-bytea-lone-backslash":     "ERR 22P02",
	"ins/text-bytea-odd":                "ERR 22023",
	"ins/simple":                        "ok",
	"copy":                              "ok",
	"upd/binary-bytea":                  "ok",
	"cmp/binary-backslash":              "oids=20 | 2",
	"cmp/binary-hi":                     "oids=20 | 1 | 3 | 5 | 7 | 8",
	"cmp/text-bytea":                    "oids=20 | 1 | 3 | 5 | 7 | 8",
	"cmp/text-undeclared":               "oids=20 | 1 | 3 | 5 | 7 | 8",
	"select/text-param-text-result":     "oids=17 | \\x6869",
	"select/binary-param-binary-result": "oids=17 | bin:005c",
	"read/text":                         "oids=20,17 | 1,\\x6869 | 2,\\x00ff5c | 3,\\x6869 | 4,\\x615c6200 | 5,\\x6869 | 6,\\x5c | 7,\\x6869 | 8,\\x6869",
	"read/binary":                       "oids=20,17 | bin:0000000000000001,bin:6869 | bin:0000000000000002,bin:00ff5c | bin:0000000000000003,bin:6869 | bin:0000000000000004,bin:615c6200 | bin:0000000000000005,bin:6869 | bin:0000000000000006,bin:5c | bin:0000000000000007,bin:6869 | bin:0000000000000008,bin:6869",
	"read/hex":                          "oids=20,25 | 1,6869 | 2,00ff5c | 3,6869 | 4,615c6200 | 5,6869 | 6,5c | 7,6869 | 8,6869",
}

func byWireRun(ctx context.Context, conn *pgconn.PgConn, s byWireStep, pg bool) string {
	sql := s.sql
	if pg {
		sql = strings.ReplaceAll(sql, "BYTES", "bytea")
	}
	if s.copyIn != "" {
		if _, err := conn.CopyFrom(ctx, strings.NewReader(s.copyIn), sql); err != nil {
			return byWireErr(err)
		}
		return "ok"
	}
	if s.simple {
		if _, err := conn.Exec(ctx, sql).ReadAll(); err != nil {
			return byWireErr(err)
		}
		return "ok"
	}
	res := conn.ExecParams(ctx, sql, s.params, s.oids, s.formats, []int16{s.result}).Read()
	if res.Err != nil {
		return byWireErr(res.Err)
	}
	if len(res.FieldDescriptions) == 0 {
		return "ok"
	}
	var oids []string
	for _, f := range res.FieldDescriptions {
		oids = append(oids, fmt.Sprint(f.DataTypeOID))
	}
	out := []string{"oids=" + strings.Join(oids, ",")}
	for _, r := range res.Rows {
		var cells []string
		for _, c := range r {
			switch {
			case c == nil:
				cells = append(cells, "NULL")
			case s.result == 1:
				cells = append(cells, "bin:"+hex.EncodeToString(c))
			default:
				cells = append(cells, string(c))
			}
		}
		out = append(out, strings.Join(cells, ","))
	}
	return strings.Join(out, " | ")
}

func byWireErr(err error) string {
	if pe, ok := err.(*pgconn.PgError); ok {
		return "ERR " + pe.Code
	}
	return "ERR " + err.Error()
}

// TestArcBYByteaWireDoorsMatchPostgres is the wire half of #1501: a text bytea
// parameter is read by byteain, a binary one carries its bytes untouched into
// an assignment and a comparison (parameters-pgwire#r11), a bytea parameter is
// a bytea value (#r10), and the column reads back in both result formats.
func TestArcBYByteaWireDoorsMatchPostgres(t *testing.T) {
	ctx := context.Background()
	if dsn := os.Getenv("BY_PG_DSN"); dsn != "" {
		conn, err := pgconn.Connect(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close(ctx)
		_, _ = conn.Exec(ctx, "SET statement_timeout = '30s'; DROP TABLE IF EXISTS pb").ReadAll()
		for _, s := range byWireSteps {
			t.Logf("%q: %q,", s.name, byWireRun(ctx, conn, s, true))
		}
		return
	}
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	srv := NewServer(db, Config{}, nil)
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	conn := connectPgconn(t, srv.Addr())
	for _, s := range byWireSteps {
		got := byWireRun(ctx, conn, s, false)
		if want := byWireAnswers[s.name]; got != want {
			t.Errorf("%s: %s\n  got  %s\n  want %s (PostgreSQL 17.11)", s.name, s.sql, got, want)
		}
	}
}
