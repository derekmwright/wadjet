// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// The assignment matrix of arc BY (#1501, measured case B1): a BYTES value
// — a BYTES column, CAST(… AS BYTES), decode() — assigned to a TEXT, a
// VARCHAR(n), a BYTES and an integer column through every write door. PostgreSQL
// 17.11 assigns bytea to the string types by its I/O conversion (bytea_output
// hex, `\x41`), and refuses bytea into an integer (42804). At 714098e5 every
// BYTES → TEXT cell was 42804; at c67ebf5b the CAST and decode sources were text
// pass-throughs that stored the same `\x41` (decode: its hex text).
//
// The VARCHAR(3) cells hold text-collation#r8 (this engine's VARCHAR(n) column
// keeps no length; PostgreSQL's 22001 is in the answer table).
//
// BY_PG_DSN=<dsn> runs the same cells on PostgreSQL (BYTES is a domain over
// bytea there) and prints the answer table this test pins.

type byAsgTarget struct{ name, decl, seed string }

var byAsgTargets = []byAsgTarget{
	{"text", "TEXT", "'x'"},
	{"varchar3", "VARCHAR(3)", "'x'"},
	{"varchar12", "VARCHAR(12)", "'x'"},
	{"bytes", "BYTES", "'x'"},
	{"bigint", "BIGINT", "7"},
}

var byAsgSources = []struct{ name, expr, from string }{
	{"cast", `CAST('\x41' AS BYTES)`, ""},
	{"decode", `decode('41', 'hex')`, ""},
	{"col", `b`, " FROM by_src WHERE id = 1"},
}

type byAsgCell struct {
	name, target string
	stmts        []string
}

func byAsgCells() []byAsgCell {
	var out []byAsgCell
	for _, t := range byAsgTargets {
		for _, s := range byAsgSources {
			doors := map[string]string{
				"insert-select": fmt.Sprintf(`INSERT INTO ba_t (id, c) SELECT 1, %s%s`, s.expr, s.from),
				"update":        fmt.Sprintf(`UPDATE ba_t SET c = (%s) WHERE id = 0`, s.expr),
				"merge-insert": fmt.Sprintf(`MERGE INTO ba_t USING (SELECT 1 AS id, %s AS x%s) s ON ba_t.id = s.id `+
					`WHEN NOT MATCHED THEN INSERT (id, c) VALUES (s.id, s.x)`, s.expr, s.from),
				"merge-update": fmt.Sprintf(`MERGE INTO ba_t USING (SELECT 0 AS id, %s AS x%s) s ON ba_t.id = s.id `+
					`WHEN MATCHED THEN UPDATE SET c = s.x`, s.expr, s.from),
			}
			if s.from == "" {
				doors["values"] = fmt.Sprintf(`INSERT INTO ba_t (id, c) VALUES (1, %s)`, s.expr)
			} else {
				// A column in UPDATE SET reads the source through a scalar
				// subquery, which this engine refuses (0A000, base-identical);
				// the column source is assigned through INSERT … SELECT and MERGE.
				delete(doors, "update")
			}
			for _, d := range []string{"values", "insert-select", "update", "merge-insert", "merge-update"} {
				st, ok := doors[d]
				if !ok {
					continue
				}
				out = append(out, byAsgCell{
					name:   t.name + "/" + s.name + "/" + d,
					target: t.decl,
					stmts: []string{
						`CREATE TABLE ba_t (id BIGINT, c ` + t.decl + `)`,
						`INSERT INTO ba_t (id, c) VALUES (0, ` + t.seed + `)`,
						st,
					},
				})
			}
		}
	}
	return out
}

// byAsgAnswers: PostgreSQL 17.11 (BY_PG_DSN mode; by_author/r2/asg_pg.out).
var byAsgAnswers = map[string]string{
	"text/cast/values":               "0|x ; 1|\\x41",
	"text/cast/insert-select":        "0|x ; 1|\\x41",
	"text/cast/update":               "0|\\x41",
	"text/cast/merge-insert":         "0|x ; 1|\\x41",
	"text/cast/merge-update":         "0|\\x41",
	"text/decode/values":             "0|x ; 1|\\x41",
	"text/decode/insert-select":      "0|x ; 1|\\x41",
	"text/decode/update":             "0|\\x41",
	"text/decode/merge-insert":       "0|x ; 1|\\x41",
	"text/decode/merge-update":       "0|\\x41",
	"text/col/insert-select":         "0|x ; 1|\\x41",
	"text/col/merge-insert":          "0|x ; 1|\\x41",
	"text/col/merge-update":          "0|\\x41",
	"varchar3/cast/values":           "ERR 22001 ; 0|x",
	"varchar3/cast/insert-select":    "ERR 22001 ; 0|x",
	"varchar3/cast/update":           "ERR 22001 ; 0|x",
	"varchar3/cast/merge-insert":     "ERR 22001 ; 0|x",
	"varchar3/cast/merge-update":     "ERR 22001 ; 0|x",
	"varchar3/decode/values":         "ERR 22001 ; 0|x",
	"varchar3/decode/insert-select":  "ERR 22001 ; 0|x",
	"varchar3/decode/update":         "ERR 22001 ; 0|x",
	"varchar3/decode/merge-insert":   "ERR 22001 ; 0|x",
	"varchar3/decode/merge-update":   "ERR 22001 ; 0|x",
	"varchar3/col/insert-select":     "ERR 22001 ; 0|x",
	"varchar3/col/merge-insert":      "ERR 22001 ; 0|x",
	"varchar3/col/merge-update":      "ERR 22001 ; 0|x",
	"varchar12/cast/values":          "0|x ; 1|\\x41",
	"varchar12/cast/insert-select":   "0|x ; 1|\\x41",
	"varchar12/cast/update":          "0|\\x41",
	"varchar12/cast/merge-insert":    "0|x ; 1|\\x41",
	"varchar12/cast/merge-update":    "0|\\x41",
	"varchar12/decode/values":        "0|x ; 1|\\x41",
	"varchar12/decode/insert-select": "0|x ; 1|\\x41",
	"varchar12/decode/update":        "0|\\x41",
	"varchar12/decode/merge-insert":  "0|x ; 1|\\x41",
	"varchar12/decode/merge-update":  "0|\\x41",
	"varchar12/col/insert-select":    "0|x ; 1|\\x41",
	"varchar12/col/merge-insert":     "0|x ; 1|\\x41",
	"varchar12/col/merge-update":     "0|\\x41",
	"bytes/cast/values":              "0|\\x78 ; 1|\\x41",
	"bytes/cast/insert-select":       "0|\\x78 ; 1|\\x41",
	"bytes/cast/update":              "0|\\x41",
	"bytes/cast/merge-insert":        "0|\\x78 ; 1|\\x41",
	"bytes/cast/merge-update":        "0|\\x41",
	"bytes/decode/values":            "0|\\x78 ; 1|\\x41",
	"bytes/decode/insert-select":     "0|\\x78 ; 1|\\x41",
	"bytes/decode/update":            "0|\\x41",
	"bytes/decode/merge-insert":      "0|\\x78 ; 1|\\x41",
	"bytes/decode/merge-update":      "0|\\x41",
	"bytes/col/insert-select":        "0|\\x78 ; 1|\\x41",
	"bytes/col/merge-insert":         "0|\\x78 ; 1|\\x41",
	"bytes/col/merge-update":         "0|\\x41",
	"bigint/cast/values":             "ERR 42804 ; 0|7",
	"bigint/cast/insert-select":      "ERR 42804 ; 0|7",
	"bigint/cast/update":             "ERR 42804 ; 0|7",
	"bigint/cast/merge-insert":       "ERR 42804 ; 0|7",
	"bigint/cast/merge-update":       "ERR 42804 ; 0|7",
	"bigint/decode/values":           "ERR 42804 ; 0|7",
	"bigint/decode/insert-select":    "ERR 42804 ; 0|7",
	"bigint/decode/update":           "ERR 42804 ; 0|7",
	"bigint/decode/merge-insert":     "ERR 42804 ; 0|7",
	"bigint/decode/merge-update":     "ERR 42804 ; 0|7",
	"bigint/col/insert-select":       "ERR 42804 ; 0|7",
	"bigint/col/merge-insert":        "ERR 42804 ; 0|7",
	"bigint/col/merge-update":        "ERR 42804 ; 0|7",
}

type byAsgRunner interface {
	exec(q string) error
	rows(q string) (string, error)
}

type byAsgEngine struct {
	ctx context.Context
	db  *DB
}

func (e byAsgEngine) exec(q string) error { _, err := e.db.Query(e.ctx, q); return err }
func (e byAsgEngine) rows(q string) (string, error) {
	res, err := e.db.Query(e.ctx, q)
	if err != nil {
		return "", err
	}
	var rows []string
	for i := range res.Rows {
		var f []string
		for _, v := range res.Cells(i) {
			if v == nil {
				f = append(f, "NULL")
				continue
			}
			f = append(f, fmt.Sprint(v))
		}
		rows = append(rows, strings.Join(f, "|"))
	}
	return strings.Join(rows, " ; "), nil
}

type byAsgPG struct {
	ctx  context.Context
	conn *pgconn.PgConn
}

func (p byAsgPG) exec(q string) error { _, err := p.conn.Exec(p.ctx, q).ReadAll(); return err }
func (p byAsgPG) rows(q string) (string, error) {
	res := p.conn.ExecParams(p.ctx, q, nil, nil, nil, nil).Read()
	if res.Err != nil {
		return "", res.Err
	}
	var rows []string
	for _, r := range res.Rows {
		v := "NULL"
		if r[1] != nil {
			v = string(r[1])
		}
		rows = append(rows, string(r[0])+"|"+v)
	}
	return strings.Join(rows, " ; "), nil
}

func byAsgState(err error) string {
	if st := sqlerr.StateOf(err); st != "" {
		return st
	}
	if pe, ok := err.(*pgconn.PgError); ok {
		return pe.Code
	}
	return "?" + err.Error()
}

func byAsgRun(t *testing.T, r byAsgRunner, c byAsgCell) string {
	_ = r.exec(`DROP TABLE IF EXISTS ba_t`)
	for _, q := range c.stmts[:2] {
		if err := r.exec(q); err != nil {
			t.Fatalf("%s: setup %s: %v", c.name, q, err)
		}
	}
	got := ""
	if err := r.exec(c.stmts[2]); err != nil {
		got = "ERR " + byAsgState(err) + " ; "
	}
	rows, err := r.rows(`SELECT id, CAST(c AS TEXT) FROM ba_t ORDER BY id`)
	if err != nil {
		return got + "read ERR " + byAsgState(err)
	}
	return got + rows
}

func TestArcBYR2BytesAssignmentMatrix(t *testing.T) {
	ctx := context.Background()
	setup := []string{
		`CREATE TABLE by_src (id BIGINT, b BYTES)`,
		`INSERT INTO by_src VALUES (1, decode('41', 'hex'))`,
	}
	var r byAsgRunner
	pg := os.Getenv("BY_PG_DSN") != ""
	if pg {
		conn, err := pgconn.Connect(ctx, os.Getenv("BY_PG_DSN"))
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close(ctx)
		for _, q := range []string{`SET statement_timeout = '30s'`, `DROP TABLE IF EXISTS by_src`, `DROP TABLE IF EXISTS ba_t`,
			`DROP DOMAIN IF EXISTS bytes`, `CREATE DOMAIN bytes AS bytea`} {
			if _, err := conn.Exec(ctx, q).ReadAll(); err != nil {
				t.Fatal(err)
			}
		}
		r = byAsgPG{ctx, conn}
	} else {
		db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		r = byAsgEngine{ctx, db}
	}
	for _, q := range setup {
		if err := r.exec(q); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range byAsgCells() {
		got := byAsgRun(t, r, c)
		if pg {
			t.Logf("%q: %q,", c.name, got)
			continue
		}
		want, ok := byAsgAnswers[c.name]
		if rest, kept := strings.CutPrefix(c.name, "varchar3/"); kept {
			// text-collation#r8 (kept superset): a VARCHAR(n) column keeps no
			// length here, so the `\x41` PostgreSQL refuses 22001 into a
			// VARCHAR(3) is stored as into TEXT — as an overlong text is.
			want, ok = byAsgAnswers["text/"+rest]
		}
		if !ok || got != want {
			t.Errorf("%s: %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.name, c.stmts[2], got, want)
		}
	}
}

// TestArcBYR2ByteaFunctionArgumentIsByteain: a quoted literal as the bytea
// argument of encode / get_byte / set_byte is read by byteain, as PostgreSQL
// 17.11 coerces it (measured case P2). At c67ebf5b and 714098e5 an even-length hex
// text was read as the bytes it spells (`encode('6869', 'hex')` answered 6869)
// and `\x…` as its characters.
func TestArcBYR2ByteaFunctionArgumentIsByteain(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, c := range []struct{ sql, want string }{
		{`SELECT encode('6869', 'hex') AS v`, "36383639"},
		{`SELECT encode('\x6869', 'hex') AS v`, "6869"},
		{`SELECT encode('a\\b', 'hex') AS v`, "615c62"},
		{`SELECT get_byte('\x41', 0) AS v`, "65"},
		{`SELECT get_byte('41', 0) AS v`, "52"},
		{`SELECT encode(set_byte('\x4142', 0, 67), 'hex') AS v`, "4342"},
		{`SELECT encode('a\b', 'hex') AS v`, "ERR 22P02"},
		{`SELECT encode('\x686', 'hex') AS v`, "ERR 22023"},
		{`SELECT get_byte('', 0) AS v`, "ERR 2202E"},
	} {
		res, err := db.Query(ctx, c.sql)
		got := ""
		if err != nil {
			got = "ERR " + sqlerr.StateOf(err)
		} else if len(res.Rows) == 1 {
			got = fmt.Sprint(res.Rows[0]["v"])
		}
		if got != c.want {
			t.Errorf("%s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.sql, got, c.want)
		}
	}
}
