// SPDX-License-Identifier: MIT

package pgwire

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// #1590 over the extended protocol: a `$1` the client sends UNTYPED (OID 0,
// text format) in a position that resolves it to bytea — COALESCE, NULLIF,
// CASE, GREATEST, `||`, a comparison, a LAG default, position — is read by
// byteain on PostgreSQL, so the text `\x6869` binds the two bytes `hi`. The
// TEXT column s in the same positions is the control. Each cell records the
// parameter type Describe reports and the answer. U1590_PG_DSN=<dsn> prints
// PostgreSQL 17.11's answers (testdata/arc_1590_bytea_unknown_param_wire_pg17.tsv).

var u1590WireParams = []struct{ name, text string }{
	{"hex", `\x6869`}, {"esc", "hi"}, {"badhex", `\x68zz`},
}

var u1590WirePositions = []struct{ name, q string }{
	{"coalesce", `SELECT id, encode(COALESCE({C}, $1), 'hex') || '/' || CAST(length(COALESCE({C}, $1)) AS TEXT) FROM u_w`},
	{"nullif", `SELECT id, encode(NULLIF($1, {C}), 'hex') FROM u_w`},
	{"case", `SELECT id, encode(CASE WHEN id = 2 THEN $1 ELSE {C} END, 'hex') FROM u_w`},
	{"greatest", `SELECT id, encode(GREATEST({C}, $1), 'hex') FROM u_w`},
	{"concat", `SELECT id, encode({C} || $1, 'hex') FROM u_w`},
	{"eq", `SELECT id FROM u_w WHERE {C} = $1`},
	{"lag-default", `SELECT id, encode(lag({C}, 1, $1) OVER (ORDER BY id), 'hex') FROM u_w`},
	{"position", `SELECT id, position($1 in {C}) FROM u_w`},
}

type u1590WireCell struct{ name, sql, param string }

// u1590WireCells: the BYTES column b, and the TEXT column s with encode(x,
// 'hex') read as convert_to-free text (encode is dropped for it).
func u1590WireCells() []u1590WireCell {
	var out []u1590WireCell
	for _, fam := range []string{"bytes", "text"} {
		for _, p := range u1590WirePositions {
			q := p.q
			if fam == "bytes" {
				q = strings.ReplaceAll(q, "{C}", "b")
			} else {
				q = strings.ReplaceAll(q, "{C}", "s")
				q = strings.ReplaceAll(q, "encode(", "(")
				q = strings.ReplaceAll(q, ", 'hex')", ")")
			}
			for _, v := range u1590WireParams {
				out = append(out, u1590WireCell{name: fam + "/" + p.name + "/" + v.name, sql: q, param: v.text})
			}
		}
	}
	return out
}

func u1590WireRun(ctx context.Context, conn *pgconn.PgConn, c u1590WireCell) string {
	sd, err := conn.Prepare(ctx, "", c.sql, nil)
	if err != nil {
		return u1590WireErr(err)
	}
	oids := fmt.Sprint(sd.ParamOIDs)
	res := conn.ExecParams(ctx, c.sql, [][]byte{[]byte(c.param)}, []uint32{0}, []int16{0}, nil).Read()
	if res.Err != nil {
		return "param=" + oids + " " + u1590WireErr(res.Err)
	}
	return "param=" + oids + " " + byR2WireRender(res)
}

func u1590WireErr(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return "ERR " + pe.Code
	}
	return "ERR " + err.Error()
}

func TestArc1590ByteaUnknownParamOnTheWire(t *testing.T) {
	ctx := context.Background()
	if dsn := os.Getenv("U1590_PG_DSN"); dsn != "" {
		conn, err := pgconn.Connect(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close(ctx)
		if _, err := conn.Exec(ctx, `SET statement_timeout = '30s'; DROP SCHEMA IF EXISTS u1590w CASCADE; CREATE SCHEMA u1590w; SET search_path = u1590w;
CREATE TABLE u_w (id bigint, b bytea, s text); INSERT INTO u_w VALUES (1, '\x6869', 'hi'), (2, NULL, NULL), (3, '\x41', 'A');`).ReadAll(); err != nil {
			t.Fatal(err)
		}
		for _, c := range u1590WireCells() {
			fmt.Printf("%s\t%s\n", c.name, u1590WireRun(ctx, conn, c))
		}
		return
	}
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	sc := parquet.Schema{Columns: []parquet.Column{{Name: "id", Type: parquet.TypeInt64},
		{Name: "b", Type: parquet.TypeBytes, Nullable: true}, {Name: "s", Type: parquet.TypeString, Nullable: true}}}
	if err := db.CreateTable(ctx, "u_w", sc, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("u_w", sc, nil, ingest.Config{MaxBufferRows: 16})
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int64(1), "b": []byte("hi"), "s": "hi"}, {"id": int64(2), "b": nil, "s": nil}, {"id": int64(3), "b": []byte("A"), "s": "A"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	srv := NewServer(db, Config{}, nil)
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	conn := connectPgconn(t, srv.Addr())
	answers := map[string]string{}
	f, err := os.Open("testdata/arc_1590_bytea_unknown_param_wire_pg17.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		if line := s.Text(); line != "" && !strings.HasPrefix(line, "#") {
			name, want, _ := strings.Cut(line, "\t")
			answers[name] = want
		}
	}
	dump := os.Getenv("U1590_WIRE_DUMP")
	var lines []string
	for _, c := range u1590WireCells() {
		want, ok := answers[c.name]
		if !ok {
			t.Fatalf("%s: no PostgreSQL answer", c.name)
		}
		got := u1590WireRun(ctx, conn, c)
		if dump != "" {
			verdict := "right"
			if got != want {
				verdict = "wrong"
			}
			lines = append(lines, c.name+"\twire\t"+verdict+"\t"+got)
			continue
		}
		if reason, kept := u1590WireKept[c.name]; kept {
			if got == want {
				t.Errorf("%s now agrees with PostgreSQL: delete its u1590WireKept line (%s)", c.name, reason)
			}
			continue
		}
		if got != want {
			t.Errorf("%s: %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.name, c.sql, got, want)
		}
	}
	if dump != "" {
		if err := os.WriteFile(dump, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// u1590WireKept are the cells still different from PostgreSQL, with cause.
var u1590WireKept = map[string]string{
	// position($1 in col): Describe reports 0 (unresolved) where PostgreSQL
	// reports the column's type, bytea 17 or text 25; the ANSWERS agree.
	"bytes/position/hex":    "Describe 0, not 17",
	"bytes/position/esc":    "Describe 0, not 17",
	"bytes/position/badhex": "Describe 0, not 17",
	"text/position/hex":     "Describe 0, not 25",
	"text/position/esc":     "Describe 0, not 25",
	"text/position/badhex":  "Describe 0, not 25",
}
