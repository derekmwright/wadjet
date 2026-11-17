// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// The bytea LIKE matrix (#1593). PostgreSQL has a bytea ~~ bytea operator whose
// pattern takes the backslash as its default escape; a text value against a
// bytea pattern has no operator (42883). The matrix crosses operator x declared
// kind of the value x declared kind of the pattern x content pair.
//
// LK_PG_DSN=<dsn> runs the same cells on PostgreSQL (BYTES is a domain over
// bytea there); LK_DUMP=<file> writes the answer table the run produced.

// lkPair is a value and a pattern, as hex.
type lkPair struct{ v, p string }

// Contents: backslash-escaped %, _ and backslash; a lone trailing backslash;
// % and _ against bytes >= 0x80 and 0x00; empty pattern; empty value.
var lkPairs = []lkPair{
	{"5c41", "5c41"},
	{"41", "5c41"},
	{"41", "41"},
	{"5c25", "5c25"},
	{"25", "5c25"},
	{"4142", "5c25"},
	{"5f", "5c5f"},
	{"41", "5c5f"},
	{"5c", "5c5c"},
	{"5c5c", "5c5c"},
	{"5c", "5c"},
	{"41", "5c"},
	{"", "5c"},
	{"415c", "415c"},
	{"41", "415c"},
	{"80", "5f"},
	{"80", "25"},
	{"ff41", "255f"},
	{"00", "5f"},
	{"00", "25"},
	{"4100", "4125"},
	{"4100", "415f"},
	{"41", ""},
	{"", ""},
	{"", "25"},
	{"", "5f"},
	{"4142", "255f"},
	{"4142", "5f42"},
	{"4142", "4141"},
	{"41", "5c5c41"},
	{"5c41", "5c5c41"},
	{"5c5c", "5c"},
	{"41", "615c"},
	{"4180", "41255f"},
	{"41", "5c255c5f"},
	{"4142", "4125"},
	{"4141", "5c4141"},
	{"21", "2121"},
}

var lkOps = []string{"LIKE", "NOT LIKE", "ILIKE", "NOT ILIKE", "LIKE ESCAPE '!'", "LIKE ESCAPE ''", "LIKE ESCAPE '\\'", "LIKE ESCAPE NULL"}

var lkKinds = []string{"bcol", "dec", "cast", "hexlit", "qlit", "tcol", "texpr", "null", "nullb"}

func lkAscii(h string) (string, bool) {
	b, _ := hex.DecodeString(h)
	for _, c := range b {
		if c == 0 || c >= 0x80 {
			return "", false
		}
	}
	return string(b), true
}

// lkOperand writes one operand of kind k for content h; side is "v" or "p".
// ok is false where the kind cannot be written for the content (text cannot
// hold a NUL or a byte >= 0x80 here).
func lkOperand(k, side, h string) (string, bool) {
	txt, isTxt := lkAscii(h)
	switch k {
	case "bcol":
		return side + "b", true
	case "dec":
		return "decode('" + h + "', 'hex')", true
	case "cast":
		return "CAST('\\x" + h + "' AS BYTES)", true
	case "hexlit":
		return "'\\x" + h + "'", true
	case "qlit":
		if !isTxt {
			return "", false
		}
		return "'" + txt + "'", true
	case "tcol":
		if !isTxt {
			return "", false
		}
		return side + "t", true
	case "texpr":
		if !isTxt {
			return "", false
		}
		return "(" + side + "t || '')", true
	case "null":
		return "NULL", true
	case "nullb":
		return "CAST(NULL AS BYTES)", true
	}
	panic(k)
}

func lkIsBytes(k string) bool {
	return k == "bcol" || k == "dec" || k == "cast" || k == "nullb"
}

type lkCell struct {
	name       string
	id         int
	pred       string
	op, vk, pk string
}

func lkCells(kinds []string, ops []string, bytesOnly bool) []lkCell {
	var out []lkCell
	for i, pr := range lkPairs {
		for _, op := range ops {
			for _, vk := range kinds {
				for _, pk := range kinds {
					if bytesOnly && !lkIsBytes(vk) && !lkIsBytes(pk) {
						continue
					}
					v, ok1 := lkOperand(vk, "v", pr.v)
					p, ok2 := lkOperand(pk, "p", pr.p)
					if !ok1 || !ok2 {
						continue
					}
					pred := v + " " + op + " " + p
					if j := strings.Index(op, " ESCAPE"); j >= 0 {
						pred = v + " " + op[:j] + " " + p + op[j:]
					}
					out = append(out, lkCell{
						name: fmt.Sprintf("%s~%s/%s/%s/%s", pr.v, pr.p, op, vk, pk),
						id:   i, pred: pred, op: op, vk: vk, pk: pk,
					})
				}
			}
		}
	}
	return out
}

type lkRunner interface {
	exec(q string) error
	rows(q string) (string, error)
}

type lkEngine struct {
	ctx context.Context
	db  *DB
}

func (e lkEngine) exec(q string) error { _, err := e.db.Query(e.ctx, q); return err }
func (e lkEngine) rows(q string) (string, error) {
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
	return strings.Join(rows, ";"), nil
}

type lkPG struct {
	ctx  context.Context
	conn *pgconn.PgConn
}

func (p lkPG) exec(q string) error { _, err := p.conn.Exec(p.ctx, q).ReadAll(); return err }
func (p lkPG) rows(q string) (string, error) {
	res := p.conn.ExecParams(p.ctx, q, nil, nil, nil, nil).Read()
	if res.Err != nil {
		return "", res.Err
	}
	var rows []string
	for _, r := range res.Rows {
		var f []string
		for _, c := range r {
			switch {
			case c == nil:
				f = append(f, "NULL")
			case string(c) == "t":
				f = append(f, "true")
			case string(c) == "f":
				f = append(f, "false")
			default:
				f = append(f, string(c))
			}
		}
		rows = append(rows, strings.Join(f, "|"))
	}
	return strings.Join(rows, ";"), nil
}

func lkState(err error) string {
	if st := sqlerr.StateOf(err); st != "" {
		return st
	}
	if pe, ok := err.(*pgconn.PgError); ok {
		return pe.Code
	}
	return "?" + err.Error()
}

// lkSetup stores each content pair in its OWN table lk<i> (one row): a
// predicate that raises on one row must not be reached through another's, which
// PostgreSQL does not order. lkall holds every pair, for the scan arm.
func lkSetup(t *testing.T, r lkRunner) {
	for i := range lkPairs {
		_ = r.exec(fmt.Sprintf(`DROP TABLE IF EXISTS lk%d`, i))
		if err := r.exec(fmt.Sprintf(`CREATE TABLE lk%d (id BIGINT, vb BYTES, pb BYTES, vt TEXT, pt TEXT, nb BYTES)`, i)); err != nil {
			t.Fatal(err)
		}
	}
	_ = r.exec(`DROP TABLE IF EXISTS lkall`)
	if err := r.exec(`CREATE TABLE lkall (id BIGINT, vb BYTES, pb BYTES, vt TEXT, pt TEXT, nb BYTES)`); err != nil {
		t.Fatal(err)
	}
	for i, pr := range lkPairs {
		vt, pt := "NULL", "NULL"
		if s, ok := lkAscii(pr.v); ok {
			vt = "'" + s + "'"
		}
		if s, ok := lkAscii(pr.p); ok {
			pt = "'" + s + "'"
		}
		for _, tbl := range []string{fmt.Sprintf("lk%d", i), "lkall"} {
			q := fmt.Sprintf(`INSERT INTO %s (id, vb, pb, vt, pt) VALUES (%d, decode('%s','hex'), decode('%s','hex'), %s, %s)`, tbl, i, pr.v, pr.p, vt, pt)
			if err := r.exec(q); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
	}
}

// lkAnswer runs one cell, in a projection or in a WHERE.
func lkAnswer(r lkRunner, c lkCell, where bool) string {
	var q string
	if where {
		q = fmt.Sprintf(`SELECT count(*) FROM lk%d WHERE %s`, c.id, c.pred)
	} else {
		q = fmt.Sprintf(`SELECT %s FROM lk%d`, c.pred, c.id)
	}
	rows, err := r.rows(q)
	if err != nil {
		return "ERR " + lkState(err)
	}
	return rows
}

func lkOpen(t *testing.T, budget int64) (lkRunner, bool) {
	ctx := context.Background()
	if dsn := os.Getenv("LK_PG_DSN"); dsn != "" {
		conn, err := pgconn.Connect(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close(ctx) })
		for _, q := range []string{`SET statement_timeout = '30s'`, `DROP DOMAIN IF EXISTS bytes CASCADE`, `CREATE DOMAIN bytes AS bytea`} {
			if _, err := conn.Exec(ctx, q).ReadAll(); err != nil {
				t.Fatal(err)
			}
		}
		return lkPG{ctx, conn}, true
	}
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test", MemoryBudget: budget})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return lkEngine{ctx, db}, false
}

// lkExpect is the answer lkAnswers pins for a cell, in the arm's spelling.
func lkExpect(code byte, where bool) string {
	switch code {
	case 't':
		if where {
			return "1"
		}
		return "true"
	case 'f':
		if where {
			return "0"
		}
		return "false"
	case 'n':
		if where {
			return "0"
		}
		return "NULL"
	case 'a':
		return "ERR 22025"
	case 'b':
		return "ERR 22P02"
	case 'c':
		return "ERR 42883"
	}
	return "?"
}

// TestByteaLike1593Matrix: every matrix cell with a BYTES operand — a column,
// a decode(), a CAST, a typed NULL — answers as PostgreSQL 17.11 does, value
// or SQLSTATE, in a projection and in a WHERE over a stored column, and under
// a memory budget small enough that a spilling operator would spill. (LIKE has
// no spilled path of its own; the budget arm holds the answers to the same
// under the memory model every other operator runs in.) With LK_PG_DSN the same
// cells run on PostgreSQL and hold lkAnswers itself to the server.
func TestByteaLike1593Matrix(t *testing.T) {
	arms := []struct {
		name   string
		budget int64
		where  bool
	}{
		{"projection", 0, false},
		{"where", 0, true},
		{"projection-budget", 64 << 10, false},
		{"where-budget", 64 << 10, true},
	}
	for _, arm := range arms {
		if testing.Short() && arm.budget != 0 {
			continue
		}
		t.Run(arm.name, func(t *testing.T) {
			r, _ := lkOpen(t, arm.budget)
			lkSetup(t, r)
			cells := 0
			bad := 0
			for _, c := range lkCells(lkKinds, lkOps, true) {
				cells++
				codes, ok := lkAnswers[c.op+"|"+c.vk+"|"+c.pk]
				if !ok || len(codes) != len(lkPairs) || codes[c.id] == '-' {
					t.Fatalf("no pinned PostgreSQL answer for %s %s", c.name, c.pred)
				}
				want := lkExpect(codes[c.id], arm.where)
				if got := lkAnswer(r, c, arm.where); got != want {
					bad++
					if bad <= 20 {
						t.Errorf("%s: %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.name, c.pred, got, want)
					}
				}
			}
			if bad > 20 {
				t.Errorf("... %d of %d cells differ", bad, cells)
			}
			if cells == 0 {
				t.Fatal("no cells ran")
			}
		})
	}
}

func TestBytea1593Dump(t *testing.T) {
	out := os.Getenv("LK_DUMP")
	if out == "" {
		t.Skip("LK_DUMP not set")
	}
	r, _ := lkOpen(t, 0)
	lkSetup(t, r)
	var lines []string
	for _, c := range lkCells(lkKinds, lkOps, false) {
		lines = append(lines, c.name+"\tP\t"+c.pred+"\t"+lkAnswer(r, c, false))
		lines = append(lines, c.name+"\tW\t"+c.pred+"\t"+lkAnswer(r, c, true))
	}
	sort.Strings(lines)
	if err := os.WriteFile(out, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}
