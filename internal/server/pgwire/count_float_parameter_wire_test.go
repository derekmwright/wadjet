// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

// A float4/float8 parameter in a LIMIT, OFFSET or FETCH FIRST count or a
// TABLESAMPLE percentage answers as PostgreSQL 17.11 does. Bind renders a
// float parameter as CAST('<text>' AS DOUBLE PRECISION | REAL) so that it
// keeps its type; those four positions read a number token, and without the
// parser reading that cast as its number they were 42601 `syntax error at or
// near "CAST"` — the spelling a client that binds a count as a double sends
// (JDBC setDouble, a Python float, a JavaScript number typed float8).
//
// Every want is PostgreSQL 17.11 over `p(id) = (1), (2)` through
// `PREPARE x(float8|float4|int8|numeric) AS …; EXECUTE x(…)`. The integer and
// numeric rows are the controls (they bind as a bare number and never
// changed). The two refusal rows are pinned where they differ from
// PostgreSQL: a count of -1 must stay a refusal (the plan reads a limit of
// -1 as "no limit", so reading the cast's text as a negative count would
// answer the whole table where PostgreSQL raises 2201W), and a fractional
// count is refused as the bare `LIMIT 1.5` is (PostgreSQL rounds it to 2 and
// answers both rows). A pin that moves fails: re-measure it.
func TestAFloatParameterCountsLikeItsNumber(t *testing.T) {
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "cntparam"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	srv := NewServer(db, Config{}, nil)
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	conn := connectPgconn(t, srv.Addr())
	for _, s := range []string{"CREATE TABLE p (id INTEGER)", "INSERT INTO p VALUES (1), (2)"} {
		if _, err := conn.Exec(ctx, s).ReadAll(); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}

	const (
		oidInt8    = 20
		oidFloat4  = 700
		oidFloat8  = 701
		oidNumeric = 1700
		limitQ     = "SELECT id FROM p ORDER BY id LIMIT $1"
		offsetQ    = "SELECT id FROM p ORDER BY id OFFSET $1"
		fetchQ     = "SELECT id FROM p ORDER BY id FETCH FIRST $1 ROWS ONLY"
		sampleQ    = "SELECT COUNT(*) FROM p TABLESAMPLE BERNOULLI ($1)"
	)
	f8 := func(v float64) []byte { return binary.BigEndian.AppendUint64(nil, math.Float64bits(v)) }
	f4 := func(v float32) []byte { return binary.BigEndian.AppendUint32(nil, math.Float32bits(v)) }
	for _, c := range []struct {
		name, sql string
		oid       uint32
		format    int16
		val       []byte
		want      string // rows joined by ";", or a SQLSTATE the cell must raise
	}{
		{"limit/float8", limitQ, oidFloat8, 0, []byte("1"), "1"},
		{"limit/float4", limitQ, oidFloat4, 0, []byte("1"), "1"},
		{"offset/float8", offsetQ, oidFloat8, 0, []byte("1"), "2"},
		{"offset/float4", offsetQ, oidFloat4, 0, []byte("1"), "2"},
		{"fetch-first/float8", fetchQ, oidFloat8, 0, []byte("1"), "1"},
		{"fetch-first/float4", fetchQ, oidFloat4, 0, []byte("1"), "1"},
		{"tablesample/float8", sampleQ, oidFloat8, 0, []byte("100"), "2"},
		{"tablesample/float4", sampleQ, oidFloat4, 0, []byte("100"), "2"},
		{"limit/float8/binary", limitQ, oidFloat8, 1, f8(1), "1"},
		{"limit/float4/binary", limitQ, oidFloat4, 1, f4(1), "1"},
		{"offset/float8/binary", offsetQ, oidFloat8, 1, f8(1), "2"},
		{"tablesample/float8/binary", sampleQ, oidFloat8, 1, f8(100), "2"},
		// controls: bound bare before and after the float change
		{"limit/int8", limitQ, oidInt8, 0, []byte("1"), "1"},
		{"limit/numeric", limitQ, oidNumeric, 0, []byte("1"), "1"},
		// pinned refusals (PostgreSQL: 2201W, and `1;2`)
		{"limit/float8/negative", limitQ, oidFloat8, 0, []byte("-1"), "42601"},
		{"limit/float8/fraction", limitQ, oidFloat8, 0, []byte("1.5"), "42000"},
	} {
		t.Run(c.name, func(t *testing.T) {
			res := conn.ExecParams(ctx, c.sql, [][]byte{c.val}, []uint32{c.oid}, []int16{c.format}, nil).Read()
			var rows []string
			for _, r := range res.Rows {
				rows = append(rows, string(r[0]))
			}
			got := strings.Join(rows, ";")
			if res.Err != nil {
				state := "(uncoded)"
				var pe *pgconn.PgError
				if errors.As(res.Err, &pe) {
					state = pe.Code
				}
				if state != c.want {
					t.Fatalf("raised %s (%v), want %s", state, res.Err, c.want)
				}
				return
			}
			if got != c.want {
				t.Fatalf("answered %q, want %s", got, c.want)
			}
		})
	}
}

// The count's `CAST('<text>' AS DOUBLE PRECISION | REAL)` reads a number
// only when the text is PostgreSQL's float input. The SQL lexer's number
// grammar is wider (`0b11`, `0o7`, `1_0`) and it skips a comment (`1--`,
// `1/*x*/`); read through the lexer alone, those counted (OFFSET 3, 7, 10, 1)
// where PostgreSQL 17.11 raises 22P02 and v0.25.1 raised 42601. They are
// pinned at 42601, the bare spelling's refusal. The spellings the float input
// accepts answer as their bare number does: surrounding whitespace and a
// leading `+` answer PostgreSQL's rows; a fraction, an exponent and a
// trailing point stay the bare spelling's pinned 42000 (`LIMIT 1.5`,
// `LIMIT 1e2`; the plan reads an integer count).
//
// Every row is PostgreSQL 17.11 over q(id) = 1..12 and p(id) = 1..3 through
// the same statement (psql, wadjet-pg-ir6), except the three pins.
func TestACountCastReadsOnlyPostgresFloatInput(t *testing.T) {
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "cntinput"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	srv := NewServer(db, Config{}, nil)
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	conn := connectPgconn(t, srv.Addr())
	for _, s := range []string{
		"CREATE TABLE q (id INTEGER)",
		"INSERT INTO q VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12)",
		"CREATE TABLE p (id INTEGER)",
		"INSERT INTO p VALUES (1), (2), (3)",
	} {
		if _, err := conn.Exec(ctx, s).ReadAll(); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	off := func(text, typ string) string {
		return "SELECT id FROM q ORDER BY id OFFSET CAST('" + text + "' AS " + typ + ")"
	}
	lim := func(text, typ string) string {
		return "SELECT id FROM p ORDER BY id LIMIT CAST('" + text + "' AS " + typ + ")"
	}
	const (
		f8     = "DOUBLE PRECISION"
		f4     = "REAL"
		from2  = "2;3;4;5;6;7;8;9;10;11;12"
		refuse = "42601" // PostgreSQL 22P02; the bare spelling's refusal, as v0.25.1
	)
	for _, c := range []struct{ name, sql, want string }{
		// refused by the float input (PostgreSQL 22P02)
		{"offset/0b11", off("0b11", f8), refuse},
		{"offset/0o7", off("0o7", f8), refuse},
		{"offset/1_0", off("1_0", f8), refuse},
		{"offset/line-comment", off("1--", f8), refuse},
		{"offset/block-comment", off("1/*x*/", f8), refuse},
		{"offset/sign-space", off("+ 1", f8), refuse},
		{"limit/0b11/real", lim("0b11", f4), refuse},
		{"limit/0o7/real", lim("0o7", f4), refuse},
		{"limit/1_0/real", lim("1_0", f4), refuse},
		{"limit/line-comment/real", lim("1--", f4), refuse},
		// accepted by the float input
		{"offset/padded", off(" 1 ", f8), from2},
		{"offset/plus", off("+1", f8), from2},
		{"limit/plus/real", lim("+2", f4), "1;2"},
		{"limit/tab-newline", lim("\t2\n", f8), "1;2"},
		{"fetch/plus", "SELECT id FROM p ORDER BY id FETCH FIRST CAST('+1' AS DOUBLE PRECISION) ROWS ONLY", "1"},
		{"tablesample/exponent/real", "SELECT COUNT(*) FROM p TABLESAMPLE BERNOULLI (CAST('1e2' AS REAL))", "3"},
		// pinned: the count is the bare spelling's, and the LIMIT / OFFSET
		// plan reads an integer, so a fraction, an exponent or a trailing
		// point is the bare `LIMIT 1.5` / `LIMIT 1e2` / `OFFSET 2.` 42000
		// (PostgreSQL: .5 rounds to 0 — every row, and none; 1e1 → 11;12;
		// 2. → 3..12; 1e2 → 1;2;3; 1e0 → 1)
		{"offset/exponent", off("1e1", f8), "42000"},
		{"offset/trailing-point", off("2.", f8), "42000"},
		{"limit/exponent", lim("1e2", f8), "42000"},
		{"limit/exponent/real", lim("1e0", f4), "42000"},
		{"offset/fraction", off(".5", f8), "42000"},
		{"limit/fraction", lim(".5", f8), "42000"},
	} {
		t.Run(c.name, func(t *testing.T) {
			results, err := conn.Exec(ctx, c.sql).ReadAll()
			if err != nil {
				state := "(uncoded)"
				var pe *pgconn.PgError
				if errors.As(err, &pe) {
					state = pe.Code
				}
				if state != c.want {
					t.Fatalf("raised %s (%v), want %s", state, err, c.want)
				}
				return
			}
			var rows []string
			for _, r := range results[0].Rows {
				rows = append(rows, string(r[0]))
			}
			if got := strings.Join(rows, ";"); got != c.want {
				t.Fatalf("answered %q, want %s", got, c.want)
			}
		})
	}
}
