// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// ARC PW ROUND 2: the review's four blocking findings as gate rows, each
// measured on PostgreSQL 17.11 over the same pgconn calls (pinned in
// arc_pw_param_types_r2_pins_test.go; with WADJET_PG_DSN the PostgreSQL half
// is re-measured, and a pin that starts agreeing fails).
//
//   - b1/*: a parameter's type is decided at Parse, against the catalog as it
//     stands then — a DROP / CREATE between two Parses of one text, on this
//     connection or another, retypes it; a statement prepared before the DDL
//     keeps the type Parse gave it.
//   - b2/*: the type of `$1 = <arithmetic>` is PostgreSQL's operator result
//     type over the operands, per operator × operand pair — not this engine's
//     declaration of the result column.
//   - b3/*: the ONE date/time text grammar, per spelling, through the
//     TIMESTAMP literal, a CAST from text to TIMESTAMP and to DATE, and a
//     timestamp / timestamptz parameter against a DATE and a TIMESTAMP column.
//   - b4/*: a statement Describe's column OID against the data row's width
//     with binary results.
//   - p1/*: `n = $1 OR $1 IS NULL` with a NULL parameter of a type n does not
//     compare with.
//   - n3/*: a column named like the walk's placeholder sentinel.

// pwR2Side is one server: two connections to it (b1's DDL runs on the second).
type pwR2Side struct {
	a, b   *pgconn.PgConn
	engine bool
}

type pwR2Cell struct {
	name string
	run  func(ctx context.Context, s pwR2Side) string
}

// pwR2Exec runs simple-protocol statements on c, the DDL respelled for this
// engine, and renders the last one's rows when it is a SELECT, or the first
// error.
func pwR2Exec(ctx context.Context, s pwR2Side, c *pgconn.PgConn, stmts ...string) string {
	out := ""
	for _, q := range stmts {
		res, err := c.Exec(ctx, pwDialect(s.engine, q)).ReadAll()
		if err != nil {
			return pwErr(err)
		}
		out = ""
		if strings.HasPrefix(q, "SELECT") && len(res) > 0 {
			out = "rows=[" + pwRows(res[len(res)-1].Rows) + "]"
		}
	}
	return out
}

// pwR2Prep is Parse + Describe (statement) of sql with the declared oids, then
// — unless vals is nil — Bind / Execute of vals (a nil entry is SQL NULL) with
// the result format given. An error is `ERR <SQLSTATE>` alone.
func pwR2Prep(ctx context.Context, c *pgconn.PgConn, name, sql string, oids []uint32, vals []*string, resultFmt int16) string {
	sd, err := c.Prepare(ctx, name, sql, oids)
	if err != nil {
		return pwErr(err)
	}
	var fo []string
	for _, f := range sd.Fields {
		fo = append(fo, fmt.Sprint(f.DataTypeOID))
	}
	out := fmt.Sprintf("params=%v fields=%s", sd.ParamOIDs, strings.Join(fo, ","))
	if vals == nil {
		return out
	}
	raw := make([][]byte, len(vals))
	for i, v := range vals {
		if v != nil {
			raw[i] = []byte(*v)
		}
	}
	rr := c.ExecPrepared(ctx, name, raw, nil, []int16{resultFmt}).Read()
	if rr.Err != nil {
		// Where it was raised is not part of the answer: PostgreSQL raises
		// some refusals at Prepare that this engine raises at Execute, and a
		// client sees the same error (arc_pw_param_types_wire_test.go's
		// convention).
		return pwErr(rr.Err)
	}
	if resultFmt == 1 {
		var cells []string
		for _, r := range rr.Rows {
			for _, v := range r {
				cells = append(cells, fmt.Sprintf("%d:%s", len(v), hex.EncodeToString(v)))
			}
		}
		return out + " binary=[" + strings.Join(cells, " ") + "]"
	}
	return out + " rows=[" + pwRows(rr.Rows) + "]"
}

func pwStr(s string) *string { return &s }

func pwR2Cells() []pwR2Cell {
	var cs []pwR2Cell
	add := func(name string, run func(ctx context.Context, s pwR2Side) string) {
		cs = append(cs, pwR2Cell{name: name, run: run})
	}

	// B1: two Parses of one text with a DROP / CREATE between them, the DDL
	// on the same connection or on another.
	for _, where := range []string{"same", "other"} {
		where := where
		ddl := func(s pwR2Side) *pgconn.PgConn {
			if where == "other" {
				return s.b
			}
			return s.a
		}
		add("b1/insert 2.5, NUMERIC then INTEGER/"+where, func(ctx context.Context, s pwR2Side) string {
			t := "r2a_" + where
			q := "INSERT INTO " + t + " VALUES (1, $1)"
			out := pwR2Exec(ctx, s, s.a, "CREATE TABLE "+t+" (id INTEGER, c NUMERIC(10,2))")
			out += " | " + pwR2Prep(ctx, s.a, "", q, []uint32{0}, []*string{pwStr("2.5")}, 0)
			out += " | " + pwR2Exec(ctx, s, ddl(s), "DROP TABLE "+t, "CREATE TABLE "+t+" (id INTEGER, c INTEGER)")
			out += " | " + pwR2Prep(ctx, s.a, "", q, []uint32{0}, []*string{pwStr("2.5")}, 0)
			return out + " | " + pwR2Exec(ctx, s, s.a, "SELECT c FROM "+t+" ORDER BY id")
		})
		add("b1/c < $1, TIMESTAMP then DATE/"+where, func(ctx context.Context, s pwR2Side) string {
			t := "r2b_" + where
			q := "SELECT id FROM " + t + " WHERE c < $1 ORDER BY id"
			out := pwR2Exec(ctx, s, s.a, "CREATE TABLE "+t+" (id INTEGER, c TIMESTAMP)",
				"INSERT INTO "+t+" VALUES (1, TIMESTAMP '2024-03-04 00:00:00')")
			out += " | " + pwR2Prep(ctx, s.a, "", q, []uint32{0}, []*string{pwStr("2024-03-04 18:00:00")}, 0)
			out += " | " + pwR2Exec(ctx, s, ddl(s), "DROP TABLE "+t, "CREATE TABLE "+t+" (id INTEGER, c DATE)",
				"INSERT INTO "+t+" VALUES (1, DATE '2024-03-04')")
			return out + " | " + pwR2Prep(ctx, s.a, "", q, []uint32{0}, []*string{pwStr("2024-03-04 18:00:00")}, 0)
		})
		add("b1/insert NULL, INTEGER then DATE/"+where, func(ctx context.Context, s pwR2Side) string {
			t := "r2c_" + where
			q := "INSERT INTO " + t + " VALUES (2, $1)"
			out := pwR2Exec(ctx, s, s.a, "CREATE TABLE "+t+" (id INTEGER, c INTEGER)")
			out += " | " + pwR2Prep(ctx, s.a, "", q, []uint32{0}, []*string{nil}, 0)
			out += " | " + pwR2Exec(ctx, s, ddl(s), "DROP TABLE "+t, "CREATE TABLE "+t+" (id INTEGER, c DATE)")
			out += " | " + pwR2Prep(ctx, s.a, "", q, []uint32{0}, []*string{nil}, 0)
			return out + " | " + pwR2Exec(ctx, s, s.a, "SELECT id, c FROM "+t+" ORDER BY id")
		})
		for i, typ := range []string{"NUMERIC(10,2)", "DOUBLE PRECISION", "VARCHAR", "BIGINT", "DATE", "TIMESTAMP"} {
			i, typ := i, typ
			add("b1/declares INTEGER then "+typ+"/"+where, func(ctx context.Context, s pwR2Side) string {
				t := fmt.Sprintf("r2d%d_%s", i, where)
				q := "INSERT INTO " + t + " VALUES (2, $1)"
				out := pwR2Exec(ctx, s, s.a, "CREATE TABLE "+t+" (id INTEGER, c INTEGER)")
				out += " | " + pwR2Prep(ctx, s.a, "", q, []uint32{0}, nil, 0)
				out += " | " + pwR2Exec(ctx, s, ddl(s), "DROP TABLE "+t, "CREATE TABLE "+t+" (id INTEGER, c "+typ+")")
				return out + " | " + pwR2Prep(ctx, s.a, "", q, []uint32{0}, nil, 0)
			})
		}
		// A NAMED statement prepared before the DDL keeps the type its Parse
		// decided (PostgreSQL re-plans it against the new table with the
		// parameter still numeric, and the assignment rounds).
		add("b1/named before the DDL, NUMERIC then INTEGER/"+where, func(ctx context.Context, s pwR2Side) string {
			t := "r2e_" + where
			name := "r2e_" + where
			out := pwR2Exec(ctx, s, s.a, "CREATE TABLE "+t+" (id INTEGER, c NUMERIC(10,2))")
			sd, err := s.a.Prepare(ctx, name, "INSERT INTO "+t+" VALUES (1, $1)", []uint32{0})
			if err != nil {
				return out + " | " + pwErr(err)
			}
			out += fmt.Sprintf(" | params=%v", sd.ParamOIDs)
			out += " | " + pwR2Exec(ctx, s, ddl(s), "DROP TABLE "+t, "CREATE TABLE "+t+" (id INTEGER, c INTEGER)")
			rr := s.a.ExecPrepared(ctx, name, [][]byte{[]byte("2.5")}, nil, nil).Read()
			if rr.Err != nil {
				out += " | " + pwErr(rr.Err)
			} else {
				out += " | " + rr.CommandTag.String()
			}
			s.a.Exec(ctx, "DEALLOCATE "+name).ReadAll()
			return out + " | " + pwR2Exec(ctx, s, s.a, "SELECT c FROM "+t+" ORDER BY id")
		})
	}

	// B2: `$1 = a <op> b` per operator × operand pair. Operands: the fixture's
	// integer, bigint, numeric, real and double precision columns and an
	// integer, a bigint and a numeric literal.
	operands := []string{"n", "b8", "num", "r", "f", "2", "3000000000", "2.5"}
	isLit := func(o string) bool { return o[0] >= '0' && o[0] <= '9' }
	for _, a := range operands {
		for _, op := range []string{"+", "-", "*", "/", "%"} {
			for _, b := range operands {
				if isLit(a) && isLit(b) {
					continue
				}
				e := a + " " + op + " " + b
				add("b2/$1 = "+e, func(ctx context.Context, s pwR2Side) string {
					return pwR2Prep(ctx, s.a, "", "SELECT id FROM p WHERE $1 = "+e+" ORDER BY id", nil, nil, 0)
				})
			}
		}
	}
	for _, w := range []string{"$1 = -n", "$1 = -b8", "$1 = -num", "$1 = -r", "$1 = -f", "$1 = 2 * 3", "$1 = 2.5 * 2",
		"$1 = -7", "$1 = 7", "$1 = 3000000000", "$1 = 1.5", "$1 = n + 1 + 1", "$1 = (n + 1) * 2", "n * 2 = $1",
		"$1 < id * 10", "$1 IN (n + 1, n * 2)", "$1 BETWEEN n - 1 AND n + 1", "n + $1 = 9", "$1 = d - d",
		"$1 = d + 1", "$1 = d - 1", "$1 = 1 + d", "$1 = n * 0x10", "$1 = n * 1_000"} {
		w := w
		add("b2/"+w, func(ctx context.Context, s pwR2Side) string {
			return pwR2Prep(ctx, s.a, "", "SELECT id FROM p WHERE "+w+" ORDER BY id", nil, nil, 0)
		})
	}
	add("b2/derived m = $1 over n * 2 AS m", func(ctx context.Context, s pwR2Side) string {
		return pwR2Prep(ctx, s.a, "", "SELECT m FROM (SELECT n * 2 AS m FROM p) x WHERE m = $1 ORDER BY m", nil, nil, 0)
	})
	// The value half: an undeclared parameter beside integer arithmetic
	// binds and answers.
	add("b2/value $1 = n * 2 with 14", func(ctx context.Context, s pwR2Side) string {
		return pwR2Prep(ctx, s.a, "", "SELECT id FROM p WHERE $1 = n * 2 ORDER BY id", nil, []*string{pwStr("14")}, 0)
	})
	add("b2/value $1 = n / 2 with 3", func(ctx context.Context, s pwR2Side) string {
		return pwR2Prep(ctx, s.a, "", "SELECT id FROM p WHERE $1 = n / 2 ORDER BY id", nil, []*string{pwStr("3")}, 0)
	})
	// An integer parameter's text that is no integer is 22P02 at the
	// parameter (int4in), wherever it lands — beside an expression too.
	for _, c := range []struct {
		name, sql string
		oid       uint32
		v         string
	}{
		{"$1 = n * 2 with 14.5", "SELECT id FROM p WHERE $1 = n * 2 ORDER BY id", 0, "14.5"},
		{"$1 = n + 1 int4 2.5", "SELECT id FROM p WHERE $1 = n + 1 ORDER BY id", oidInt4, "2.5"},
		{"$1 = b8 + 1 int8 2.5", "SELECT id FROM p WHERE $1 = b8 + 1 ORDER BY id", oidInt8, "2.5"},
		{"$1 = n int4 x", "SELECT id FROM p WHERE $1 = n ORDER BY id", oidInt4, "x"},
		{"SELECT $1 int4 ' 7 '", "SELECT $1", oidInt4, " 7 "},
	} {
		c := c
		add("b2/value "+c.name, func(ctx context.Context, s pwR2Side) string {
			return pwR2Prep(ctx, s.a, "", c.sql, []uint32{c.oid}, []*string{pwStr(c.v)}, 0)
		})
	}

	// B3: the date/time text grammar per spelling.
	for _, sp := range pwR2Spellings {
		sp, label := sp, strings.ReplaceAll(sp, "\t", `\t`)
		lit := strings.ReplaceAll(sp, "'", "''")
		add("b3/TIMESTAMP literal/"+label, func(ctx context.Context, s pwR2Side) string {
			return pwR2Exec(ctx, s, s.a, "SELECT TIMESTAMP '"+lit+"'")
		})
		add("b3/CAST AS TIMESTAMP/"+label, func(ctx context.Context, s pwR2Side) string {
			return pwR2Exec(ctx, s, s.a, "SELECT CAST('"+lit+"' AS TIMESTAMP)")
		})
		add("b3/CAST AS DATE/"+label, func(ctx context.Context, s pwR2Side) string {
			return pwR2Exec(ctx, s, s.a, "SELECT CAST('"+lit+"' AS DATE)")
		})
		add("b3/d <= $1 timestamp/"+label, func(ctx context.Context, s pwR2Side) string {
			return pwR2Prep(ctx, s.a, "", "SELECT id FROM p WHERE d <= $1 ORDER BY id", []uint32{oidTimestamp}, []*string{pwStr(sp)}, 0)
		})
		add("b3/ts >= $1 timestamp/"+label, func(ctx context.Context, s pwR2Side) string {
			return pwR2Prep(ctx, s.a, "", "SELECT id FROM p WHERE ts >= $1 ORDER BY id", []uint32{oidTimestamp}, []*string{pwStr(sp)}, 0)
		})
		add("b3/ts >= $1 timestamptz/"+label, func(ctx context.Context, s pwR2Side) string {
			return pwR2Prep(ctx, s.a, "", "SELECT id FROM p WHERE ts >= $1 ORDER BY id", []uint32{oidTimestampTZ}, []*string{pwStr(sp)}, 0)
		})
	}

	// B4: the statement Describe's OID against the data row, binary results.
	for _, c := range []struct {
		name, sql string
		oid       uint32
		v         string
		bin       bool
	}{
		{"SELECT $1 int4 '7'", "SELECT $1", oidInt4, "7", false},
		{"SELECT $1 int4 '-7'", "SELECT $1", oidInt4, "-7", false},
		{"SELECT $1 int4 '99999999999'", "SELECT $1", oidInt4, "99999999999", false},
		{"SELECT $1 int2 '-7'", "SELECT $1", oidInt2, "-7", false},
		{"SELECT $1 int2 '40000'", "SELECT $1", oidInt2, "40000", false},
		{"SELECT $1 int8 '-7'", "SELECT $1", oidInt8, "-7", false},
		{"SELECT COALESCE($1, 0) int4 '-7'", "SELECT COALESCE($1, 0)", oidInt4, "-7", false},
		{"SELECT $1 int4 binary -7", "SELECT $1", oidInt4, "\xff\xff\xff\xf9", true},
		{"SELECT $1 int2 binary -7", "SELECT $1", oidInt2, "\xff\xf9", true},
	} {
		c := c
		add("b4/"+c.name, func(ctx context.Context, s pwR2Side) string {
			sd, err := s.a.Prepare(ctx, "", c.sql, []uint32{c.oid})
			if err != nil {
				return pwErr(err)
			}
			pf := int16(0)
			if c.bin {
				pf = 1
			}
			rr := s.a.ExecPrepared(ctx, "", [][]byte{[]byte(c.v)}, []int16{pf}, []int16{1}).Read()
			out := fmt.Sprintf("describe=%d", sd.Fields[0].DataTypeOID)
			if rr.Err != nil {
				return out + " " + pwErr(rr.Err)
			}
			return out + fmt.Sprintf(" bytes=%d", len(rr.Rows[0][0]))
		})
	}

	// P1: a NULL parameter of a type the column does not compare with.
	for _, oid := range []uint32{0, oidInt4, oidDate, oidTimestamp, oidTimestampTZ, oidBool, oidUUID} {
		oid := oid
		add(fmt.Sprintf("p1/n = $1 OR $1 IS NULL/%d", oid), func(ctx context.Context, s pwR2Side) string {
			return pwR2Prep(ctx, s.a, "", "SELECT id FROM p WHERE n = $1 OR $1 IS NULL ORDER BY id", []uint32{oid}, []*string{nil}, 0)
		})
	}

	// N3: a column named like the placeholder sentinel.
	add("n3/__pw_param_1 = $1", func(ctx context.Context, s pwR2Side) string {
		out := pwR2Exec(ctx, s, s.a, "CREATE TABLE r2n (id INTEGER, __pw_param_1 INTEGER)", "INSERT INTO r2n VALUES (1, 7)")
		return out + " | " + pwR2Prep(ctx, s.a, "", "SELECT id FROM r2n WHERE __pw_param_1 = $1", nil, []*string{pwStr("7")}, 0)
	})
	return cs
}

// pwR2Spellings is the date/time text grammar's table: every accepted form
// (separators, field widths, a missing seconds field, the T separator, the
// zone suffixes) and every refused family, at their boundaries.
var pwR2Spellings = []string{
	"2024-03-04", "2024/03/04", "2024.03.04", "2024-3-4", "2024/3/4", "20240304", "10000-01-01", " 2024-03-04 ",
	"2024-0003-04", "2024-03-004",
	"2024-03-04 12:00", "2024-03-04 12:00:00", "2024-03-04 12:00:00.5", "2024-03-04 12:00:00.", "2024-03-04 1:2:3",
	"2024-03-04 1:2", "2024-03-04 12:5", "2024-03-04  12:00:00", "2024-03-04\t12:00",
	"2024-03-04T12:00", "2024-03-04t12:00:00", "2024-03-04T 12:00", "2024-3-4T1:2", "2024/03/04 12:00", "2024-3-4 12:00",
	"20240304 12:00", "10000-01-01 12:00:00",
	"2024-03-04 12:00:00Z", "2024-03-04 12:00:00z", "2024-03-04 12:00Z", "2024-03-04 12:00:00 Z", "2024-03-04 12:00:00.5Z",
	"2024-03-04 12:00:00+05", "2024-03-04 12:00:00-05", "2024-03-04 12:00:00+5", "2024-03-04 12:00+05",
	"2024-03-04 12:00:00+05:30", "2024-03-04 12:00:00-05:30", "2024-03-04 12:00:00+5:30", "2024-03-04 12:00:00+05:3",
	"2024-03-04 12:00:00+0530", "2024-03-04 12:00:00-0800", "2024-03-04 12:00:00+123", "2024-03-04 12:00:00 +05:30",
	"2024-03-04 12:00:00 -08", "2024-03-04 12:00:00+05:30:15", "2024-03-04 12:00:00-15:59:59", "2024-03-04 12:00:00+15:59",
	"2024-03-04 12:00:00.000+00", "2024-03-04 12:00:00.5+05", "2024-03-04 12:00:00.+05",
	"2024-03-04 24:00:00", "2024-03-04 23:59:60", "2024-03-04 12:30:60",
	// field ranges (22008) and displacement ranges (22009)
	"2024-03-04 24:00:01", "2024-03-04 24:00:00.5", "2024-03-04 23:59:60.5", "2024-03-04 25:00:00", "2024-03-04 12:60:00",
	"2024-02-30 12:00", "2024-13-01", "0000-01-01", "99999999999-01-01", "294276-12-31 23:59:59", "294277-01-01",
	"5874897-12-31", "5874898-01-01",
	"2024-03-04 12:00:00+16", "2024-03-04 12:00:00+99", "2024-03-04 12:00:00+05:60",
	// refused here: shapes outside the grammar, PostgreSQL's too or not
	"2024-03-04 12", "2024-03-04T12", "2024-03-04T", "2024-03-04 12:00:00ZZ", "2024-03-04 12:00:00 junk",
	"2024-03-04 12:00:00+", "2024-03-04 12:00:00:00", "2024-03-04TZ", "x", "allballs",
	"2024-03-04 12:00:00 UTC", "2024-03-04 12:00:00 America/New_York", "2024-03-04 12:00:00 EST", "2024-03-04 12:00 PM",
	"epoch", "infinity", "-infinity", "2024-03-04 BC", "March 4 2024", "Mar 4, 2024 12:00", "04/03/2024", "J2460374",
	"2024-03-04 012:00", "20240304T120000", "2024-03-04 12:00:00 - 05", "2024-03-04 12:00:00+0530:00",
}

func TestArcPWRound2MatchesPostgres(t *testing.T) {
	ctx := context.Background()
	engine := pwEngine(t, ctx)
	es := pwR2Side{a: engine.conn, b: connectPgconn(t, engine.addr), engine: true}
	var pg *pwR2Side
	if dsn := os.Getenv("WADJET_PG_DSN"); dsn != "" {
		side := pwPostgres(t, ctx, dsn)
		b, err := pgconn.Connect(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { b.Close(context.Background()) })
		for _, q := range []string{"SET statement_timeout = '30s'", "SET search_path = pw"} {
			if _, err := b.Exec(ctx, q).ReadAll(); err != nil {
				t.Fatal(err)
			}
		}
		pg = &pwR2Side{a: side.conn, b: b}
	}
	for _, s := range []*pwR2Side{&es, pg} {
		if s == nil {
			continue
		}
		for _, q := range []string{"SET TimeZone = 'UTC'"} {
			s.a.Exec(ctx, q).ReadAll()
		}
	}
	pins := pwR2Pins()
	cells := pwR2Cells()
	var dump []string
	seen := map[string]bool{}
	for _, c := range cells {
		if seen[c.name] {
			t.Fatalf("cell %q listed twice", c.name)
		}
		seen[c.name] = true
		cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		got := c.run(cctx, es)
		var gotPG string
		if pg != nil {
			gotPG = c.run(cctx, *pg)
		}
		cancel()
		dump = append(dump, c.name+"\t"+got+"\t"+gotPG)
		pin, ok := pins[c.name]
		if !ok {
			t.Errorf("%s: no pinned answer (engine %q)", c.name, got)
			continue
		}
		if got != pin.want {
			t.Errorf("%s: this engine answered\n  %s\nwant\n  %s", c.name, got, pin.want)
		}
		if pg == nil {
			continue
		}
		wantPG := pin.want
		if pin.pg != "" {
			wantPG = pin.pg
		}
		if gotPG != wantPG {
			t.Errorf("%s: PostgreSQL answered\n  %s\npinned\n  %s", c.name, gotPG, wantPG)
		}
	}
	for name := range pins {
		if !seen[name] {
			t.Errorf("pin %q names no cell", name)
		}
	}
	if path := os.Getenv("PW_R2_DUMP"); path != "" {
		sort.Strings(dump)
		os.WriteFile(path, []byte(strings.Join(dump, "\n")+"\n"), 0o644)
	}
}
