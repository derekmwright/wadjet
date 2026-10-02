// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// ARC PW: A PARAMETER'S TYPE IS THE CLIENT'S OID, OR THE TYPE OF THE POSITION
// IT OCCUPIES, AND IT IS SPLICED AS A LITERAL OF THAT TYPE (#1426 #1410).
//
// The coverage table: declared OID × format × position, every cell measured on
// PostgreSQL 17.11 over the same pgconn calls (Prepare — Parse + Describe
// statement — then ExecPrepared) and pinned as PostgreSQL's answer. A cell this
// engine answers differently carries the catalog row that records why (pg is
// PostgreSQL's answer, want this engine's). With WADJET_PG_DSN naming a live
// 17.11 the PostgreSQL half is re-measured: a pin that starts agreeing fails,
// and deleting it is the proof.
//
// An answer is `params=[<ParameterDescription>] fields=<RowDescription OIDs>
// rows=[…]`, or `ERR <SQLSTATE>` wherever it was raised (PostgreSQL raises some
// at Prepare that this engine raises at Execute; a client sees the same
// refusal). A DML cell adds the target's stored value read back through the
// EMBEDDED engine (`stored=`), not through the wire.

// pwParam is one bound parameter: the OID declared at Parse (0 = left to the
// server), the format, and the value (a string is sent as text bytes; any
// other value is encoded with pgx's codec for the OID; nil is SQL NULL).
type pwParam struct {
	oid uint32
	bin bool
	v   any
}

type pwCell struct {
	name  string
	setup []string
	sql   string
	ps    []pwParam
	read  string // the stored value's read, run on the embedded engine
}

func pwT(s string) pwParam             { return pwParam{v: s} }
func pwO(oid uint32, s string) pwParam { return pwParam{oid: oid, v: s} }
func pwB(oid uint32, v any) pwParam    { return pwParam{oid: oid, bin: true, v: v} }
func pwNum(unscaled int64, exp int32) pgtype.Numeric {
	return pgtype.Numeric{Int: big.NewInt(unscaled), Exp: exp, Valid: true}
}

func pwTS(s string) time.Time {
	t, err := time.Parse("2006-01-02 15:04:05.999999", s)
	if err != nil {
		panic(err)
	}
	return t
}

// pwFixture is spelled for PostgreSQL; pwDialect respells its DDL for this
// engine (DOUBLE PRECISION and REAL are single-word types in this CREATE
// TABLE grammar, and BYTEA is BYTES).
var pwFixture = []string{
	`CREATE TABLE p (id INTEGER, n INTEGER, b8 BIGINT, f DOUBLE PRECISION, r REAL, num NUMERIC(10,2), s VARCHAR, t TEXT, d DATE, ts TIMESTAMP, bo BOOLEAN, u UUID, bt BYTEA)`,
	`INSERT INTO p VALUES (1, 7, 7, 7.5, 1.5, 7.25, 'abc', 'abc', DATE '2024-03-04', TIMESTAMP '2024-03-04 12:00:00', true, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', NULL)`,
	`INSERT INTO p VALUES (2, 14, 14, 0.5, 2.5, 0.5, 'xyz', 'xyz', DATE '1969-12-31', TIMESTAMP '1969-12-31 23:59:59.999', false, 'b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', NULL)`,
	`INSERT INTO p VALUES (3, 2, 3000000000, 2.5, 0.25, 2.5, 'a''b', 'a b', DATE '1000-01-01', TIMESTAMP '1000-01-01 00:00:00', true, NULL, NULL)`,
	`INSERT INTO p VALUES (4, 3, 3, 3.0, 3.0, 3.00, '3', '3', DATE '9999-12-31', TIMESTAMP '9999-12-31 23:59:59', NULL, NULL, NULL)`,
	`INSERT INTO p VALUES (5, NULL, NULL, NULL, NULL, NULL, NULL, NULL, DATE '2000-01-01', TIMESTAMP '2000-01-01 00:00:00', NULL, NULL, NULL)`,
	`INSERT INTO p VALUES (10, 1, 1, 1, 1, 1, 'q', 'q', DATE '1969-12-30', TIMESTAMP '1969-12-30 00:00:00', true, NULL, NULL)`,
	`CREATE TABLE q (k INTEGER, v DOUBLE PRECISION)`,
	`INSERT INTO q VALUES (7, 1.5), (14, 2.5), (99, 3.5)`,
}

func pwDialect(engine bool, s string) string {
	if engine && strings.HasPrefix(s, "CREATE TABLE") {
		return strings.NewReplacer("DOUBLE PRECISION", "DOUBLE", "REAL", "FLOAT32", "BYTEA", "BYTES").Replace(s)
	}
	return s
}

// pwSide is one server under test: a pgconn, and for this engine the
// embedded database the stored values are read back through.
type pwSide struct {
	conn   *pgconn.PgConn
	db     *wadjet.DB // nil for PostgreSQL
	engine bool
	addr   string // this engine's pgwire address
}

func pwEngine(t *testing.T, ctx context.Context) pwSide {
	t.Helper()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "pwparam"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	srv := NewServer(db, Config{}, nil)
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	side := pwSide{conn: connectPgconn(t, srv.Addr()), db: db, engine: true, addr: srv.Addr()}
	pwLoad(t, ctx, side)
	return side
}

func pwPostgres(t *testing.T, ctx context.Context, dsn string) pwSide {
	t.Helper()
	conn, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connecting to WADJET_PG_DSN: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	for _, s := range []string{"SET statement_timeout = '30s'", "DROP SCHEMA IF EXISTS pw CASCADE",
		"CREATE SCHEMA pw", "SET search_path = pw"} {
		if _, err := conn.Exec(ctx, s).ReadAll(); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	side := pwSide{conn: conn}
	pwLoad(t, ctx, side)
	return side
}

// pwLoad writes the fixture. The bytea value goes in as a binary parameter:
// a quoted '\x6869' assigned to a BYTES column stores its six characters here
// (a literal-assignment defect outside this arc, recorded in its notes).
func pwLoad(t *testing.T, ctx context.Context, side pwSide) {
	t.Helper()
	for _, s := range pwFixture {
		if _, err := side.conn.Exec(ctx, pwDialect(side.engine, s)).ReadAll(); err != nil {
			t.Fatalf("fixture %q: %v", s, err)
		}
	}
	for id, v := range map[int]string{1: "hi", 2: "\x00\xff\\"} {
		r := side.conn.ExecParams(ctx, fmt.Sprintf("UPDATE p SET bt = $1 WHERE id = %d", id),
			[][]byte{[]byte(v)}, []uint32{oidBytea}, []int16{1}, nil).Read()
		if r.Err != nil {
			t.Fatalf("fixture bytea %d: %v", id, r.Err)
		}
	}
}

// pwRun runs one cell and renders its answer.
func pwRun(ctx context.Context, side pwSide, c pwCell, k int) string {
	for _, s := range c.setup {
		if _, err := side.conn.Exec(ctx, pwDialect(side.engine, s)).ReadAll(); err != nil {
			return "SETUP " + pwErr(err)
		}
	}
	m := pgtype.NewMap()
	oids := make([]uint32, len(c.ps))
	vals := make([][]byte, len(c.ps))
	fmts := make([]int16, len(c.ps))
	for i, p := range c.ps {
		oids[i] = p.oid
		if p.bin {
			fmts[i] = 1
		}
		if p.v == nil {
			continue
		}
		if v, ok := p.v.(string); ok && !p.bin {
			vals[i] = []byte(v)
			continue
		}
		b, err := m.Encode(p.oid, fmts[i], p.v, nil)
		if err != nil {
			return "ENCODE " + err.Error()
		}
		vals[i] = b
	}
	name := fmt.Sprintf("pw%d", k)
	sd, err := side.conn.Prepare(ctx, name, c.sql, oids)
	if err != nil {
		return pwErr(err)
	}
	defer func() { side.conn.Exec(ctx, "DEALLOCATE "+name).ReadAll() }()
	var fo []string
	for _, f := range sd.Fields {
		fo = append(fo, fmt.Sprint(f.DataTypeOID))
	}
	out := fmt.Sprintf("params=%v fields=%s", sd.ParamOIDs, strings.Join(fo, ","))
	rr := side.conn.ExecPrepared(ctx, name, vals, fmts, nil).Read()
	if rr.Err != nil {
		return pwErr(rr.Err)
	}
	out += " rows=[" + pwRows(rr.Rows) + "]"
	if c.read != "" {
		if side.db == nil {
			res, err := side.conn.Exec(ctx, c.read).ReadAll()
			if err != nil {
				return out + " READ " + pwErr(err)
			}
			out += " stored=[" + pwRows(res[0].Rows) + "]"
		} else {
			res, err := side.db.Query(ctx, c.read)
			if err != nil {
				return out + " READ " + pwErr(err)
			}
			var rows []string
			for _, r := range res.Rows {
				var vs []string
				for i, col := range res.Columns {
					vs = append(vs, pwStored(r[col], res.ColumnMetas[i].TypeID))
				}
				rows = append(rows, strings.Join(vs, "|"))
			}
			out += " stored=[" + strings.Join(rows, " ; ") + "]"
		}
	}
	return out
}

// pwValue renders an embedded engine value the way the wire's text renders
// it, for the types the DML cells store.
func pwValue(v any) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case time.Time:
		if x.Hour() == 0 && x.Minute() == 0 && x.Second() == 0 && x.Nanosecond() == 0 {
			return x.Format("2006-01-02")
		}
		return x.Format("2006-01-02 15:04:05.999999")
	case float64:
		return fmt.Sprint(x)
	}
	return fmt.Sprint(v)
}

// pwStored renders a value the embedded engine read back, as the wire's text
// renders one of its type.
func pwStored(v any, typ parquet.TypeID) string {
	switch x := v.(type) {
	case int64:
		if typ == parquet.TypeTimestamp {
			return time.UnixMilli(x).UTC().Format("2006-01-02 15:04:05.999999")
		}
	case bool:
		if x {
			return "t"
		}
		return "f"
	}
	return pwValue(v)
}

func pwRows(rows [][][]byte) string {
	var out []string
	for _, r := range rows {
		var vs []string
		for _, v := range r {
			if v == nil {
				vs = append(vs, "NULL")
			} else {
				vs = append(vs, string(v))
			}
		}
		out = append(out, strings.Join(vs, "|"))
	}
	return strings.Join(out, " ; ")
}

func pwErr(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return "ERR " + pe.Code
	}
	return "ERR " + err.Error()
}

func TestArcPWParameterTypesMatchPostgres(t *testing.T) {
	ctx := context.Background()
	engine := pwEngine(t, ctx)
	var pg *pwSide
	if dsn := os.Getenv("WADJET_PG_DSN"); dsn != "" {
		side := pwPostgres(t, ctx, dsn)
		pg = &side
	}
	pins := pwPins()
	cells := pwCells()
	seen := map[string]bool{}
	for k, c := range cells {
		if seen[c.name] {
			t.Fatalf("cell %q listed twice", c.name)
		}
		seen[c.name] = true
		pin, ok := pins[c.name]
		if !ok {
			t.Errorf("%s: no pinned answer", c.name)
			continue
		}
		t.Run(c.name, func(t *testing.T) {
			cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			if got := pwRun(cctx, engine, c, k); got != pin.want {
				t.Errorf("this engine answered\n  %s\nwant\n  %s", got, pin.want)
				if pin.pg == "" {
					t.Errorf("(PostgreSQL 17.11 answers the want)")
				}
			}
			if pg == nil {
				return
			}
			wantPG := pin.want
			if pin.pg != "" {
				wantPG = pin.pg
			}
			if got := pwRun(cctx, *pg, c, k); got != wantPG {
				t.Errorf("PostgreSQL answered\n  %s\npinned\n  %s", got, wantPG)
			}
		})
	}
	for name := range pins {
		if !seen[name] {
			t.Errorf("pin %q names no cell", name)
		}
	}
}

// pwCells is the coverage table.
func pwCells() []pwCell {
	var cs []pwCell
	add := func(name, sql string, ps ...pwParam) {
		cs = append(cs, pwCell{name: name, sql: sql, ps: ps})
	}
	q := "SELECT id FROM p WHERE %s ORDER BY id"

	// `col = $1` for every declared OID against a column of its own type
	// (and the cross-type pairs a driver sends), text and binary.
	for _, c := range []struct {
		col  string
		oid  uint32
		text string
		bin  any
	}{
		{"n", 20, "7", int64(7)}, {"n", 23, "7", int32(7)}, {"n", 21, "7", int16(7)},
		{"b8", 20, "3000000000", int64(3000000000)}, {"b8", 23, "7", int32(7)},
		{"f", 701, "7.5", 7.5}, {"f", 700, "2.5", float32(2.5)}, {"f", 1700, "7.5", pwNum(75, -1)},
		{"r", 700, "1.5", float32(1.5)}, {"r", 701, "0.25", 0.25},
		{"num", 1700, "7.25", pwNum(725, -2)}, {"num", 701, "7.25", 7.25}, {"num", 20, "3", int64(3)},
		{"n", 701, "7", 7.0}, {"n", 1700, "7", pwNum(7, 0)},
		{"s", 25, "abc", "abc"}, {"s", 1043, "abc", "abc"}, {"t", 25, "a b", "a b"}, {"t", 1043, "a b", "a b"},
		{"d", 1082, "2024-03-04", pwTS("2024-03-04 00:00:00")},
		{"ts", 1114, "2024-03-04 12:00:00", pwTS("2024-03-04 12:00:00")},
		{"ts", 1184, "2024-03-04 12:00:00", pwTS("2024-03-04 12:00:00")},
		{"ts", 1082, "2024-03-04", pwTS("2024-03-04 00:00:00")},
		{"bo", 16, "t", true},
		{"u", 2950, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"},
		{"bt", 17, `\x6869`, []byte("hi")},
		{"n", 25, "7", "7"},
		{"d", 25, "2024-03-04", "2024-03-04"},
	} {
		add(fmt.Sprintf("eq/%s/%d/text", c.col, c.oid), fmt.Sprintf(q, c.col+" = $1"), pwO(c.oid, c.text))
		add(fmt.Sprintf("eq/%s/%d/bin", c.col, c.oid), fmt.Sprintf(q, c.col+" = $1"), pwB(c.oid, c.bin))
	}
	add("eq/bt/17/bin/backslash", fmt.Sprintf(q, "bt = $1"), pwB(17, []byte("\x00\xff\\")))
	add("eq/bt/17/text/backslash", fmt.Sprintf(q, "bt = $1"), pwO(17, `\x00ff5c`))
	// timestamptz with an offset names an instant (TimeZone is UTC on both).
	add("eq/ts/1184/text/offset", fmt.Sprintf(q, "ts = $1"), pwO(1184, "2024-03-04 17:00:00+05"))
	add("eq/ts/1184/text/zulu", fmt.Sprintf(q, "ts = $1"), pwO(1184, "2024-03-04T12:00:00Z"))
	add("eq/ts/1184/text/halfhour", fmt.Sprintf(q, "ts = $1"), pwO(1184, "2024-03-04 06:30:00-05:30"))
	add("eq/ts/1114/text/offset", fmt.Sprintf(q, "ts = $1"), pwO(1114, "2024-03-04 12:00:00+05"))

	// #1426: a timestamp parameter against a DATE column — midnight, not
	// midnight, the epoch's edges, the calendar's ends — in every position the
	// issue measured, declared text and binary, declared timestamptz, and
	// undeclared (a DATE then: the column's type).
	for _, v := range []string{"2024-03-04 00:00:00", "2024-03-04 12:00:00", "1969-12-31 23:59:59.999",
		"1969-12-31 00:00:00.001", "1969-12-30 00:00:00.001", "1000-01-01 00:00:00", "9999-12-31 00:00:00", "9999-12-31 23:59:59"} {
		for _, op := range []string{"d = $1", "d < $1", "$1 = d", "d IN ($1, $2)", "d NOT IN ($1, $2)",
			"d IN (SELECT $1)", "d NOT IN (SELECT $1)", "d = ANY (SELECT $1 UNION ALL SELECT $2)"} {
			two := strings.Contains(op, "$2")
			for _, form := range []string{"text", "bin", "tz", "unk"} {
				var p, p2 pwParam
				switch form {
				case "text":
					p, p2 = pwO(1114, v), pwO(1114, "2000-01-02 00:00:00")
				case "bin":
					p, p2 = pwB(1114, pwTS(v)), pwB(1114, pwTS("2000-01-02 00:00:00"))
				case "tz":
					p, p2 = pwO(1184, v), pwO(1184, "2000-01-02 00:00:00")
				case "unk":
					p, p2 = pwT(v), pwT("2000-01-02 00:00:00")
				}
				ps := []pwParam{p}
				if two {
					ps = append(ps, p2)
				}
				add(fmt.Sprintf("date/%s/%s/%s", op, v, form), fmt.Sprintf(q, op), ps...)
			}
		}
	}

	// `SELECT $1`: the declared type is the column's, and a NULL of each.
	nulled := map[uint32]bool{}
	for _, c := range []struct {
		oid  uint32
		text string
		bin  any
	}{{0, "7", nil}, {20, "7", int64(7)}, {20, "-7", int64(-7)}, {23, "7", int32(7)}, {21, "7", int16(7)},
		{700, "1.5", float32(1.5)}, {701, "2.5", 2.5}, {1700, "2.50", pwNum(250, -2)}, {1700, "7", pwNum(7, 0)},
		{1700, "-7", pwNum(-7, 0)}, {1700, "12345678901234567890123.5", pwNum(123456789012345678, 0)},
		{25, "abc", "abc"}, {1043, "abc", "abc"},
		{1082, "2024-03-04", pwTS("2024-03-04 00:00:00")}, {1114, "2024-03-04 12:00:00.5", pwTS("2024-03-04 12:00:00.5")},
		{1184, "2024-03-04 12:00:00+00", nil}, {16, "true", true},
		{2950, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"}, {17, `\x6869`, []byte("hi")},
		{1007, "{1,2}", []int32{1, 2}}, {1009, "{a,b}", []string{"a", "b"}}} {
		add(fmt.Sprintf("select/%d/text/%s", c.oid, c.text), "SELECT $1", pwO(c.oid, c.text))
		if c.bin != nil {
			add(fmt.Sprintf("select/%d/bin/%s", c.oid, c.text), "SELECT $1", pwB(c.oid, c.bin))
		}
		if !nulled[c.oid] {
			nulled[c.oid] = true
			add(fmt.Sprintf("select-null/%d", c.oid), "SELECT $1 IS NULL, $1", pwParam{oid: c.oid})
		}
	}
	add("select-null/where/0", "SELECT count(*) FROM p WHERE $1 IS NULL", pwParam{})

	// The undeclared parameter takes its position's type (#1410).
	add("infer/$1=n+f", fmt.Sprintf(q, "$1 = n + f"), pwT("14.5"))
	add("infer/$1=n+num", fmt.Sprintf(q, "$1 = n + num"), pwT("14.5"))
	add("infer/n+f=$1", fmt.Sprintf(q, "n + f = $1"), pwT("14.5"))
	add("infer/$1=n+f/f8", fmt.Sprintf(q, "$1 = n + f"), pwB(701, 14.5))
	add("infer/$1=n", fmt.Sprintf(q, "$1 = n"), pwT("7"))
	add("infer/n=$1/fraction", fmt.Sprintf(q, "n = $1"), pwT("7.0"))
	add("infer/$1 IN (1,2)", "SELECT $1 IN (1, 2)", pwT("2"))
	add("infer/n IN ($1,$2)", fmt.Sprintf(q, "n IN ($1, $2)"), pwT("7"), pwT("2"))
	add("infer/n IN (SELECT $1)", fmt.Sprintf(q, "n IN (SELECT $1)"), pwT("7"))
	add("infer/n = (SELECT $1)", fmt.Sprintf(q, "n = (SELECT $1)"), pwT("7"))
	add("infer/n = ANY (SELECT $1 UNION ALL SELECT $2)", fmt.Sprintf(q, "n = ANY (SELECT $1 UNION ALL SELECT $2)"), pwT("7"), pwT("2"))
	add("infer/SELECT $1+1", "SELECT $1 + 1", pwT("2"))
	add("infer/SELECT $1+1.5", "SELECT $1 + 1.5", pwT("2"))
	add("infer/SELECT $1||x", "SELECT $1 || 'x'", pwT("2"))
	add("infer/SELECT $1 bool text", "SELECT $1", pwT("true"))
	add("infer/LIMIT", "SELECT id FROM p ORDER BY id LIMIT $1", pwT("2"))
	add("infer/OFFSET", "SELECT id FROM p ORDER BY id OFFSET $1", pwT("3"))
	add("infer/LIMIT OFFSET", "SELECT id FROM p ORDER BY id LIMIT $1 OFFSET $2", pwT("2"), pwT("1"))
	add("infer/FETCH", "SELECT id FROM p ORDER BY id FETCH FIRST $1 ROWS ONLY", pwT("2"))
	add("infer/LIMIT int8", "SELECT id FROM p ORDER BY id LIMIT $1", pwB(20, int64(2)))
	add("infer/LIMIT int4", "SELECT id FROM p ORDER BY id LIMIT $1", pwB(23, int32(2)))
	add("infer/LIMIT int8 text", "SELECT id FROM p ORDER BY id LIMIT $1", pwO(20, "2"))
	add("infer/LAG", "SELECT id, LAG(n, $1) OVER (ORDER BY id) FROM p ORDER BY id", pwT("1"))
	add("infer/LAG int4 null", "SELECT id, LAG(n, $1) OVER (ORDER BY id) FROM p ORDER BY id", pwParam{oid: 23})
	add("infer/LAG int8", "SELECT id, LAG(n, $1) OVER (ORDER BY id) FROM p ORDER BY id", pwB(20, int64(1)))
	add("infer/LAG numeric", "SELECT id, LAG(n, $1) OVER (ORDER BY id) FROM p ORDER BY id", pwO(1700, "1"))
	add("infer/LEAD default", "SELECT id, LEAD(n, 1, $1) OVER (ORDER BY id) FROM p ORDER BY id", pwT("0"))
	add("infer/NTILE", "SELECT id, NTILE($1) OVER (ORDER BY id) FROM p ORDER BY id", pwT("2"))
	add("infer/CAST int", "SELECT CAST($1 AS INTEGER)", pwT("7"))
	add("infer/CAST ts", "SELECT CAST($1 AS TIMESTAMP)", pwT("2024-03-04 12:00:00"))
	add("infer/CAST varchar", "SELECT CAST($1 AS VARCHAR)", pwT("abc"))
	add("infer/scalar sub", "SELECT id, (SELECT $1) FROM p ORDER BY id LIMIT 1", pwT("7"))
	add("infer/scalar sub cmp", fmt.Sprintf(q, "n < (SELECT $1 + 0)"), pwT("7"))
	add("infer/scalar sub from", fmt.Sprintf(q, "n = (SELECT k FROM q WHERE v = $1)"), pwT("1.5"))
	add("infer/correlated", fmt.Sprintf(q, "EXISTS (SELECT 1 FROM q WHERE q.k = p.n AND q.v > $1)"), pwT("2"))
	add("infer/d=$1 timestamp text", fmt.Sprintf(q, "d = $1"), pwT("2024-03-04 12:00:00"))
	add("infer/f=$1", fmt.Sprintf(q, "f = $1"), pwT("7.5"))
	add("infer/n BETWEEN", fmt.Sprintf(q, "n BETWEEN $1 AND $2"), pwT("3"), pwT("7"))
	add("infer/CASE WHEN", "SELECT CASE WHEN $1 THEN 1 ELSE 0 END", pwT("true"))
	add("infer/CASE result", "SELECT id, CASE WHEN id = 1 THEN $1 ELSE f END FROM p ORDER BY id", pwT("9.5"))
	add("infer/COALESCE", fmt.Sprintf(q, "COALESCE(n, $1) = 0"), pwT("0"))
	add("infer/two unknown", "SELECT $1 = $2", pwT("1"), pwT("1"))
	add("infer/LIKE", fmt.Sprintf(q, "s LIKE $1"), pwT("a%"))
	add("infer/qualified", "SELECT x.id FROM p x WHERE x.n = $1", pwT("7"))
	add("infer/quoted", `SELECT id FROM p WHERE "n" = $1`, pwT("7"))
	add("infer/join", "SELECT a.id FROM p a JOIN p b ON a.id = b.id WHERE b.f = $1", pwT("7.5"))
	add("infer/join on", "SELECT a.id FROM p a JOIN q ON q.k = a.n AND q.v = $1", pwT("1.5"))
	add("infer/derived", "SELECT id FROM (SELECT id, n * 2 AS m FROM p) z WHERE m = $1", pwT("14"))
	add("infer/cte", "WITH c AS (SELECT id, f FROM p) SELECT id FROM c WHERE f = $1", pwT("7.5"))
	add("infer/having", "SELECT n FROM p GROUP BY n HAVING SUM(f) > $1 ORDER BY n", pwT("2.5"))
	add("infer/abs", fmt.Sprintf(q, "abs(n) = $1"), pwT("7"))
	add("infer/lower", fmt.Sprintf(q, "lower(s) = $1"), pwT("abc"))
	add("infer/order by", "SELECT id FROM p WHERE n > $1 ORDER BY id", pwT("5"))

	// A typed parameter in the expression positions around it: table
	// function arguments, a VALUES list, temporal functions and arithmetic,
	// a TABLESAMPLE percentage, LAG / LEAD's default.
	add("consumer/generate_series/int8", "SELECT * FROM generate_series(1, $1) ORDER BY 1", pwB(20, int64(3)))
	add("consumer/generate_series/oid0", "SELECT * FROM generate_series(1, $1) ORDER BY 1", pwT("3"))
	add("consumer/values/1114", "SELECT x FROM (VALUES ($1), ($2)) v(x) ORDER BY 1",
		pwO(1114, "2024-03-04 12:00:00"), pwO(1114, "1969-12-31 23:59:59.999"))
	add("consumer/values/1082", "SELECT x FROM (VALUES ($1), ($2)) v(x) ORDER BY 1",
		pwB(1082, pwTS("2024-03-04 00:00:00")), pwO(1082, "1000-01-01"))
	add("consumer/values/oid0", "SELECT x FROM (VALUES ($1), ($2)) v(x) ORDER BY 1", pwT("b"), pwT("a"))
	add("consumer/date_trunc/1114", "SELECT date_trunc('day', $1)", pwO(1114, "2024-03-04 12:34:56"))
	add("consumer/interval/1114", "SELECT $1 + INTERVAL '1 day'", pwO(1114, "2024-03-04 12:00:00"))
	add("consumer/interval/1082", "SELECT $1 + INTERVAL '1 day'", pwO(1082, "2024-03-04"))
	add("consumer/extract/1082", "SELECT extract(year FROM $1)", pwO(1082, "2024-03-04"))
	add("consumer/between/1114", fmt.Sprintf(q, "ts BETWEEN $1 AND $2"), pwO(1114, "2024-03-04 00:00:00"), pwO(1114, "2024-03-04 23:59:59"))
	add("consumer/between/d/1114", fmt.Sprintf(q, "d BETWEEN $1 AND $2"), pwO(1114, "2024-03-04 00:00:01"), pwO(1114, "2024-03-05 00:00:00"))
	add("consumer/coalesce/1082", "SELECT COALESCE($1, d) FROM p WHERE id = 1", pwO(1082, "2000-01-01"))
	add("consumer/case/bool", "SELECT CASE WHEN $1 THEN 'a' ELSE 'b' END", pwB(16, true))
	add("consumer/upper/25", "SELECT upper($1)", pwO(25, "abc"))
	add("consumer/length/1043", "SELECT length($1)", pwO(1043, "abc"))
	add("consumer/uuid IN", fmt.Sprintf(q, "u IN ($1, $2)"), pwO(2950, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"), pwB(2950, [16]byte{0xb0, 0xee, 0xbc, 0x99, 0x9c, 0x0b, 0x4e, 0xf8, 0xbb, 0x6d, 0x6b, 0xb9, 0xbd, 0x38, 0x0a, 0x11}))
	add("consumer/b8 > int8", fmt.Sprintf(q, "b8 > $1"), pwO(20, "7"))
	add("consumer/limit numeric", "SELECT id FROM p ORDER BY id LIMIT $1", pwO(1700, "2"))
	add("consumer/tablesample/oid0", "SELECT count(*) FROM p TABLESAMPLE BERNOULLI ($1)", pwT("100"))
	add("consumer/tablesample/float4", "SELECT count(*) FROM p TABLESAMPLE BERNOULLI ($1)", pwB(700, float32(100)))
	add("consumer/lead default/1082", "SELECT id, LEAD(d, 1, $1) OVER (ORDER BY id) FROM p ORDER BY id", pwO(1082, "2000-01-02"))
	add("consumer/lead default/int8", "SELECT id, LEAD(b8, 1, $1) OVER (ORDER BY id) FROM p ORDER BY id", pwB(20, int64(-1)))
	add("consumer/pg cast", "SELECT $1::date", pwT("2024-03-04"))
	add("consumer/cte", "WITH c AS (SELECT $1 AS v) SELECT id FROM p, c WHERE p.ts = c.v ORDER BY id", pwO(1114, "2024-03-04 12:00:00"))

	// The stored value of each typed position (gate 2: read back through the
	// embedded engine): INSERT … VALUES, INSERT … SELECT, UPDATE … SET.
	dml := func(name, typ, stmt string, p pwParam) {
		tbl := fmt.Sprintf("w%d", len(cs))
		setup := []string{fmt.Sprintf("CREATE TABLE %s (k INTEGER, c %s)", tbl, typ)}
		if strings.HasPrefix(stmt, "UPDATE") || strings.HasPrefix(stmt, "DELETE") || strings.HasPrefix(stmt, "MERGE") {
			setup = append(setup, fmt.Sprintf("INSERT INTO %s VALUES (1, NULL)", tbl))
		}
		cs = append(cs, pwCell{name: name, setup: setup, sql: fmt.Sprintf(stmt, tbl), ps: []pwParam{p},
			read: fmt.Sprintf("SELECT c FROM %s ORDER BY k", tbl)})
	}
	for _, c := range []struct {
		typ string
		ps  []pwParam
	}{
		{"INTEGER", []pwParam{pwT("2"), pwT("2.5"), pwB(701, 2.5), pwO(701, "2.5"), pwB(1700, pwNum(25, -1)),
			pwB(20, int64(3000000000)), pwO(25, "2"), pwB(23, int32(-7)), {oid: 23}}},
		{"NUMERIC(10,2)", []pwParam{pwT("2.345"), pwB(701, 2.345), pwB(20, int64(2)), pwO(1700, "7"), {oid: 1700}}},
		{"DOUBLE PRECISION", []pwParam{pwT("2.5"), pwB(1700, pwNum(25, -1)), pwB(20, int64(2))}},
		{"DATE", []pwParam{pwT("2024-03-04"), pwO(1114, "2024-03-04 12:00:00"), pwB(1114, pwTS("2024-03-04 12:00:00")),
			pwB(1082, pwTS("1969-12-31 00:00:00")), {oid: 1082}}},
		{"TIMESTAMP", []pwParam{pwT("2024-03-04 12:00:00.5"), pwO(1082, "2024-03-04"), pwB(1082, pwTS("2024-03-04 00:00:00")),
			pwO(1184, "2024-03-04 12:00:00+02"), pwB(1184, pwTS("1969-12-31 23:59:59.999"))}},
		{"VARCHAR", []pwParam{pwT("abc"), pwO(23, "7"), pwB(23, int32(7)), pwO(701, "2.5")}},
		{"BOOLEAN", []pwParam{pwT("t"), pwB(16, false)}},
		{"UUID", []pwParam{pwT("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"), pwO(2950, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")}},
	} {
		for i, p := range c.ps {
			suffix := fmt.Sprintf("%s/%d/oid%d", c.typ, i, p.oid)
			dml("insert/"+suffix, c.typ, "INSERT INTO %s VALUES (1, $1)", p)
			dml("update/"+suffix, c.typ, "UPDATE %s SET c = $1", p)
		}
	}
	dml("insert-select/INTEGER/oid0", "INTEGER", "INSERT INTO %s SELECT 1, $1", pwT("2"))
	dml("insert-cols/DATE/oid0", "DATE", "INSERT INTO %s (c, k) VALUES ($1, 1)", pwT("2024-03-04"))
	dml("update-expr/INTEGER/oid0", "INTEGER", "UPDATE %s SET c = COALESCE(c, 0) + $1", pwT("2"))
	dml("update-where/INTEGER/oid0", "INTEGER", "UPDATE %s SET c = 5 WHERE k = $1", pwT("1"))
	dml("update-where/DATE/1114", "DATE", "UPDATE %s SET c = DATE '2000-01-01' WHERE k = 1 AND $1 > DATE '2024-03-04'", pwO(1114, "2024-03-04 12:00:00"))
	for _, c := range []struct {
		typ string
		p   pwParam
	}{{"INTEGER", pwT("2")}, {"DATE", pwT("2024-03-04")}, {"DATE", pwO(1114, "2024-03-04 12:00:00")}, {"DOUBLE PRECISION", pwT("2.5")}} {
		dml(fmt.Sprintf("merge-update/%s/oid%d", c.typ, c.p.oid), c.typ,
			"MERGE INTO %s t USING (SELECT 1 AS k) s ON t.k = s.k WHEN MATCHED THEN UPDATE SET c = $1", c.p)
		dml(fmt.Sprintf("merge-insert/%s/oid%d", c.typ, c.p.oid), c.typ,
			"MERGE INTO %s t USING (SELECT 2 AS k) s ON t.k = s.k WHEN NOT MATCHED THEN INSERT (k, c) VALUES (s.k, $1)", c.p)
	}
	dml("delete-where/INTEGER/oid0", "INTEGER", "DELETE FROM %s WHERE k = $1", pwT("1"))
	return cs
}

// TestArcPWParameterDescriptionOfOtherStatements: an EXPLAIN's parameters are
// typed as its statement's are (PostgreSQL 17.11 answers [23] for this one).
func TestArcPWParameterDescriptionOfOtherStatements(t *testing.T) {
	ctx := context.Background()
	engine := pwEngine(t, ctx)
	for _, c := range []struct {
		sql  string
		want string
	}{
		{"EXPLAIN SELECT id FROM p WHERE n = $1", "[23]"},
		{"EXPLAIN SELECT id FROM p WHERE $1 = n + f", "[701]"},
	} {
		sd, err := engine.conn.Prepare(ctx, "", c.sql, nil)
		if err != nil {
			t.Fatalf("%s: %v", c.sql, err)
		}
		if got := fmt.Sprint(sd.ParamOIDs); got != c.want {
			t.Errorf("%s: ParameterDescription %s, want %s", c.sql, got, c.want)
		}
	}
}

// TestArcPWNegativeIntegerOutputResidual pins the one residual of the typed
// stand-in (parameters-pgwire r12): a statement's Describe stands an int4 NULL
// in for an integer parameter and declares integer, while a NEGATIVE integer
// literal declares bigint in this engine, so `SELECT $1` declared int4 and
// bound -7 executes as bigint — a client that decodes binary rows by the
// statement's Describe reads eight bytes where it was told four, and refuses
// them. (A non-negative value, and every other position — a comparison, an
// INSERT, arithmetic — agree.) When this starts agreeing, delete the pin.
func TestArcPWNegativeIntegerOutputResidual(t *testing.T) {
	ctx := context.Background()
	engine := pwEngine(t, ctx)
	sd, err := engine.conn.Prepare(ctx, "neg", "SELECT $1", []uint32{oidInt4})
	if err != nil {
		t.Fatal(err)
	}
	if len(sd.Fields) != 1 || sd.Fields[0].DataTypeOID != oidInt4 {
		t.Fatalf("statement Describe declared %v, want integer", sd.Fields)
	}
	for _, c := range []struct {
		val  string
		size int
	}{{"7", 4}, {"-7", 8}} {
		r := engine.conn.ExecPrepared(ctx, "neg", [][]byte{[]byte(c.val)}, nil, []int16{1}).Read()
		if r.Err != nil {
			t.Fatal(r.Err)
		}
		if len(r.Rows) != 1 || len(r.Rows[0][0]) != c.size {
			t.Errorf("%s: binary value %x, want %d bytes", c.val, r.Rows, c.size)
		}
	}
}

// TestArcPWPgxClientsBindAsPostgres: #1410's cells as pgx sends them — Go
// values, pgx choosing each parameter's encoding by the ParameterDescription
// (QueryExecModeCacheStatement, its default) or by the Go type
// (QueryExecModeExec). Pinned as PostgreSQL 17.11 answers them; re-measured
// with WADJET_PG_DSN.
func TestArcPWPgxClientsBindAsPostgres(t *testing.T) {
	ctx := context.Background()
	dsn := os.Getenv("WADJET_PG_DSN")
	for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeCacheStatement, pgx.QueryExecModeExec} {
		t.Run(mode.String(), func(t *testing.T) {
			engine := pwEngine(t, ctx)
			wc := pwPgx(t, ctx, engine.addr, mode)
			var pc *pgx.Conn
			if dsn != "" {
				pwPostgres(t, ctx, dsn)
				cfg, err := pgx.ParseConfig(dsn)
				if err != nil {
					t.Fatal(err)
				}
				cfg.DefaultQueryExecMode = mode
				cfg.RuntimeParams["search_path"] = "pw"
				if pc, err = pgx.ConnectConfig(ctx, cfg); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { pc.Close(context.Background()) })
			}
			for k, c := range pwPgxCells() {
				t.Run(c.name, func(t *testing.T) {
					want := c.want
					if mode == pgx.QueryExecModeExec && c.wantExec != "" {
						want = c.wantExec
					}
					if got := pwPgxRun(ctx, wc, engine.db, c, fmt.Sprintf("x%d_%d", mode, k)); got != want {
						t.Errorf("this engine answered\n  %s\nwant\n  %s", got, want)
					}
					if pc == nil {
						return
					}
					wantPG := want
					if c.pg != "" {
						wantPG = c.pg
					}
					if got := pwPgxRun(ctx, pc, nil, c, fmt.Sprintf("x%d_%d", mode, k)); got != wantPG {
						t.Errorf("PostgreSQL answered\n  %s\npinned\n  %s", got, wantPG)
					}
				})
			}
		})
	}
}

// pwPgx opens a pgx connection to this engine's server at addr.
func pwPgx(t *testing.T, ctx context.Context, addr string, mode pgx.QueryExecMode) *pgx.Conn {
	t.Helper()
	cfg, err := pgx.ParseConfig("postgres://wadjet:wadjet@" + addr + "/wadjet?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	cfg.DefaultQueryExecMode = mode
	c, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close(context.Background()) })
	return c
}

type pwPgxCell struct {
	name     string
	row      string // the catalog row recording a difference
	setup    string
	sql      string
	args     []any
	read     string
	want     string // this engine's answer under QueryExecModeCacheStatement
	wantExec string // … under QueryExecModeExec, where it differs
	pg       string // PostgreSQL 17.11's, where it differs (a recorded divergence)
}

func pwPgxRun(ctx context.Context, conn *pgx.Conn, db *wadjet.DB, c pwPgxCell, tbl string) string {
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if c.setup != "" {
		if _, err := conn.Exec(cctx, pwDialect(db != nil, strings.ReplaceAll(c.setup, "TBL", tbl))); err != nil {
			return "SETUP " + pwErr(err)
		}
	}
	rows, err := conn.Query(cctx, strings.ReplaceAll(c.sql, "TBL", tbl), c.args...)
	if err != nil {
		return pwErr(err)
	}
	var oids, out []string
	for _, f := range rows.FieldDescriptions() {
		oids = append(oids, fmt.Sprint(f.DataTypeOID))
	}
	for rows.Next() {
		vs, err := rows.Values()
		if err != nil {
			rows.Close()
			return "SCAN " + err.Error()
		}
		var s []string
		for _, v := range vs {
			s = append(s, pwValue(v))
		}
		out = append(out, strings.Join(s, "|"))
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return pwErr(err)
	}
	res := "fields=" + strings.Join(oids, ",") + " rows=[" + strings.Join(out, " ; ") + "]"
	if c.read != "" {
		read := strings.ReplaceAll(c.read, "TBL", tbl)
		var got []string
		if db != nil {
			r, err := db.Query(cctx, read)
			if err != nil {
				return res + " READ " + pwErr(err)
			}
			for _, row := range r.Rows {
				got = append(got, pwStored(row[r.Columns[0]], r.ColumnMetas[0].TypeID))
			}
		} else {
			r2, err := conn.Query(cctx, read)
			if err != nil {
				return res + " READ " + pwErr(err)
			}
			for r2.Next() {
				vs, _ := r2.Values()
				got = append(got, pwValue(vs[0]))
			}
			r2.Close()
		}
		res += " stored=[" + strings.Join(got, " ; ") + "]"
	}
	return res
}

// pwPgxCells are #1410's four cells and the client shapes around them.
func pwPgxCells() []pwPgxCell {
	ts := pwTS
	return pwPgxPinned([]pwPgxCell{
		{name: "1410/$1 = n + f float64", sql: "SELECT id FROM p WHERE $1 = n + f ORDER BY id", args: []any{14.5}},
		{name: "1410/$1 = n + num float64", sql: "SELECT id FROM p WHERE $1 = n + num ORDER BY id", args: []any{14.5}},
		{name: "1410/insert 2.5 into integer", setup: "CREATE TABLE TBL (k INTEGER, c INTEGER)",
			sql: "INSERT INTO TBL VALUES (2, $1)", args: []any{2.5}, read: "SELECT c FROM TBL"},
		{name: "1410/select int64", sql: "SELECT $1", args: []any{int64(7)}},
		{name: "1410/limit int64", sql: "SELECT id FROM p ORDER BY id LIMIT $1", args: []any{int64(2)}},
		{name: "1410/limit int", sql: "SELECT id FROM p ORDER BY id LIMIT $1", args: []any{2}},
		{name: "1410/offset int", sql: "SELECT id FROM p ORDER BY id OFFSET $1", args: []any{3}},
		{name: "n = int64", sql: "SELECT id FROM p WHERE n = $1 ORDER BY id", args: []any{int64(7)}},
		{name: "n = float64 7.0", sql: "SELECT id FROM p WHERE n = $1 ORDER BY id", args: []any{7.0}},
		{name: "insert 2.0 into integer", setup: "CREATE TABLE TBL (k INTEGER, c INTEGER)",
			sql: "INSERT INTO TBL VALUES (2, $1)", args: []any{2.0}, read: "SELECT c FROM TBL"},
		{name: "insert time into date", setup: "CREATE TABLE TBL (k INTEGER, c DATE)",
			sql: "INSERT INTO TBL VALUES (2, $1)", args: []any{ts("2024-03-04 12:00:00")}, read: "SELECT c FROM TBL"},
		{name: "select float64", sql: "SELECT $1", args: []any{2.5}},
		{name: "select string", sql: "SELECT $1", args: []any{"abc"}},
		{name: "select $1 + 1 int", sql: "SELECT $1 + 1", args: []any{2}},
		{name: "select $1 + 1 negative int", sql: "SELECT $1 + 1", args: []any{-2}},
		{name: "1426/d = time not midnight", sql: "SELECT id FROM p WHERE d = $1 ORDER BY id", args: []any{ts("2024-03-04 12:00:00")}},
		{name: "1426/d = time midnight", sql: "SELECT id FROM p WHERE d = $1 ORDER BY id", args: []any{ts("2024-03-04 00:00:00")}},
		{name: "1426/d < time", sql: "SELECT id FROM p WHERE d < $1 ORDER BY id", args: []any{ts("1969-12-31 00:00:00.001")}},
		{name: "1426/d IN (time, time)", sql: "SELECT id FROM p WHERE d IN ($1, $2) ORDER BY id",
			args: []any{ts("1969-12-30 00:00:00.001"), ts("2024-03-04 12:00:00")}},
		{name: "ts = time", sql: "SELECT id FROM p WHERE ts = $1 ORDER BY id", args: []any{ts("2024-03-04 12:00:00")}},
		{name: "lag int", sql: "SELECT id, LAG(n, $1) OVER (ORDER BY id) FROM p ORDER BY id", args: []any{1}},
		{name: "s = string", sql: "SELECT id FROM p WHERE s = $1 ORDER BY id", args: []any{"abc"}},
		{name: "num = float64", sql: "SELECT id FROM p WHERE num = $1 ORDER BY id", args: []any{7.25}},
		{name: "f = int", sql: "SELECT id FROM p WHERE f = $1 ORDER BY id", args: []any{3}},
		{name: "u = string", sql: "SELECT id FROM p WHERE u = $1 ORDER BY id", args: []any{"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"}},
		{name: "bo = bool", sql: "SELECT id FROM p WHERE bo = $1 ORDER BY id", args: []any{true}},
		{name: "n IN (SELECT $1)", sql: "SELECT id FROM p WHERE n IN (SELECT $1) ORDER BY id", args: []any{7}},
	})
}

type pwPin struct {
	want string // this engine's answer
	pg   string // PostgreSQL 17.11's, where it differs
	row  string // the catalog row recording the difference
}
