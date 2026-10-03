// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

// A FLOAT COLUMN IS DOUBLE PRECISION ON THE WIRE (#1464): the RowDescription
// OID, psql's `\d` column type (format_type, the statement psql 17 sends),
// information_schema's data_type, the stored value read back as text, and a
// float4 / float8 PARAMETER bound into the column, and the parameter type the
// server DESCRIBES for `$1::<spelling>` with the value bound as it — for every
// float spelling a CREATE TABLE sent over pgwire can carry. The want lines are
// PostgreSQL 17.11's, measured by TestArcFTWireMeasure against the same
// statements. At v0.25.3 `c FLOAT` described as real (700, 6.75e+08), the
// other spellings refused the CREATE TABLE, `$1::FLOAT` described its
// parameter as 700 and `$1::FLOAT4`, `REAL`, `FLOAT8` as 0.

var ftWireSpellings = []struct{ key, spell string }{
	{"float", "FLOAT"},
	{"float1", "FLOAT(1)"},
	{"float25", "float(25)"},
	{"float4", "FLOAT4"},
	{"float8", "FLOAT8"},
	{"real", "REAL"},
	{"dp", "DOUBLE PRECISION"},
}

// ftWireWant is PostgreSQL 17.11's answer, line for line.
var ftWireWant = []string{
	"float: select oid=701 rows=[675000000 674999997 674999997]",
	"float: \\d c|double precision|f",
	"float: information_schema double precision",
	"float: CAST($1 AS FLOAT) oid=701 [674999997]",
	"float: PREPARE $1::FLOAT params=[701] oid=701 [674999997]",
	"float1: select oid=700 rows=[6.75e+08 6.75e+08 6.75e+08]",
	"float1: \\d c|real|f",
	"float1: information_schema real",
	"float1: CAST($1 AS FLOAT(1)) oid=700 [6.75e+08]",
	"float1: PREPARE $1::FLOAT(1) params=[700] oid=700 [6.75e+08]",
	"float25: select oid=701 rows=[675000000 674999997 674999997]",
	"float25: \\d c|double precision|f",
	"float25: information_schema double precision",
	"float25: CAST($1 AS float(25)) oid=701 [674999997]",
	"float25: PREPARE $1::float(25) params=[701] oid=701 [674999997]",
	"float4: select oid=700 rows=[6.75e+08 6.75e+08 6.75e+08]",
	"float4: \\d c|real|f",
	"float4: information_schema real",
	"float4: CAST($1 AS FLOAT4) oid=700 [6.75e+08]",
	"float4: PREPARE $1::FLOAT4 params=[700] oid=700 [6.75e+08]",
	"float8: select oid=701 rows=[675000000 674999997 674999997]",
	"float8: \\d c|double precision|f",
	"float8: information_schema double precision",
	"float8: CAST($1 AS FLOAT8) oid=701 [674999997]",
	"float8: PREPARE $1::FLOAT8 params=[701] oid=701 [674999997]",
	"real: select oid=700 rows=[6.75e+08 6.75e+08 6.75e+08]",
	"real: \\d c|real|f",
	"real: information_schema real",
	"real: CAST($1 AS REAL) oid=700 [6.75e+08]",
	"real: PREPARE $1::REAL params=[700] oid=700 [6.75e+08]",
	"dp: select oid=701 rows=[675000000 674999997 674999997]",
	"dp: \\d c|double precision|f",
	"dp: information_schema double precision",
	"dp: CAST($1 AS DOUBLE PRECISION) oid=701 [674999997]",
	"dp: PREPARE $1::DOUBLE PRECISION params=[701] oid=701 [674999997]",
}

// ftWireRun sends every statement over conn and renders what came back.
// Rows are inserted as a literal, a float8 parameter and a float4 parameter
// (the float4 parameter's own value is 6.75e+08 before it reaches the column).
func ftWireRun(t *testing.T, conn *pgconn.PgConn) []string {
	t.Helper()
	ctx := context.Background()
	exec := func(sql string, params [][]byte, oids []uint32) *pgconn.Result {
		return conn.ExecParams(ctx, sql, params, oids, nil, nil).Read()
	}
	must := func(sql string, params [][]byte, oids []uint32) *pgconn.Result {
		r := exec(sql, params, oids)
		if r.Err != nil {
			t.Fatalf("%s\n  -> %v", sql, r.Err)
		}
		return r
	}
	texts := func(r *pgconn.Result) []string {
		out := make([]string, 0, len(r.Rows))
		for _, row := range r.Rows {
			f := make([]string, len(row))
			for i, v := range row {
				f[i] = string(v)
			}
			out = append(out, strings.Join(f, "|"))
		}
		return out
	}
	var out []string
	for _, s := range ftWireSpellings {
		tbl := "ftw_" + s.key
		exec("DROP TABLE IF EXISTS "+tbl, nil, nil)
		r := exec("CREATE TABLE "+tbl+" (c "+s.spell+")", nil, nil)
		if r.Err != nil {
			out = append(out, fmt.Sprintf("%s: CREATE TABLE refused: %v", s.key, r.Err))
			continue
		}
		must("INSERT INTO "+tbl+" VALUES (674999997)", nil, nil)
		must("INSERT INTO "+tbl+" VALUES ($1)", [][]byte{[]byte("674999997")}, []uint32{701})
		must("INSERT INTO "+tbl+" VALUES ($1)", [][]byte{[]byte("674999997")}, []uint32{700})
		sel := must("SELECT c FROM "+tbl+" ORDER BY c DESC", nil, nil)
		out = append(out, fmt.Sprintf("%s: select oid=%d rows=%v", s.key, sel.FieldDescriptions[0].DataTypeOID, texts(sel)))

		lookup := texts(must(fmt.Sprintf(psqlRelationLookup, tbl), nil, nil))
		if len(lookup) != 1 {
			t.Fatalf("\\d %s found %v", tbl, lookup)
		}
		oid := strings.SplitN(lookup[0], "|", 2)[0]
		for _, col := range texts(must(fmt.Sprintf(psqlColumnLookup, oid), nil, nil)) {
			out = append(out, fmt.Sprintf("%s: \\d %s", s.key, col))
		}
		for _, dt := range texts(must("SELECT data_type FROM information_schema.columns WHERE table_name = '"+tbl+"'", nil, nil)) {
			out = append(out, fmt.Sprintf("%s: information_schema %s", s.key, dt))
		}
		cast := must("SELECT CAST($1 AS "+s.spell+")", [][]byte{[]byte("674999997")}, []uint32{701})
		out = append(out, fmt.Sprintf("%s: CAST($1 AS %s) oid=%d %v", s.key, s.spell, cast.FieldDescriptions[0].DataTypeOID, texts(cast)))

		// The SERVER's parameter type: a client that sends no OID reads the
		// ParameterDescription and binds the value as that type (pgx does), so
		// the described OID decides the value. At v0.25.3 `$1::FLOAT` described
		// 700 and a bound 674999997 came back 6.75e+08; FLOAT4/REAL/FLOAT8
		// described 0.
		sd, err := conn.Prepare(ctx, "ftw_"+s.key, "SELECT $1::"+s.spell, nil)
		if err != nil {
			t.Fatalf("PREPARE $1::%s: %v", s.spell, err)
		}
		bound := conn.ExecPrepared(ctx, "ftw_"+s.key, [][]byte{[]byte("674999997")}, nil, nil).Read()
		if bound.Err != nil {
			t.Fatalf("EXECUTE $1::%s: %v", s.spell, bound.Err)
		}
		out = append(out, fmt.Sprintf("%s: PREPARE $1::%s params=%v oid=%d %v", s.key, s.spell, sd.ParamOIDs, bound.FieldDescriptions[0].DataTypeOID, texts(bound)))
		conn.Exec(ctx, "DEALLOCATE ftw_"+s.key).ReadAll()
		exec("DROP TABLE IF EXISTS "+tbl, nil, nil)
	}
	return out
}

func TestArcFTFloatTypeNamesOnTheWire(t *testing.T) {
	db, err := wadjet.Open(context.Background(), wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	srv := startTestServer(t, db)
	got := ftWireRun(t, sec5Pgconn(t, srv.Addr()))
	want := make([]string, len(ftWireWant))
	pinned := 0
	for i, w := range ftWireWant {
		want[i] = w
		if pin := ftWirePinned(w); pin != "" {
			want[i] = pin
			pinned++
		}
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("the wire answers differ from PostgreSQL 17.11's (with the pinned real text)\n got:\n  %s\n want:\n  %s",
			strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
	if pinned != 9 {
		t.Fatalf("%d pinned lines, want 9", pinned)
	}
}

// ftWirePinned is the pinned answer of a PostgreSQL line this server renders
// differently for a reason outside the type-name seam, identically at base
// (measured: a v0.25.3 FLOAT column, then float4, read over psql): a real
// value's TEXT is written at float8's digit threshold, `675000000`, where
// PostgreSQL's float4out writes `6.75e+08` — the value, the OID (700) and
// the declaration agree. The pin FAILS when the text starts agreeing; delete
// it then. "" = no pin.
func ftWirePinned(line string) string {
	if strings.Contains(line, "6.75e+08") {
		return strings.ReplaceAll(line, "6.75e+08", "675000000")
	}
	return ""
}

// TestArcFTWireMeasure prints PostgreSQL's lines for ftWireWant (FT_PG_DSN).
func TestArcFTWireMeasure(t *testing.T) {
	dsn := os.Getenv("FT_PG_DSN")
	if dsn == "" {
		t.Skip("FT_PG_DSN unset")
	}
	conn, err := pgconn.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	conn.ExecParams(context.Background(), "SET statement_timeout = '30s'", nil, nil, nil, nil).Read()
	for _, line := range ftWireRun(t, conn) {
		t.Logf("%q,", line)
	}
}
