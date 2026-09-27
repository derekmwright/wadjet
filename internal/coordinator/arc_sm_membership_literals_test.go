// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// smNumTable is the membership table's second fixture, `sm_num`: the
// DECIMAL declarations st_pair does not hold — NUMERIC(38,10) and a NUMERIC
// with no modifier (stored here as NUMERIC(38,0), which is what this
// engine's DDL makes of it; its values are integers, so the stored values
// are PostgreSQL's) — and a float8 beside a PORT and a PROTOCOL whose values
// coincide with it on rows 1..3 (80 / 6 / 65535 against 80 / 443 / 65535
// and 6 / 17 / 255). Row 4 is NULL in every column.
func smNumTable() tmdTable {
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "v_n38", Type: parquet.TypeDecimal, Precision: 38, Scale: 10, Nullable: true},
		{Name: "v_n", Type: parquet.TypeDecimal, Precision: 38, Scale: 0, Nullable: true},
		{Name: "v_f64", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "v_port", Type: parquet.TypePort, Nullable: true},
		{Name: "v_proto", Type: parquet.TypeProtocol, Nullable: true},
	}}
	rows := []map[string]any{
		{"id": int64(1), "v_n38": 12.5, "v_n": 12.0, "v_f64": 80.0, "v_port": int32(80), "v_proto": int32(6)},
		{"id": int64(2), "v_n38": 13.25, "v_n": 13.0, "v_f64": 6.0, "v_port": int32(443), "v_proto": int32(17)},
		{"id": int64(3), "v_n38": 14.0, "v_n": 14.0, "v_f64": 65535.0, "v_port": int32(65535), "v_proto": int32(255)},
		{"id": int64(4), "v_n38": nil, "v_n": nil, "v_f64": nil, "v_port": nil, "v_proto": nil},
	}
	return tmdTable{name: "sm_num", schema: schema, rows: rows}
}

// smNumPGFixture is sm_num as PostgreSQL DDL + INSERT (PORT and PROTOCOL
// are integer there, the unmodified NUMERIC is numeric).
func smNumPGFixture() string {
	return "DROP TABLE IF EXISTS sm_num;\n" +
		"CREATE TABLE sm_num (id bigint, v_n38 numeric(38,10), v_n numeric, v_f64 double precision, v_port integer, v_proto integer);\n" +
		"INSERT INTO sm_num VALUES (1, 12.5, 12, 80, 80, 6), (2, 13.25, 13, 6, 443, 17), (3, 14, 14, 65535, 65535, 255), (4, NULL, NULL, NULL, NULL, NULL);\n"
}

// smArms is stArms with sm_num beside st_pair on every arm.
func smArms(t *testing.T, ctx context.Context) []brArm {
	t.Helper()
	standalone := func(budget int64) *wadjet.DB {
		db := stStandalone(t, ctx, budget)
		tbl := smNumTable()
		if err := db.CreateTable(ctx, tbl.name, tbl.schema, nil); err != nil {
			t.Fatalf("create %s: %v", tbl.name, err)
		}
		ing := db.NewIngester(tbl.name, tbl.schema, nil, ingest.Config{MaxBufferRows: len(tbl.rows) + 1, RowGroupSize: 2})
		if err := ing.Ingest(ctx, tbl.rows); err != nil {
			t.Fatalf("ingest %s: %v", tbl.name, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", tbl.name, err)
		}
		return db
	}
	single, spilled := standalone(0), standalone(512*1024)
	stand := func(opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, []tmdTable{stTable(), smNumTable()})
		return tmdCoordinator(t, ctx, infra, opts...)
	}
	coord := stand()
	coordB := stand(func(c *Config) { c.BroadcastBytesOverride = 1 })
	infraM := tmdInfra(t, ctx)
	tmdWriteTableList(t, ctx, infraM, nil, []tmdTable{stTable(), smNumTable()})
	coordM := tmdCoordinatorWithWorkers(t, ctx, infraM, func(w *worker.Config) { w.MorselWorkers = 4 })
	runSingle := func(db *wadjet.DB) func(string) (*oracle.Result, error) {
		return func(sql string) (*oracle.Result, error) { return tmdRunSingle(ctx, db, sql) }
	}
	runDAG := func(c *Coordinator) func(string) (*oracle.Result, error) {
		return func(sql string) (*oracle.Result, error) { return tmdRunDAG(ctx, c, sql) }
	}
	return []brArm{
		{"single", runSingle(single)},
		{"spilled512k", runSingle(spilled)},
		{"dag", runDAG(coord)},
		{"dag-shuffled", runDAG(coordB)},
		{"dag-morsel4", runDAG(coordM)},
	}
}

// smLitOp is one membership spelling over a quoted-literal outer value.
type smLitOp struct {
	name string
	pred func(lit, body string) string
}

func smLitOps() []smLitOp {
	return []smLitOp{
		{"in", func(l, b string) string { return "'" + l + "' IN (" + b + ")" }},
		{"notIn", func(l, b string) string { return "'" + l + "' NOT IN (" + b + ")" }},
		{"eqAny", func(l, b string) string { return "'" + l + "' = ANY (" + b + ")" }},
	}
}

// smSpelled is one quoted literal and the tag its cell is named by.
type smSpelled struct{ tag, lit string }

// smLiteralCells is the quoted-literal outer against a typed body, in the
// input function's spellings rather than only the member's own rendering
// (#1372): the literal takes the body's TYPE — never its typmod — and is
// read by that type's input function on every arm, so a spelling the type
// reads as a member's value matches it and one it cannot read is the input
// function's refusal.
//
//   - num/: a NUMERIC body at scale 4 (st_pair.v_dec), at scale 10
//     (sm_num.v_n38) and with no modifier (sm_num.v_n): a literal with more
//     fractional digits than the column's scale is compared as written —
//     rounding it to the column's scale answered every row where
//     PostgreSQL answers none — and one wider than the column's precision is
//     no overflow, since the column's typmod is not the literal's.
//   - lit/: DATE, IPv6, UUID, bigint and PORT bodies against spellings the
//     type's input function reads as a member (`'2024-1-2'`, `'20240102'`,
//     `'2001:DB8::1'`, a braced uuid, `'1_2'`, `'0x0C'`, whitespace, sign,
//     leading zeros) and spellings it refuses. The DAG inlined the set as a
//     list of literals and compared the literal's TEXT with the members'
//     spelling: 0 rows (NOT IN every row) where the single-process arms and
//     PostgreSQL match.
func smLiteralCells() []smCell {
	var out []smCell
	add := func(name, sql string) {
		out = append(out, smCell{name: name, sql: sql, pgSQL: sql})
	}
	nums := []struct {
		key, tbl, col string
		lits          []smSpelled
	}{
		{"dec", "st_pair", "v_dec", []smSpelled{
			{"over", "12.50001"}, {"roundUp", "13.24996"}, {"exact", "12.5"}, {"pad", "12.50"},
			{"wide", "12.5000000"}, {"exp", "1.25e1"}, {"space", " 12.5 "}, {"sign", "+12.5"},
			{"overflow", "123456789012345.12345"},
		}},
		{"n38", "sm_num", "v_n38", []smSpelled{
			{"over", "12.50000000001"}, {"roundUp", "13.249999999996"}, {"exact", "12.5"},
			{"overflow", "123456789012345678901234567890.5"},
		}},
		{"n", "sm_num", "v_n", []smSpelled{
			{"frac", "12.4"}, {"half", "12.5"}, {"exact", "12"}, {"pad", "12.0"}, {"exp", "1.2e1"},
		}},
	}
	for _, n := range nums {
		body := "SELECT r." + n.col + " FROM " + n.tbl + " r WHERE r.id <= 3"
		corr := "SELECT r." + n.col + " FROM " + n.tbl + " r WHERE r.id = a.id"
		for _, l := range n.lits {
			for _, op := range smLitOps() {
				add("num/"+op.name+"/"+n.key+"/"+l.tag, "SELECT a.id FROM "+n.tbl+" a WHERE "+op.pred(l.lit, body))
			}
			add("num/neAll/"+n.key+"/"+l.tag, "SELECT a.id FROM "+n.tbl+" a WHERE '"+l.lit+"' <> ALL ("+body+")")
			add("num/corrIn/"+n.key+"/"+l.tag, "SELECT a.id FROM "+n.tbl+" a WHERE '"+l.lit+"' IN ("+corr+")")
			add("num/corrNotIn/"+n.key+"/"+l.tag, "SELECT a.id FROM "+n.tbl+" a WHERE '"+l.lit+"' NOT IN ("+corr+")")
			add("num/select/"+n.key+"/"+l.tag, "SELECT a.id, '"+l.lit+"' IN ("+body+") AS m FROM "+n.tbl+" a")
		}
	}
	// A body whose own CAST declares a narrower scale: the literal takes
	// NUMERIC, and the members are the CAST's rounded values.
	add("num/in/dec/castScale", "SELECT a.id FROM st_pair a WHERE '13.26' IN (SELECT CAST(r.v_dec AS NUMERIC(10,1)) FROM st_pair r WHERE r.id <= 3)")
	add("num/in/dec/castScaleHit", "SELECT a.id FROM st_pair a WHERE '13.3' IN (SELECT CAST(r.v_dec AS NUMERIC(10,1)) FROM st_pair r WHERE r.id <= 3)")

	spelled := []struct {
		key, col string
		lits     []smSpelled
	}{
		{"date", "v_date", []smSpelled{
			{"short", "2024-1-2"}, {"compact", "20240102"}, {"midnight", "2024-01-02 00:00:00"},
			{"space", " 2024-01-02 "}, {"canon", "2024-01-02"},
			{"badMonth", "2024-13-02"}, {"junk", "2024-01-02x"},
		}},
		{"ipv6", "v_ipv6", []smSpelled{
			{"upper", "2001:DB8::1"}, {"long", "2001:db8:0:0::1"},
			{"full", "2001:0db8:0000:0000:0000:0000:0000:0001"}, {"host", "2001:db8::1/128"},
			{"canon", "2001:db8::1"}, {"bad", "2001:db8::g"},
		}},
		{"uuid", "v_uuid", []smSpelled{
			{"braced", "{00000000-0000-4000-8000-000000000001}"}, {"bare", "00000000000040008000000000000001"},
			{"canon", "00000000-0000-4000-8000-000000000001"}, {"bad", "00000000-0000-4000-8000-00000000000z"},
		}},
		{"i64", "v_i64", []smSpelled{
			{"underscore", "1_2"}, {"hex", "0x0C"}, {"octal", "0o14"}, {"binary", "0b1100"},
			{"space", " 12 "}, {"sign", "+12"}, {"zeros", "012"}, {"canon", "12"},
			{"doubleUnderscore", "1__2"}, {"hexEmpty", "0x"}, {"frac", "12.0"},
		}},
		{"port", "v_port", []smSpelled{
			{"underscore", "8_0"}, {"hex", "0x50"}, {"space", " 80 "}, {"sign", "+80"}, {"zeros", "080"},
			{"canon", "80"}, {"bad", "8o"},
		}},
	}
	for _, s := range spelled {
		body := "SELECT r." + s.col + " FROM st_pair r WHERE r.id <= 3"
		for _, l := range s.lits {
			for _, op := range smLitOps() {
				add("lit/"+op.name+"/"+s.key+"/"+l.tag, "SELECT a.id FROM st_pair a WHERE "+op.pred(l.lit, body))
			}
		}
	}
	// The POSITIONS a membership with a quoted-literal outer can sit in: the
	// literal is typed wherever the plan carries the expression — a derived
	// table's or a CTE's WHERE, HAVING, a body reading a CTE, a parenthesized
	// literal, the SELECT list, a CASE, a scalar subquery's own WHERE, a
	// set-operation arm, a LATERAL body's SELECT list.
	const q = "(SELECT q.v_date FROM st_pair q)"
	for _, c := range [][2]string{
		{"derived", "SELECT d.id FROM (SELECT a.id FROM st_pair a WHERE '2024-1-2' IN " + q + ") d"},
		{"cte", "WITH c AS (SELECT a.id FROM st_pair a WHERE '1_2' IN (SELECT q.v_i64 FROM st_pair q)) SELECT id FROM c"},
		{"having", "SELECT count(*) AS n FROM st_pair a GROUP BY a.v_bool HAVING '2024-1-2' IN " + q},
		{"cteBody", "WITH c AS (SELECT v_date AS k FROM st_pair) SELECT a.id FROM st_pair a WHERE '2024-1-2' IN (SELECT c.k FROM c)"},
		{"paren", "SELECT a.id FROM st_pair a WHERE ('2024-1-2') IN " + q},
		{"select", "SELECT a.id, '2001:DB8::1' IN (SELECT q.v_ipv6 FROM st_pair q) AS m FROM st_pair a"},
		{"case", "SELECT a.id, CASE WHEN '1_2' IN (SELECT q.v_i64 FROM st_pair q) THEN 1 ELSE 0 END AS m FROM st_pair a"},
		{"scalar", "SELECT (SELECT count(*) FROM st_pair r WHERE '0x0C' IN (SELECT q.v_i64 FROM st_pair q)) AS n"},
		{"unionArm", "SELECT a.id FROM st_pair a WHERE '2024-1-2' IN " + q + " UNION ALL SELECT 9"},
		{"lateral", "SELECT a.id, l.m FROM st_pair a, LATERAL (SELECT '1_2' IN (SELECT q.v_i64 FROM st_pair q) AS m) l"},
	} {
		add("lit/pos/"+c[0], c[1])
	}
	return out
}

// smRenderKeptCells are the two `CAST(x AS TEXT)` pairs ADR-0012 §5 keeps
// although their types differ: a float8 against the text of a PORT or a
// PROTOCOL. Every value of either renders as its float8 does (an integer
// in 0..65535), so the membership that converts the text and the EXISTS
// key that compares it directly answer one value — PostgreSQL's
// `CAST(CAST(x AS TEXT) AS float8)` rewrite, which the expected rows are
// (the PostgreSQL spelling compares the float8 with the integer itself).
// Every spelling of the membership and the key over sm_num, whose float8
// column coincides with the port on rows 1 and 3 and with the protocol on
// row 2.
func smRenderKeptCells() []smCell {
	var out []smCell
	for _, origin := range []string{"port", "proto"} {
		cast := "CAST(r.v_" + origin + " AS TEXT)"
		body := func(filter string) string { return "SELECT " + cast + " FROM sm_num r WHERE " + filter }
		sqls := [][2]string{
			{"in", "a.v_f64 IN (" + body("r.id <= 3") + ")"},
			{"notIn", "a.v_f64 NOT IN (" + body("r.id <= 3") + ")"},
			{"notInNull", "a.v_f64 NOT IN (" + body("r.id >= 3") + ")"},
			{"eqAny", "a.v_f64 = ANY (" + body("r.id <= 3") + ")"},
			{"neAll", "a.v_f64 <> ALL (" + body("r.id <= 3") + ")"},
			{"unionAll", "a.v_f64 IN (" + body("r.id = 1") + " UNION ALL " + body("r.id >= 2") + ")"},
			{"union", "a.v_f64 IN (" + body("r.id = 1") + " UNION " + body("r.id = 3") + ")"},
			{"intersect", "a.v_f64 IN (" + body("r.id <= 3") + " INTERSECT " + body("r.id >= 2") + ")"},
			{"except", "a.v_f64 IN (" + body("r.id <= 3") + " EXCEPT " + body("r.id = 1") + ")"},
			{"corrIn", "a.v_f64 IN (" + body("r.id >= a.id") + ")"},
			{"exists", "EXISTS (SELECT 1 FROM sm_num r WHERE r.id <= 3 AND a.v_f64 = " + cast + ")"},
			{"notExists", "NOT EXISTS (SELECT 1 FROM sm_num r WHERE r.id <= 3 AND a.v_f64 = " + cast + ")"},
		}
		for _, s := range sqls {
			sql := "SELECT a.id FROM sm_num a WHERE " + s[1]
			out = append(out, smCell{name: "xcast/" + s[0] + "/f64From" + origin, sql: sql,
				pgSQL: strings.ReplaceAll(sql, cast, "r.v_"+origin)})
		}
	}
	return out
}

// smPGFixtures is both fixtures as PostgreSQL DDL, for re-measuring.
func smPGFixtures() string { return stPGFixture() + smNumPGFixture() }
