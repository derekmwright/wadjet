// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// stType is one typed column of the `st_pair` fixture beside a TEXT column
// holding PostgreSQL 17.11's rendering of the same value on rows 1 and 2, a
// text no value renders as on row 3 ('zz'), and NULL on row 4 (both sides).
type stType struct {
	key   string // the column suffix: v_<key> / s_<key>
	pg    string // the PostgreSQL column type the oracle fixture declares
	name  string // PostgreSQL's name for it in `operator does not exist: <name> = text`
	typ   parquet.TypeID
	prec  int
	scale int
	vals  [3]any    // rows 1..3
	texts [2]string // rows 1..2: the value's own text
}

func stTypes() []stType {
	return []stType{
		{key: "i64", pg: "bigint", name: "bigint", typ: parquet.TypeInt64,
			vals: [3]any{int64(12), int64(13), int64(14)}, texts: [2]string{"12", "13"}},
		{key: "i32", pg: "integer", name: "integer", typ: parquet.TypeInt32,
			vals: [3]any{int32(12), int32(13), int32(14)}, texts: [2]string{"12", "13"}},
		{key: "dec", pg: "numeric(18,4)", name: "numeric", typ: parquet.TypeDecimal, prec: 18, scale: 4,
			vals: [3]any{12.5, 13.25, 14.0}, texts: [2]string{"12.5000", "13.2500"}},
		{key: "f64", pg: "double precision", name: "double precision", typ: parquet.TypeFloat64,
			vals: [3]any{12.5, 0.25, 3.0}, texts: [2]string{"12.5", "0.25"}},
		{key: "port", pg: "integer", name: "integer", typ: parquet.TypePort,
			vals: [3]any{int32(80), int32(443), int32(22)}, texts: [2]string{"80", "443"}},
		{key: "proto", pg: "integer", name: "integer", typ: parquet.TypeProtocol,
			vals: [3]any{int32(6), int32(17), int32(1)}, texts: [2]string{"6", "17"}},
		{key: "uuid", pg: "uuid", name: "uuid", typ: parquet.TypeUUID,
			vals: [3]any{"00000000-0000-4000-8000-000000000001", "00000000-0000-4000-8000-000000000002",
				"00000000-0000-4000-8000-000000000003"},
			texts: [2]string{"00000000-0000-4000-8000-000000000001", "00000000-0000-4000-8000-000000000002"}},
		{key: "ipv6", pg: "inet", name: "inet", typ: parquet.TypeIPv6,
			vals:  [3]any{"2001:db8::1", "2001:db8::2", "2001:db8::3"},
			texts: [2]string{"2001:db8::1", "2001:db8::2"}},
		{key: "cidr", pg: "cidr", name: "cidr", typ: parquet.TypeCIDR,
			vals:  [3]any{"10.0.0.0/8", "192.168.0.0/16", "172.16.0.0/12"},
			texts: [2]string{"10.0.0.0/8", "192.168.0.0/16"}},
		{key: "dur", pg: "bigint", name: "bigint", typ: parquet.TypeDuration,
			vals: [3]any{int64(1_000_000), int64(2_000_000), int64(3_000_000)}, texts: [2]string{"1000000", "2000000"}},
		{key: "date", pg: "date", name: "date", typ: parquet.TypeDate,
			vals: [3]any{"2024-01-02", "2024-03-04", "2024-05-06"}, texts: [2]string{"2024-01-02", "2024-03-04"}},
		{key: "ts", pg: "timestamp", name: "timestamp without time zone", typ: parquet.TypeTimestamp,
			vals:  [3]any{int64(1_704_164_645_000), int64(1_709_521_445_000), int64(1_714_964_645_000)},
			texts: [2]string{"2024-01-02 03:04:05", "2024-03-04 03:04:05"}},
		{key: "bool", pg: "boolean", name: "boolean", typ: parquet.TypeBool,
			vals: [3]any{true, false, true}, texts: [2]string{"true", "false"}},
	}
}

// stTable is the `st_pair` fixture: id 1..4, and per type v_<key> / s_<key>.
func stTable() tmdTable {
	schema := parquet.Schema{Columns: []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}}
	rows := make([]map[string]any, 4)
	for i := range rows {
		rows[i] = map[string]any{"id": int64(i + 1)}
	}
	for _, ty := range stTypes() {
		schema.Columns = append(schema.Columns,
			parquet.Column{Name: "v_" + ty.key, Type: ty.typ, Precision: ty.prec, Scale: ty.scale, Nullable: true},
			parquet.Column{Name: "s_" + ty.key, Type: parquet.TypeString, Nullable: true})
		for i := 0; i < 3; i++ {
			rows[i]["v_"+ty.key] = ty.vals[i]
		}
		rows[0]["s_"+ty.key], rows[1]["s_"+ty.key], rows[2]["s_"+ty.key] = ty.texts[0], ty.texts[1], "zz"
		rows[3]["v_"+ty.key], rows[3]["s_"+ty.key] = nil, nil
	}
	return tmdTable{name: "st_pair", schema: schema, rows: rows}
}

// stPGFixture is the same fixture as PostgreSQL DDL + INSERT, for the oracle.
func stPGFixture() string {
	var cols []string
	vals := make([][]string, 4)
	for i := range vals {
		vals[i] = []string{fmt.Sprint(i + 1)}
	}
	for _, ty := range stTypes() {
		cols = append(cols, "v_"+ty.key+" "+ty.pg, "s_"+ty.key+" text")
		for i := 0; i < 3; i++ {
			v := ty.vals[i]
			switch ty.key {
			case "ts":
				vals[i] = append(vals[i], "to_timestamp("+fmt.Sprint(v.(int64)/1000)+") AT TIME ZONE 'UTC'")
			case "bool", "i64", "i32", "dec", "f64", "port", "proto", "dur":
				vals[i] = append(vals[i], fmt.Sprint(v))
			default:
				vals[i] = append(vals[i], "'"+fmt.Sprint(v)+"'")
			}
			s := "zz"
			if i < 2 {
				s = ty.texts[i]
			}
			vals[i] = append(vals[i], "'"+s+"'")
		}
		vals[3] = append(vals[3], "NULL", "NULL")
	}
	var b strings.Builder
	b.WriteString("DROP TABLE IF EXISTS st_pair;\nCREATE TABLE st_pair (id bigint, " + strings.Join(cols, ", ") + ");\n")
	for _, r := range vals {
		b.WriteString("INSERT INTO st_pair VALUES (" + strings.Join(r, ", ") + ");\n")
	}
	return b.String()
}

// stStandalone is one embedded engine over `st_pair` alone (budget 0 = none).
func stStandalone(t *testing.T, ctx context.Context, budget int64) *wadjet.DB {
	t.Helper()
	cfg := wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"}
	if budget > 0 {
		cfg.MemoryBudget = budget
		cfg.SpillDir = t.TempDir()
	}
	db, err := wadjet.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("open standalone: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	tbl := stTable()
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

// stArms is the five arms over `st_pair`: one process, one process under a
// 512 KiB budget, the DAG, the DAG with every join shuffled (the build is
// hash-PARTITIONED by its key), and the DAG with four morsel workers.
func stArms(t *testing.T, ctx context.Context) []brArm {
	t.Helper()
	single := stStandalone(t, ctx, 0)
	spilled := stStandalone(t, ctx, 512*1024)
	stand := func(wcfg func(*worker.Config), opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, []tmdTable{stTable()})
		if wcfg != nil {
			return tmdCoordinatorWithWorkers(t, ctx, infra, wcfg, opts...)
		}
		return tmdCoordinator(t, ctx, infra, opts...)
	}
	coord := stand(nil)
	coordB := stand(nil, func(c *Config) { c.BroadcastBytesOverride = 1 })
	coordM := stand(func(w *worker.Config) { w.MorselWorkers = 4 })
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

// stBody is one way a membership body can produce the TEXT key: `from` is
// the relation (aliased r, with columns id and the key), `key` the key
// expression over r, `with` a leading CTE.
type stBody struct {
	kind, with, from, key string
}

// stBodies are the five spellings of a TEXT key for type ty (#1308's scope):
// a stored column, `CAST(v AS TEXT)` (#1073's kept shape), a TEXT literal, a
// derived table's column and a CTE's column. mirror swaps the sides: the
// outer value is the TEXT column and the body selects the typed column.
func stBodies(ty stType, mirror bool) []stBody {
	v, s := "v_"+ty.key, "s_"+ty.key
	if mirror {
		v, s = s, v
	}
	out := []stBody{
		{kind: "stored", from: "st_pair r", key: "r." + s},
		{kind: "derived", from: "(SELECT id, " + s + " AS k FROM st_pair) r", key: "r.k"},
		{kind: "cte", with: "WITH c AS (SELECT id, " + s + " AS k FROM st_pair) ", from: "c r", key: "r.k"},
	}
	if !mirror {
		out = append(out,
			stBody{kind: "cast", from: "st_pair r", key: "CAST(r." + v + " AS TEXT)"},
			stBody{kind: "literal", from: "st_pair r", key: "'" + ty.texts[0] + "'"})
	}
	return out
}

// stShape is one membership spelling; filter is the body's row filter
// (ids 1..3 hold no NULL; id 4 is NULL on both sides; id < 0 is empty).
type stShape struct {
	name, filter string
	pred         func(outer, key, from, filter string) string
}

func stShapes() []stShape {
	sub := func(key, from, filter string) string { return "SELECT " + key + " FROM " + from + " WHERE " + filter }
	exists := func(not string) func(o, k, f, fl string) string {
		return func(o, k, f, fl string) string {
			return not + "EXISTS (SELECT 1 FROM " + f + " WHERE " + fl + " AND " + o + " = " + k + ")"
		}
	}
	return []stShape{
		{"in", "r.id <= 3", func(o, k, f, fl string) string { return o + " IN (" + sub(k, f, fl) + ")" }},
		{"eqAny", "r.id <= 3", func(o, k, f, fl string) string { return o + " = ANY (" + sub(k, f, fl) + ")" }},
		{"notIn", "r.id <= 3", func(o, k, f, fl string) string { return o + " NOT IN (" + sub(k, f, fl) + ")" }},
		{"neAll", "r.id <= 3", func(o, k, f, fl string) string { return o + " <> ALL (" + sub(k, f, fl) + ")" }},
		{"exists", "r.id <= 3", exists("")},
		{"notExists", "r.id <= 3", exists("NOT ")},
		{"inNullBody", "r.id <= 4", func(o, k, f, fl string) string { return o + " IN (" + sub(k, f, fl) + ")" }},
		{"notInNullBody", "r.id <= 4", func(o, k, f, fl string) string { return o + " NOT IN (" + sub(k, f, fl) + ")" }},
		{"inEmptyBody", "r.id < 0", func(o, k, f, fl string) string { return o + " IN (" + sub(k, f, fl) + ")" }},
		{"notInEmptyBody", "r.id < 0", func(o, k, f, fl string) string { return o + " NOT IN (" + sub(k, f, fl) + ")" }},
	}
}

// stQuery is one cell: name and SQL (a count over the four outer rows).
type stQuery struct{ name, sql string }

// stQueryFor is one generated cell. `a.v = '12'` in an EXISTS is an UNKNOWN
// literal compared directly — not a TEXT key, not #1308 — so the literal
// body has no EXISTS cells.
func stQueryFor(ty stType, mirror bool, b stBody, sh stShape) (stQuery, bool) {
	if b.kind == "literal" && strings.Contains(sh.name, "xists") {
		return stQuery{}, false
	}
	outer, dir := "a.v_"+ty.key, ty.key
	if mirror {
		outer, dir = "a.s_"+ty.key, "mirror/"+ty.key
	}
	return stQuery{
		name: sh.name + "/" + dir + "/" + b.kind,
		sql:  b.with + "SELECT count(*) AS n FROM st_pair a WHERE " + sh.pred(outer, b.key, b.from, sh.filter),
	}, true
}

// stQueries is every generated cell, for the PostgreSQL measurement.
func stQueries() []stQuery {
	var out []stQuery
	for _, ty := range stTypes() {
		for _, mirror := range []bool{false, true} {
			for _, b := range stBodies(ty, mirror) {
				for _, sh := range stShapes() {
					if q, ok := stQueryFor(ty, mirror, b, sh); ok {
						out = append(out, q)
					}
				}
			}
		}
	}
	return out
}

// stKept is the typed side of a pair ADR-0012 §5 keeps for a membership
// against a `CAST(x AS TEXT)` body (textConversionAnswers); DATE, TIMESTAMP
// and BOOLEAN are 42883 there.
func stKept(key string) bool { return key != "date" && key != "ts" && key != "bool" }

// stWant is the tip's disposition for one generated cell: the count a kept
// CAST body answers (the conversion extension: every non-NULL outer value's
// own text is in the body), or PostgreSQL 17.11's 42883 in the explicit
// JOIN's words for every other body. PostgreSQL refuses EVERY cell of the
// table (measured live: 1014 of 1014 are 42883 `operator does not exist`);
// the CAST rows are the recorded superset. `<> ALL` names `=` where
// PostgreSQL names `<>`: the parser reads `x <> ALL (subquery)` as
// `x NOT IN (subquery)` before any rule sees it (recorded in the notes).
func stWant(ty stType, mirror bool, body, shape string) (state, msg, want string) {
	exists := shape == "exists" || shape == "notExists"
	if body == "cast" && !mirror && (exists || stKept(ty.key)) {
		return "", "", map[string]string{
			"in": "rows=1 3", "eqAny": "rows=1 3", "notIn": "rows=1 0", "neAll": "rows=1 0",
			"exists": "rows=1 3", "notExists": "rows=1 1",
			"inNullBody": "rows=1 3", "notInNullBody": "rows=1 0",
			"inEmptyBody": "rows=1 0", "notInEmptyBody": "rows=1 4",
		}[shape]
	}
	if mirror {
		return "42883", "operator does not exist: text = " + ty.name, ""
	}
	return "42883", "operator does not exist: " + ty.name + " = text", ""
}

// stNeighbourCells are the spellings beside the table: the same key
// mechanism through DISTINCT, LIMIT, an aggregate, a correlated IN key, a
// swapped EXISTS key, a typed literal outer, a CAST of TEXT, a derived CAST
// column — each refused — and the controls that keep answering: a
// correlated equality under OR and an uncorrelated one (filters, the direct
// comparison's reading), a scalar subquery, a CAST body under LIMIT.
func stNeighbourCells() []brArmCell {
	refuse := func(name, sql, msg string) brArmCell {
		return brArmCell{name: "neighbour/" + name, sql: sql, state: "42883", msg: msg}
	}
	const bt = "operator does not exist: bigint = text"
	return []brArmCell{
		refuse("distinctBody", "SELECT count(*) AS n FROM st_pair a WHERE a.v_i64 IN (SELECT DISTINCT r.s_i64 FROM st_pair r WHERE r.id <= 3)", bt),
		refuse("limitBody", "SELECT count(*) AS n FROM st_pair a WHERE a.v_i64 IN (SELECT r.s_i64 FROM st_pair r WHERE r.id <= 3 LIMIT 5)", bt),
		refuse("aggGrouped", "SELECT count(*) AS n FROM st_pair a WHERE a.v_i64 IN (SELECT max(r.s_i64) FROM st_pair r WHERE r.id <= 3 GROUP BY r.id)", bt),
		refuse("aggUngrouped", "SELECT count(*) AS n FROM st_pair a WHERE a.v_i64 IN (SELECT max(r.s_i64) FROM st_pair r WHERE r.id <= 1)", bt),
		refuse("correlatedInKey", "SELECT count(*) AS n FROM st_pair a WHERE a.v_i64 IN (SELECT r.s_i64 FROM st_pair r WHERE r.id = a.id)", bt),
		refuse("correlatedKeyOnly", "SELECT count(*) AS n FROM st_pair a WHERE a.id IN (SELECT r.id FROM st_pair r WHERE r.s_i64 = a.v_i64)", "operator does not exist: text = bigint"),
		refuse("existsSwapped", "SELECT count(*) AS n FROM st_pair a WHERE EXISTS (SELECT 1 FROM st_pair r WHERE r.id <= 3 AND r.s_i64 = a.v_i64)", "operator does not exist: text = bigint"),
		refuse("typedLiteralOuter", "SELECT count(*) AS n FROM st_pair a WHERE 12 IN (SELECT r.s_i64 FROM st_pair r WHERE r.id <= 3)", "operator does not exist: integer = text"),
		refuse("literalBodyNotConverting", "SELECT count(*) AS n FROM st_pair a WHERE a.v_i64 IN (SELECT 'zz' FROM st_pair r WHERE r.id <= 3)", bt),
		refuse("castOfText", "SELECT count(*) AS n FROM st_pair a WHERE a.v_i64 IN (SELECT CAST(r.s_i64 AS TEXT) FROM st_pair r WHERE r.id <= 3)", bt),
		refuse("derivedCastColumn", "SELECT count(*) AS n FROM st_pair a WHERE a.v_i64 IN (SELECT r.k FROM (SELECT CAST(v_i64 AS TEXT) AS k FROM st_pair) r)", bt),
		refuse("existsDerivedCastColumn", "SELECT count(*) AS n FROM st_pair a WHERE EXISTS (SELECT 1 FROM (SELECT id, CAST(v_i64 AS TEXT) AS k FROM st_pair) r WHERE r.id <= 3 AND a.v_i64 = r.k)", bt),
		refuse("inInSelectList", "SELECT a.id, a.v_i64 IN (SELECT r.s_i64 FROM st_pair r) AS m FROM st_pair a", bt),
		refuse("explicitJoin", "SELECT count(*) AS n FROM st_pair a JOIN st_pair r ON a.v_i64 = r.s_i64", bt),
		{name: "neighbour/existsKeyUnderOr", want: "rows=1 2",
			sql: "SELECT count(*) AS n FROM st_pair a WHERE EXISTS (SELECT 1 FROM st_pair r WHERE r.id <= 3 AND (a.v_i64 = r.s_i64 OR r.id = 99))"},
		{name: "neighbour/existsUncorrelated", want: "rows=1 4",
			sql: "SELECT count(*) AS n FROM st_pair a WHERE EXISTS (SELECT 1 FROM st_pair r WHERE r.v_i64 = r.s_i64)"},
		{name: "neighbour/scalarSubquery", want: "rows=1 1",
			sql: "SELECT count(*) AS n FROM st_pair a WHERE a.v_i64 = (SELECT r.s_i64 FROM st_pair r WHERE r.id = 1)"},
		{name: "neighbour/castBodyLimit", want: "rows=1 3",
			sql: "SELECT count(*) AS n FROM st_pair a WHERE a.v_i64 IN (SELECT CAST(r.v_i64 AS TEXT) FROM st_pair r WHERE r.id <= 3 LIMIT 5)"},
		{name: "neighbour/directColumnPair", want: "rows=1 2",
			sql: "SELECT count(*) AS n FROM st_pair a WHERE a.v_i64 = a.s_i64"},
		{name: "neighbour/inList", want: "rows=1 2",
			sql: "SELECT count(*) AS n FROM st_pair a WHERE a.v_i64 IN (a.s_i64)"},
	}
}

// A TYPED IN / = ANY / NOT IN / <> ALL / EXISTS / NOT EXISTS BODY SELECTING A
// STORED TEXT COLUMN (#1308). At v0.25.1 every such body planned a semi/anti
// join whose key pair (typed, text) was never typed: `v IN (SELECT s …)`
// answered 0 rows over matching values and NOT IN every row on all five
// arms, the mirror (`s IN (SELECT v …)`) failed with #615's key error on the
// single arms and answered 0 on one DAG arm, and a TEXT literal body answered
// 0 rows on the single arms and 1 on the DAG for DATE / TIMESTAMP / BOOLEAN.
// PostgreSQL 17.11 refuses every cell 42883. The pair now takes the rule an
// explicit JOIN with those keys takes: kept only where the body's text is a
// CAST from the typed side's own (kept) class — #1073's shape — and 42883 in
// the JOIN's words everywhere else, on every arm, before a row is read.
func TestArcSTStoredTextMembershipEveryArmRefusesOrConverts(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the stored-text membership table")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	arms := stArms(t, ctx)
	var cells []brArmCell
	for _, ty := range stTypes() {
		for _, mirror := range []bool{false, true} {
			for _, b := range stBodies(ty, mirror) {
				for _, sh := range stShapes() {
					q, ok := stQueryFor(ty, mirror, b, sh)
					if !ok {
						continue
					}
					c := brArmCell{name: q.name, sql: q.sql}
					c.state, c.msg, c.want = stWant(ty, mirror, b.kind, sh.name)
					cells = append(cells, c)
				}
			}
		}
	}
	cells = append(cells, stNeighbourCells()...)
	refused, answered := 0, 0
	for _, tc := range cells {
		if tc.state != "" {
			refused++
		} else {
			answered++
		}
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				res, err := arm.run(tc.sql)
				if tc.state != "" {
					if err == nil {
						t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s %q (PostgreSQL 17.11), before a row is read",
							tc.sql, arm.name, brRender(res), tc.state, tc.msg)
						continue
					}
					if st := sqlerr.StateOf(err); st != tc.state || !strings.Contains(err.Error(), tc.msg) {
						t.Errorf("%s\n  arm  %s\n  got  %s %v\n  want %s %q", tc.sql, arm.name, st, err, tc.state, tc.msg)
					}
					continue
				}
				if err != nil {
					t.Errorf("%s\n  arm  %s\n  refused: %v\n  want %s (the kept conversion)", tc.sql, arm.name, err, tc.want)
					continue
				}
				if got := brRender(res); got != tc.want {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s", tc.sql, arm.name, got, tc.want)
				}
			}
		})
	}
	if refused < 800 || answered < 100 {
		t.Fatalf("%d refused / %d answered cells: the table must hold both the refusals and the kept conversion", refused, answered)
	}
}

// THE PLAN SHOWS THE TYPED KEY OR THE REFUSAL (#1308): EXPLAIN over a
// stored-text body refuses with the statement, on every arm — it never shows
// `Join: semi ON v_i64 = s_i64` — and the kept CAST body plans no bare
// text/typed semi-join key at all (its key is the CAST, a filter).
func TestArcSTExplainNeverShowsABareTextTypedSemiKey(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	arms := stArms(t, ctx)
	refused := []string{
		"EXPLAIN SELECT count(*) AS n FROM st_pair a WHERE a.v_i64 IN (SELECT r.s_i64 FROM st_pair r WHERE r.id <= 3)",
		"EXPLAIN SELECT count(*) AS n FROM st_pair a WHERE a.v_i64 NOT IN (SELECT r.s_i64 FROM st_pair r WHERE r.id <= 3)",
		"EXPLAIN SELECT count(*) AS n FROM st_pair a WHERE EXISTS (SELECT 1 FROM st_pair r WHERE a.v_i64 = r.s_i64)",
		"EXPLAIN SELECT count(*) AS n FROM st_pair a WHERE NOT EXISTS (SELECT 1 FROM st_pair r WHERE a.v_uuid = r.s_uuid)",
		"EXPLAIN SELECT count(*) AS n FROM st_pair a WHERE a.s_i64 IN (SELECT r.v_i64 FROM st_pair r)",
	}
	kept := []string{
		"EXPLAIN SELECT count(*) AS n FROM st_pair a WHERE a.v_i64 IN (SELECT CAST(r.v_i64 AS TEXT) FROM st_pair r WHERE r.id <= 3)",
		"EXPLAIN SELECT count(*) AS n FROM st_pair a WHERE EXISTS (SELECT 1 FROM st_pair r WHERE a.v_i64 = CAST(r.v_i64 AS TEXT))",
	}
	for _, arm := range arms {
		for _, sql := range refused {
			res, err := arm.run(sql)
			if err == nil {
				t.Errorf("%s\n  arm  %s\n  plan %s\n  want 42883: a bare text/typed key never reaches a plan", sql, arm.name, brRenderOrdered(res))
				continue
			}
			if st := sqlerr.StateOf(err); st != "42883" {
				t.Errorf("%s\n  arm  %s\n  got  %s %v\n  want 42883", sql, arm.name, st, err)
			}
		}
		for _, sql := range kept {
			res, err := arm.run(sql)
			if err != nil {
				t.Errorf("%s\n  arm  %s\n  refused: %v", sql, arm.name, err)
				continue
			}
			plan := brRenderOrdered(res)
			if !strings.Contains(plan, "CAST(") {
				t.Errorf("%s\n  arm  %s\n  plan %s\n  want the CAST at the key", sql, arm.name, plan)
			}
			for _, bare := range []string{"ON v_i64 = s_i64", "ON s_i64 = v_i64"} {
				if strings.Contains(plan, bare) {
					t.Errorf("%s\n  arm  %s\n  plan %s\n  shows the bare key %q", sql, arm.name, plan, bare)
				}
			}
		}
	}
}
