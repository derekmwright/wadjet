// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/server/pgwire"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// tcSchema is the fixture's one schema: a TIMESTAMP, a DATE and a TEXT
// column beside the key. tc_t holds the EPOCH in both temporal columns
// (row 1) beside non-epoch rows, so a literal read as 0 answers a row
// PostgreSQL never does; tc_e is the same schema with no rows; tc_k is a
// join partner.
func tcSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "ts", Type: parquet.TypeTimestamp, Nullable: true},
		{Name: "d", Type: parquet.TypeDate, Nullable: true},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
	}}
}

func tcMillis(s string) int64 {
	tm, err := time.Parse("2006-01-02 15:04:05.999", s)
	if err != nil {
		panic(err)
	}
	return tm.UnixMilli()
}

// tcRowsSpec: id, ts, d, s ("" in ts/d = NULL; s uses "<null>").
var tcRowsSpec = [][4]string{
	{"1", "1970-01-01 00:00:00", "1970-01-01", "garbage"},
	{"2", "2024-01-15 10:30:00", "2024-01-15", "2024-01-15 10:30:00"},
	{"3", "2024-01-15 10:30:00.123", "2024-01-16", ""},
	{"4", "2000-02-29 00:00:00", "2000-02-29", "2000-02-29"},
	{"5", "2024-01-15 00:00:00", "2024-01-15", "<null>"},
	{"6", "", "", "1970-01-01 00:00:00"},
	{"7", "9999-12-31 23:59:59", "9999-12-31", "infinity"},
	{"8", "1969-12-31 23:59:59.999", "1969-12-31", "2024-02-30"},
}

func tcRows() []map[string]any {
	rows := make([]map[string]any, len(tcRowsSpec))
	for i, r := range tcRowsSpec {
		var id int64
		fmt.Sscan(r[0], &id)
		m := map[string]any{"id": id, "ts": nil, "d": nil, "s": nil}
		if r[1] != "" {
			m["ts"] = tcMillis(r[1])
		}
		if r[2] != "" {
			m["d"] = r[2]
		}
		if r[3] != "<null>" {
			m["s"] = r[3]
		}
		rows[i] = m
	}
	return rows
}

func tcKSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{{Name: "k", Type: parquet.TypeInt64}}}
}

func tcKRows() []map[string]any {
	var rows []map[string]any
	for _, k := range []int64{1, 2, 3, 4, 5, 6, 7, 8} {
		rows = append(rows, map[string]any{"k": k})
	}
	return rows
}

// tcPGFixture is the same fixture as PostgreSQL DDL.
func tcPGFixture() []string {
	var vals []string
	q := func(s string) string {
		if s == "" || s == "<null>" {
			return "NULL"
		}
		return "'" + s + "'"
	}
	for _, r := range tcRowsSpec {
		s := "NULL"
		if r[3] != "<null>" {
			s = "'" + r[3] + "'"
		}
		vals = append(vals, fmt.Sprintf("(%s,%s,%s,%s)", r[0], q(r[1]), q(r[2]), s))
	}
	return []string{
		"CREATE TABLE tc_t (id bigint, ts timestamp, d date, s text)",
		"INSERT INTO tc_t VALUES " + strings.Join(vals, ","),
		"CREATE TABLE tc_e (id bigint, ts timestamp, d date, s text)",
		"CREATE TABLE tc_k (k bigint)",
		"INSERT INTO tc_k VALUES (1),(2),(3),(4),(5),(6),(7),(8)",
	}
}

func tcTables() []tmdTable {
	return []tmdTable{
		{name: "tc_t", schema: tcSchema(), rows: tcRows()},
		{name: "tc_e", schema: tcSchema()},
		{name: "tc_k", schema: tcKSchema(), rows: tcKRows()},
	}
}

// tcStandalone is one embedded engine over the fixture, two rows per row
// group so a scan crosses several (budget 0 = none).
func tcStandalone(t *testing.T, ctx context.Context, budget int64) *wadjet.DB {
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
	for _, tbl := range tcTables() {
		if err := db.CreateTable(ctx, tbl.name, tbl.schema, nil); err != nil {
			t.Fatalf("create %s: %v", tbl.name, err)
		}
		if len(tbl.rows) == 0 {
			continue
		}
		ing := db.NewIngester(tbl.name, tbl.schema, nil, ingest.Config{MaxBufferRows: len(tbl.rows) + 1, RowGroupSize: 2})
		if err := ing.Ingest(ctx, tbl.rows); err != nil {
			t.Fatalf("ingest %s: %v", tbl.name, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", tbl.name, err)
		}
	}
	return db
}

// tcTexts is the text dimension: PostgreSQL 17.11's date/time input grammar
// as the comparison meets it — accepted spellings (ISO with `T` or a space,
// a fraction, a numeric zone the TIMESTAMP reading discards, a date alone,
// 24:00:00, padding, the year-first separators, YYYYMMDD, the range edges),
// refused ones (22007 shapes, 22008 fields, 22009 zones), and the special
// values and named forms PostgreSQL reads and this grammar refuses
// (temporal r2 / r25).
var tcTexts = []string{
	// accepted, both grammars
	"2024-01-15 10:30:00", "2024-01-15T10:30:00", "2024-01-15 10:30:00.123",
	"2024-01-15 10:30:00+05:30", "2024-01-15 10:30:00Z", "2024-01-15 10:30:00-08",
	"1970-01-01 00:00:00", "1970-01-01", "2024-01-15", "  2024-01-15 10:30:00  ",
	"2024-01-14 24:00:00", "2024-01-15 10:30", "20240115", "2024/01/15",
	"1969-12-31 23:59:59.999", "2000-02-29", "10000-01-01",
	// refused
	"garbage", "", "   ", "2024-02-30", "2024-13-01", "2024-01-15 25:00:00",
	"2024-01-15 24:00:01", "2024-01-15 10:60:00", "2024-01-15 10:30:00+16",
	"2024-01-15 10:30:00-24", "2024-01-15 10:30:00+053000", "2024-01-15 10:30:00Z+05",
	"0000-01-01", "2024-01-15 10:30:00 garbage", "2024-01-15x", "294277-01-01",
	"2023-02-29",
	// PostgreSQL's special values and named forms
	"infinity", "-infinity", "epoch", "now", "today", "allballs",
	"Jan 15 2024", "J2460325", "2024-01-15 BC",
}

// tcSubset is the text subset the consumer, origin and parameter
// dimensions take: a valid instant, the epoch spelled validly, the issue's
// refused spellings (22007, 22008, 22009, year zero), a special value and
// a zone the TIMESTAMP reading discards.
var tcSubset = []string{
	"2024-01-15 10:30:00", "1970-01-01 00:00:00", "garbage", "", "2024-02-30",
	"2024-01-15 10:30:00+16", "0000-01-01", "infinity", "epoch",
	"2024-01-15 10:30:00+05:30",
}

type tcCell struct {
	name   string
	sql    string
	params []string // non-nil: run through ExecParams with paramOIDs
	oids   []uint32
}

func tcQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// tcCells generates the table: column × text on `=` (the grammar), then
// column × consumer × subset, column × origin × subset, column × a bound
// parameter (OID unknown / text / the column's type) × subset, and the
// TEXT-column comparisons.
func tcCells() []tcCell {
	var cells []tcCell
	cols := []string{"ts", "d"}
	for _, c := range cols {
		for _, x := range tcTexts {
			cells = append(cells, tcCell{
				name: fmt.Sprintf("eq/%s/%q", c, x),
				sql:  fmt.Sprintf("SELECT id FROM tc_t WHERE %s = %s ORDER BY id", c, tcQuote(x)),
			})
		}
	}
	consumers := []struct{ name, tmpl string }{
		{"ne", "SELECT id FROM tc_t WHERE {C} <> {L} ORDER BY id"},
		{"lt", "SELECT id FROM tc_t WHERE {C} < {L} ORDER BY id"},
		{"le", "SELECT id FROM tc_t WHERE {C} <= {L} ORDER BY id"},
		{"gt", "SELECT id FROM tc_t WHERE {C} > {L} ORDER BY id"},
		{"ge", "SELECT id FROM tc_t WHERE {C} >= {L} ORDER BY id"},
		{"lit_left", "SELECT id FROM tc_t WHERE {L} = {C} ORDER BY id"},
		{"not_distinct", "SELECT id FROM tc_t WHERE {C} IS NOT DISTINCT FROM {L} ORDER BY id"},
		{"between", "SELECT id FROM tc_t WHERE {C} BETWEEN {L} AND '2030-01-01' ORDER BY id"},
		{"between_hi", "SELECT id FROM tc_t WHERE {C} BETWEEN '1960-01-01' AND {L} ORDER BY id"},
		{"in", "SELECT id FROM tc_t WHERE {C} IN ('2000-02-29', {L}) ORDER BY id"},
		{"in_one", "SELECT id FROM tc_t WHERE {C} IN ({L}) ORDER BY id"},
		{"not_in", "SELECT id FROM tc_t WHERE {C} NOT IN ('2000-02-29', {L}) ORDER BY id"},
		{"any", "SELECT id FROM tc_t WHERE {C} = ANY({A}) ORDER BY id"},
		{"case", "SELECT id, CASE WHEN {C} = {L} THEN 1 ELSE 0 END FROM tc_t ORDER BY id"},
		{"select", "SELECT id, {C} = {L} FROM tc_t ORDER BY id"},
		{"or", "SELECT id FROM tc_t WHERE id = 4 OR {C} = {L} ORDER BY id"},
		{"not", "SELECT id FROM tc_t WHERE NOT ({C} = {L}) ORDER BY id"},
		{"join_on", "SELECT a.id FROM tc_t a JOIN tc_k b ON a.id = b.k AND a.{C} = {L} ORDER BY 1"},
		{"left_join_on", "SELECT b.k, a.id FROM tc_k b LEFT JOIN tc_t a ON a.id = b.k AND a.{C} = {L} ORDER BY 1"},
		{"having", "SELECT id % 2, count(*) FROM tc_t GROUP BY 1 HAVING max({C}) = {L} ORDER BY 1"},
		{"count", "SELECT count(*) FROM tc_t WHERE {C} = {L}"},
		{"no_row_reaches", "SELECT id FROM tc_t WHERE id > 100 AND {C} = {L} ORDER BY id"},
		{"empty", "SELECT id FROM tc_e WHERE {C} = {L} ORDER BY id"},
	}
	origins := []struct{ name, tmpl string }{
		{"derived", "SELECT id FROM (SELECT id, ts, d FROM tc_t) x WHERE x.{C} = {L} ORDER BY id"},
		{"cte", "WITH x AS (SELECT id, ts, d FROM tc_t) SELECT id FROM x WHERE {C} = {L} ORDER BY id"},
		{"union_all", "SELECT id FROM (SELECT id, ts, d FROM tc_t WHERE id <= 4 UNION ALL SELECT id, ts, d FROM tc_t WHERE id > 4) x WHERE {C} = {L} ORDER BY id"},
		{"values", "SELECT id FROM (VALUES (1, TIMESTAMP '1970-01-01 00:00:00', DATE '1970-01-01'), (2, TIMESTAMP '2024-01-15 10:30:00', DATE '2024-01-15')) x(id, ts, d) WHERE {C} = {L} ORDER BY id"},
		{"scalar_subquery", "SELECT id FROM tc_t WHERE id = 1 AND (SELECT min({C}) FROM tc_t) = {L} ORDER BY id"},
		{"cast", "SELECT id FROM tc_t WHERE CAST({C} AS {T}) = {L} ORDER BY id"},
	}
	typeOf := map[string]string{"ts": "TIMESTAMP", "d": "DATE"}
	arrayLit := func(x string) string {
		return tcQuote(`{"2000-02-29","` + strings.ReplaceAll(x, `"`, `\"`) + `"}`)
	}
	expand := func(tmpl, c, x string) string {
		r := strings.NewReplacer("{C}", c, "{L}", tcQuote(x), "{A}", arrayLit(x), "{T}", typeOf[c])
		return r.Replace(tmpl)
	}
	for _, c := range cols {
		for _, k := range append(consumers, origins...) {
			for _, x := range tcSubset {
				cells = append(cells, tcCell{name: fmt.Sprintf("%s/%s/%q", k.name, c, x), sql: expand(k.tmpl, c, x)})
			}
		}
	}
	// A bound parameter: unknown-typed (OID 0), text (25), and the column's
	// own type (1114 / 1082), text format.
	ownOID := map[string]uint32{"ts": 1114, "d": 1082}
	for _, c := range cols {
		for _, oid := range []uint32{0, 25, ownOID[c]} {
			for _, x := range tcSubset {
				cells = append(cells, tcCell{
					name:   fmt.Sprintf("param%d/%s/%q", oid, c, x),
					sql:    fmt.Sprintf("SELECT id FROM tc_t WHERE %s = $1 ORDER BY id", c),
					params: []string{x}, oids: []uint32{oid},
				})
			}
		}
	}
	// A TEXT column against the temporal one (PostgreSQL: 42883).
	for _, c := range cols {
		for _, op := range []string{"=", "<", "<>"} {
			cells = append(cells, tcCell{
				name: fmt.Sprintf("text_col/%s/%s", c, op),
				sql:  fmt.Sprintf("SELECT id FROM tc_t WHERE %s %s s ORDER BY id", c, op),
			})
		}
		cells = append(cells, tcCell{
			name: fmt.Sprintf("text_col_cast/%s", c),
			sql:  fmt.Sprintf("SELECT id FROM tc_t WHERE %s = CAST(s AS %s) ORDER BY id", c, typeOf[c]),
		})
	}
	return cells
}

func tcRender(rr *pgconn.ResultReader) string {
	res := rr.Read()
	if res.Err != nil {
		return pwDoorErr(res.Err)
	}
	var rows []string
	for _, r := range res.Rows {
		var f []string
		for _, v := range r {
			if v == nil {
				f = append(f, "NULL")
			} else {
				f = append(f, string(v))
			}
		}
		rows = append(rows, strings.Join(f, "|"))
	}
	return "rows=[" + strings.Join(rows, " ; ") + "]"
}

func tcRun(ctx context.Context, conn *pgconn.PgConn, c tcCell) string {
	if c.params != nil {
		args := make([][]byte, len(c.params))
		for i, p := range c.params {
			args[i] = []byte(p)
		}
		return tcRender(conn.ExecParams(ctx, c.sql, args, c.oids, nil, nil))
	}
	mrr := conn.Exec(ctx, c.sql)
	defer mrr.Close()
	out := "rows=[]"
	for mrr.NextResult() {
		out = tcRender(mrr.ResultReader())
	}
	if err := mrr.Close(); err != nil {
		return pwDoorErr(err)
	}
	return out
}

type tcPin struct{ pg, kept, row string }

// tcPins reads testdata/arc_tc_timestamp_text_compare_pg17.tsv: cell,
// PostgreSQL 17.11's answer, and for a kept divergence the engine's answer
// and its catalog row (a pin naming an open defect carries its issue there).
func tcPins(t *testing.T) map[string]tcPin {
	t.Helper()
	f, err := os.Open("testdata/arc_tc_timestamp_text_compare_pg17.tsv")
	if err != nil {
		if os.Getenv("TC_PG_DUMP") != "" || os.Getenv("TC_ENGINE_DUMP") != "" {
			return map[string]tcPin{}
		}
		t.Fatal(err)
	}
	defer f.Close()
	pins := map[string]tcPin{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		cols := strings.Split(line, "\t")
		for len(cols) < 4 {
			cols = append(cols, "")
		}
		pins[cols[0]] = tcPin{pg: cols[1], kept: cols[2], row: cols[3]}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return pins
}

// tcDoorPin is an answer one door gives for a reason that is not this
// arc's rule; a pin that starts agreeing with PostgreSQL FAILS. #1190: a
// relation with no files has no distributed scan stage, so on the three DAG
// doors a statement over tc_e that the plan does not refuse first answers
// 42000.
func tcDoorPin(door string, c tcCell, want string) (string, string) {
	if strings.HasPrefix(door, "dag") && strings.HasPrefix(c.name, "empty/") && !strings.HasPrefix(want, "ERR ") {
		return "ERR 42000", "#1190 — a relation with no files has no distributed scan stage"
	}
	return "", ""
}

// TestArcTCTimestampTextComparisonEveryArm: a TIMESTAMP or DATE compared
// with (or coerced from) quoted text, on five doors over pgwire — the
// embedded engine, the embedded engine under a 512 KiB budget, and the
// coordinator's router over the DAG, the shuffled DAG and the DAG with four
// morsel workers — against PostgreSQL 17.11's answer (re-measured when
// WADJET_PG_DSN names a server: the column must equal the live answer).
//
// The cells are the text dimension on `=` (eq/…), every consumer and every
// column origin over the issue's subset, and a TEXT column against the
// temporal one. The bound-parameter cells are TestArcTCBoundParameterEveryDoor.
//
// At 93e4804e `ts = 'garbage'` (and ”, '2024-02-30', the +16 / -24 /
// +053000 zones, '0000-01-01', …) answered the epoch row 1 on every door,
// a CASE / SELECT-list / HAVING comparison answered FALSE for every row, the
// DATE comparison answered 0 rows for '0000-01-01' and 22008 for a 22009
// zone, and over an empty table or under a conjunct no row passes neither
// type raised.
func TestArcTCTimestampTextComparisonEveryArm(t *testing.T) {
	tcGate(t, func(c tcCell) bool { return c.params == nil })
}

// TestArcTCBoundParameterEveryDoor: `ts = $1` / `d = $1` with the parameter
// declared unknown (OID 0), text (25) and the column's own type (1114 /
// 1082), text format, over the issue's subset, on the five doors. At
// 93e4804e a refused text bound to a TIMESTAMP comparison answered the epoch
// row.
func TestArcTCBoundParameterEveryDoor(t *testing.T) {
	tcGate(t, func(c tcCell) bool { return c.params != nil })
}

func tcGate(t *testing.T, keep func(tcCell) bool) {
	if testing.Short() {
		t.Skip("-short: five doors over pgwire")
	}
	ctx := context.Background()
	var cells []tcCell
	for _, c := range tcCells() {
		if keep(c) {
			cells = append(cells, c)
		}
	}
	all := tcPins(t)
	pins := map[string]tcPin{}
	for _, c := range tcCells() {
		if p, ok := all[c.name]; ok && keep(c) {
			pins[c.name] = p
		}
	}
	for name, p := range all {
		if _, ok := pins[name]; !ok && !tcGenerated(name) {
			pins[name] = p // stale: reported below
		}
	}
	if dsn := os.Getenv("WADJET_PG_DSN"); dsn != "" {
		pg, err := pgconn.Connect(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer pg.Close(ctx)
		pre := append([]string{"SET statement_timeout = '30s'", "SET TimeZone = 'UTC'", "SET DateStyle = 'ISO, MDY'",
			"DROP SCHEMA IF EXISTS tcarc CASCADE", "CREATE SCHEMA tcarc", "SET search_path = tcarc"}, tcPGFixture()...)
		for _, s := range pre {
			if _, err := pg.Exec(ctx, s).ReadAll(); err != nil {
				t.Fatalf("%s: %v", s, err)
			}
		}
		var dump []string
		for _, c := range cells {
			got := tcRun(ctx, pg, c)
			dump = append(dump, c.name+"\t"+got)
			if p, ok := pins[c.name]; ok && p.pg != got {
				t.Errorf("PostgreSQL %s answered %s, pinned %s", c.name, got, p.pg)
			}
		}
		if f := os.Getenv("TC_PG_DUMP"); f != "" {
			if err := os.WriteFile(f+"."+t.Name(), []byte(strings.Join(dump, "\n")+"\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			return
		}
	}

	serve := func(db *wadjet.DB, c *Coordinator) string {
		srv := pgwire.NewServer(db, pgwire.Config{}, nil)
		if c != nil {
			srv.SetRouter(NewQueryRouter(c))
		}
		if err := srv.Start("127.0.0.1:0"); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { srv.Shutdown() })
		return srv.Addr()
	}
	stand := func(wcfg func(*worker.Config), opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, tcTables())
		return tmdCoordinatorWithWorkers(t, ctx, infra, wcfg, opts...)
	}
	type door struct{ name, addr string }
	doors := []door{
		{"single", serve(tcStandalone(t, ctx, 0), nil)},
		{"spilled512k", serve(tcStandalone(t, ctx, 512*1024), nil)},
		{"dag", serve(tcStandalone(t, ctx, 0), stand(nil))},
		{"dag-shuffled", serve(tcStandalone(t, ctx, 0), stand(nil, func(c *Config) { c.BroadcastBytesOverride = 1 }))},
		{"dag-morsel4", serve(tcStandalone(t, ctx, 0), stand(func(w *worker.Config) { w.MorselWorkers = 4 }))},
	}
	seen := map[string]bool{}
	dumpPrefix := os.Getenv("TC_ENGINE_DUMP")
	for _, d := range doors {
		t.Run(d.name, func(t *testing.T) {
			conn, err := pgconn.Connect(ctx, "postgres://wadjet@"+d.addr+"/test?sslmode=disable")
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close(ctx)
			var dump []string
			for _, c := range cells {
				got := tcRun(ctx, conn, c)
				dump = append(dump, c.name+"\t"+got)
				if dumpPrefix != "" {
					continue
				}
				p, ok := pins[c.name]
				if !ok {
					t.Errorf("%s: no pinned PostgreSQL answer (engine %s)", c.name, got)
					continue
				}
				seen[c.name] = true
				want, why := p.pg, ""
				if p.kept != "" {
					want, why = p.kept, "; kept: "+p.row
				}
				if w, r := tcDoorPin(d.name, c, want); w != "" {
					want, why = w, "; pinned: "+r
				}
				if got != want {
					t.Errorf("%s on %s: %s\n  got  %s\n  want %s (PostgreSQL 17.11 %s%s)", c.name, d.name, c.sql, got, want, p.pg, why)
				}
			}
			if dumpPrefix != "" {
				_ = os.WriteFile(dumpPrefix+"."+strings.ReplaceAll(t.Name(), "/", ".")+".tsv", []byte(strings.Join(dump, "\n")+"\n"), 0o644)
			}
		})
	}
	if dumpPrefix != "" {
		return
	}
	var stale []string
	for name := range pins {
		if !seen[name] {
			stale = append(stale, name)
		}
	}
	sort.Strings(stale)
	for _, name := range stale {
		t.Errorf("pinned cell %q is no longer generated", name)
	}
}

func tcGenerated(name string) bool {
	for _, c := range tcCells() {
		if c.name == name {
			return true
		}
	}
	return false
}
