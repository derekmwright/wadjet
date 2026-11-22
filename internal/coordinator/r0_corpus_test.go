// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/queryroute"
	"github.com/derekmwright/wadjet/internal/server/pgwire"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// The R0 corpus (epic #1678, ADR-0051): the PostgreSQL counterexample corpus
// of reference-preserving stage emission, on six arms over the WIRE, compared
// with PostgreSQL 17.11's exact answer (column names, declared type OIDs and
// typmods, every value's text, row order where the statement orders it, and
// the SQLSTATE when PostgreSQL raises), and each cell's verdict pinned.
//
// The pins (testdata/r0_corpus_pins.tsv) are the BASE verdicts measured at the
// commit that introduced them. A pin of "wrong" is a recorded known defect on
// #1678, not an acceptance. The test fails when any cell's verdict differs
// from its pin in EITHER direction: a stage that repairs a cell re-pins it,
// with the reason, and nothing updates itself (R0_PINS_WRITE writes a new pin
// file for review; it never edits the committed one).
//
// The arms: single and spilled512k (the embedded engine, the second with a
// 512 KiB budget), dag, dag-shuffled (every build through an exchange) and
// dag-morsel4 (the stage DAG), and localD (the stage DAG's own D executed
// single-process, r0_locald_test.go). A DAG arm whose statement ROUTED to the
// bound plan L is reported as routed-right / routed-wrong: its answer is L's,
// not evidence about the DAG.
//
// Run modes (all skip under -short):
//   - default: compare every cell with testdata/r0_corpus_pg17.tsv and the pins;
//   - WADJET_PG_DSN + R0_PG_WRITE=1: TestR0CorpusMeasurePostgres loads the
//     fixtures into PostgreSQL and writes its answers as r0_corpus_pg17.tsv;
//   - R0_PINS_WRITE=1: write each fixture's measured verdicts as
//     r0_corpus_pins_<fixture>.tsv (pin rows, no header);
//   - R0_REPORT=1: write the differential report (deliverable 3) as
//     r0_report_<fixture>.tsv.
//
// Every writer switch (R0_PG_WRITE, R0_PINS_WRITE, R0_FIXTURE_WRITE,
// R0_COST_WRITE and the report switches) takes NO path: it is 1 or unset, and
// any other value fails the test before it measures. The test writes into a
// fresh directory it makes under os.TempDir() and logs the artifact's path
// (r0WriteArtifact). Re-pinning is a deliberate two-step act: run with the
// switch set to 1, review the artifact at the logged path, copy it into
// testdata/ by hand.

type r0Cell struct {
	name, aliases, sources string
	fixtures               []string
	mode, why, sql         string
}

func r0ReadCells(t *testing.T) []r0Cell {
	t.Helper()
	f, err := os.Open("testdata/r0_corpus_cells.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []r0Cell
	seen := map[string]bool{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		p := strings.Split(line, "\t")
		if len(p) != 7 {
			t.Fatalf("malformed cell line %q", line)
		}
		if seen[p[0]] {
			t.Fatalf("duplicate cell %s", p[0])
		}
		seen[p[0]] = true
		switch p[4] {
		case "bag", "ordered", "near_zero":
		default:
			t.Fatalf("cell %s: unknown mode %q", p[0], p[4])
		}
		out = append(out, r0Cell{p[0], p[1], p[2], strings.Split(p[3], ","), p[4], p[5], p[6]})
	}
	return out
}

// --- fixtures ---

func r0Schema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: true},
		{Name: "v", Type: parquet.TypeFloat64, Nullable: true},
	}}
}

// r0Fixture returns a fixture's tables and the DELETEs applied after load.
// own is the first challenge's fixture (unique, duplicate and NULL keys, NULL
// values); dup and uniq are #1207 r7's (tb_d with duplicate or unique keys,
// two 20,000-row tables, tb_bd's even ids deleted).
func r0Fixture(fx string) ([]tmdTable, []string) {
	row := func(id, v any) map[string]any { return map[string]any{"id": id, "v": v} }
	i := func(n int) any { return int64(n) }
	switch fx {
	case "own":
		return []tmdTable{
			{"tb_d", r0Schema(), []map[string]any{row(i(1), 2.0), row(i(1), 5.0), row(i(2), 7.0), row(i(2), nil), row(i(3), 11.0), row(i(4), 13.0), row(nil, 17.0), row(i(5), 19.0)}},
			{"tb_bd", r0Schema(), []map[string]any{row(i(1), 23.0), row(i(1), 29.0), row(i(2), 31.0), row(i(3), nil), row(i(6), 37.0), row(nil, 41.0)}},
			{"tb_big", r0Schema(), []map[string]any{row(i(1), 43.0), row(i(1), 47.0), row(i(2), 53.0), row(i(3), 59.0), row(i(4), 61.0), row(i(7), 67.0), row(nil, nil)}},
		}, nil
	case "dup", "uniq":
		var d []map[string]any
		for id := 1; id <= 10; id++ {
			v1 := float64((id*7)%11) + 0.5
			d = append(d, row(i(id), v1))
			if fx == "dup" {
				d = append(d, row(i(id), v1+1.25))
			}
		}
		if fx == "dup" {
			d = append(d, row(i(11), 3.0), row(i(11), 3.0))
		} else {
			d = append(d, row(i(11), 3.0))
		}
		var big, bd []map[string]any
		for id := 1; id <= 20000; id++ {
			big = append(big, row(i(id), float64((id*37)%101)/4))
			bd = append(bd, row(i(id), float64((id*53)%89)/2))
		}
		return []tmdTable{{"tb_d", r0Schema(), d}, {"tb_big", r0Schema(), big}, {"tb_bd", r0Schema(), bd}},
			[]string{"DELETE FROM tb_bd WHERE id % 2 = 0"}
	}
	panic("unknown fixture " + fx)
}

// r0PGFixture is the same fixture as PostgreSQL DDL in schema r0_<fx>.
func r0PGFixture(fx string) []string {
	tables, deletes := r0Fixture(fx)
	schema := "r0_" + fx
	out := []string{"DROP SCHEMA IF EXISTS " + schema + " CASCADE", "CREATE SCHEMA " + schema, "SET search_path = " + schema}
	lit := func(x any) string {
		switch v := x.(type) {
		case nil:
			return "NULL"
		case int64:
			return strconv.FormatInt(v, 10)
		case float64:
			return strconv.FormatFloat(v, 'g', -1, 64)
		}
		panic(fmt.Sprintf("literal %T", x))
	}
	for _, tb := range tables {
		out = append(out, fmt.Sprintf("CREATE TABLE %s (id bigint, v double precision)", tb.name))
		for lo := 0; lo < len(tb.rows); lo += 1000 {
			var vals []string
			for _, r := range tb.rows[lo:min(lo+1000, len(tb.rows))] {
				vals = append(vals, "("+lit(r["id"])+","+lit(r["v"])+")")
			}
			out = append(out, "INSERT INTO "+tb.name+" VALUES "+strings.Join(vals, ","))
		}
	}
	out = append(out, deletes...)
	return append(out, "ANALYZE")
}

// --- results over the wire ---

// r0Result is one statement's client-observable outcome.
type r0Result struct {
	Err   string      `json:"err,omitempty"`
	Msg   string      `json:"msg,omitempty"`
	Cols  []string    `json:"cols,omitempty"`
	Types []string    `json:"types,omitempty"`
	Rows  [][]*string `json:"rows,omitempty"`
}

func r0TypeName(oid uint32, typmod int32) string {
	names := map[uint32]string{16: "bool", 20: "int8", 21: "int2", 23: "int4", 25: "text", 700: "float4", 701: "float8", 1043: "varchar", 1082: "date", 1114: "timestamp", 1700: "numeric"}
	n, ok := names[oid]
	if !ok {
		return fmt.Sprintf("oid%d/%d", oid, typmod)
	}
	switch {
	case oid == 1700 && typmod >= 4:
		m := typmod - 4
		return fmt.Sprintf("numeric(%d,%d)", (m>>16)&0xffff, m&0xffff)
	case oid == 1043 && typmod >= 4:
		return fmt.Sprintf("varchar(%d)", typmod-4)
	case typmod != -1:
		return fmt.Sprintf("%s/%d", n, typmod)
	}
	return n
}

// r0Query runs sql over conn with the simple query protocol.
//
// The RowDescription is read from the ResultReader BEFORE the rows are
// drained: pgconn's ResultReader.Read copies FieldDescriptions only inside its
// NextRow loop (pgx v5.9.2 pgconn.go:1665-1690), so a zero-row result read
// through Read loses its column names, OIDs and typmods although the server
// sent them (the R0 review's item 2: zero_row_decl was captured as {} on
// PostgreSQL and on every arm). TestR0ZeroRowWireDeclaration holds this.
func r0Query(ctx context.Context, conn *pgconn.PgConn, sql string) r0Result {
	mrr := conn.Exec(ctx, sql)
	var out r0Result
	for mrr.NextResult() {
		rr := mrr.ResultReader()
		fds := append([]pgconn.FieldDescription(nil), rr.FieldDescriptions()...)
		var rows [][]*string
		for rr.NextRow() {
			vals := rr.Values()
			row := make([]*string, len(vals))
			for j, v := range vals {
				if v != nil {
					s := string(v)
					row[j] = &s
				}
			}
			rows = append(rows, row)
		}
		if _, err := rr.Close(); err != nil {
			break
		}
		out = r0Result{Rows: rows}
		for _, fd := range fds {
			out.Cols = append(out.Cols, fd.Name)
			out.Types = append(out.Types, r0TypeName(fd.DataTypeOID, fd.TypeModifier))
		}
	}
	if err := mrr.Close(); err != nil {
		var pe *pgconn.PgError
		if errors.As(err, &pe) {
			return r0Result{Err: pe.Code, Msg: pe.Message}
		}
		return r0Result{Err: "CLIENT", Msg: err.Error()}
	}
	return out
}

func r0RowText(r []*string) string {
	f := make([]string, len(r))
	for i, v := range r {
		if v == nil {
			f[i] = "NULL"
		} else {
			f[i] = strconv.Quote(*v)
		}
	}
	return strings.Join(f, ",")
}

// r0Canon is a result's canonical text, without the message (only the
// SQLSTATE of an error is compared); a bag's rows are sorted.
func r0Canon(r r0Result, mode string) string {
	if r.Err != "" {
		return "ERR " + r.Err
	}
	rows := make([]string, len(r.Rows))
	for i, row := range r.Rows {
		rows[i] = r0RowText(row)
	}
	if mode != "ordered" {
		sort.Strings(rows)
	}
	return fmt.Sprintf("cols=%q types=%q rows=[%s]", r.Cols, r.Types, strings.Join(rows, " ; "))
}

func r0Digest(r r0Result, mode string) string {
	h := sha256.Sum256([]byte(r0Canon(r, mode)))
	return hex.EncodeToString(h[:6])
}

// r0Compare compares an arm's result with PostgreSQL's under the statement's
// mode, and names the first comparison that fails: sqlstate, refused, names,
// types, values, order. "" means equal.
func r0Compare(want, got r0Result, mode string) string {
	if want.Err != "" {
		if got.Err == want.Err {
			return ""
		}
		return "sqlstate"
	}
	if got.Err != "" {
		return "refused"
	}
	if !reflect.DeepEqual(want.Cols, got.Cols) {
		return "names"
	}
	if !reflect.DeepEqual(want.Types, got.Types) {
		return "types"
	}
	if mode == "near_zero" {
		near := func(r r0Result) bool {
			if len(r.Rows) != 1 || len(r.Rows[0]) != 1 || r.Rows[0][0] == nil {
				return false
			}
			x, err := strconv.ParseFloat(*r.Rows[0][0], 64)
			return err == nil && math.Abs(x) < 1e-9
		}
		if near(want) && near(got) {
			return ""
		}
		return "values"
	}
	if r0Canon(want, "bag") != r0Canon(got, "bag") {
		return "values"
	}
	if mode == "ordered" && r0Canon(want, "ordered") != r0Canon(got, "ordered") {
		return "order"
	}
	return ""
}

// r0Verdict is a cell's verdict against PostgreSQL: right, wrong:<digest>,
// refused:<SQLSTATE>, routed-right, routed-wrong:<digest>.
func r0Verdict(want, got r0Result, mode string, routed bool) string {
	why := r0Compare(want, got, mode)
	switch {
	case why == "" && routed:
		return "routed-right"
	case why == "":
		return "right"
	case routed:
		return "routed-wrong:" + r0Digest(got, mode)
	case why == "refused" || why == "sqlstate" && got.Err != "":
		return "refused:" + got.Err
	}
	return "wrong:" + r0Digest(got, mode)
}

// r0FlapMsgClass is the one resource refusal a bounded-flap pin admits: the
// memory budget's own refusal (SQLSTATE 42000, "memory budget exceeded").
var r0FlapMsgClass = regexp.MustCompile(`memory budget exceeded`)

// r0FlapAccepts judges a cell pinned "flap:refused:<SQLSTATE>" (a cell measured
// to alternate between PostgreSQL's answer and one named resource refusal):
// it accepts exactly (a) PostgreSQL's answer (verdict right, class "-") or (b)
// that refusal, with its SQLSTATE and the memory-budget message class, and the
// class the pin names for it. Anything else, a wrong successful answer
// included, is reported ("" means accepted). The R0 review's item 3(d): the
// former unpinned-flap exemption accepted any result.
func r0FlapAccepts(p r0Pin, v, cls, note string, got r0Result) string {
	state := strings.TrimPrefix(p.verdict, "flap:refused:")
	if state == p.verdict || state == "" {
		return "malformed flap pin " + p.verdict
	}
	classes := strings.Split(p.class, "|")
	if len(classes) != 2 || classes[0] != "-" {
		return "malformed flap pin class " + p.class + " (want -|<refusal class>)"
	}
	switch {
	case v == "right" && cls == "-" && note == "-":
		return ""
	case v == "refused:"+state && got.Err == state && r0FlapMsgClass.MatchString(got.Msg) && cls == classes[1] && note == "-":
		return ""
	}
	return fmt.Sprintf("verdict %s class %s (%s %q) is neither PostgreSQL's answer nor the pinned flap's refusal %s/%s %q",
		v, cls, got.Err, got.Msg, state, classes[1], r0FlapMsgClass.String())
}

// --- doors ---

// r0RouteSum totals every route counter the coordinator exposes (methods named
// *Routes or LocalFastPath*, returning int64), so a counter added later is
// counted without editing this list.
func r0RouteSum(c *Coordinator) (int64, string) {
	v := reflect.ValueOf(c)
	t := v.Type()
	var sum int64
	var names []string
	for i := 0; i < t.NumMethod(); i++ {
		m := t.Method(i)
		if !(strings.HasSuffix(m.Name, "Routes") || strings.HasPrefix(m.Name, "LocalFastPath")) {
			continue
		}
		if m.Type.NumIn() != 1 || m.Type.NumOut() != 1 || m.Type.Out(0).Kind() != reflect.Int64 {
			continue
		}
		n := v.Method(i).Call(nil)[0].Int()
		sum += n
		if n != 0 {
			names = append(names, fmt.Sprintf("%s=%d", m.Name, n))
		}
	}
	sort.Strings(names)
	return sum, strings.Join(names, ",")
}

// r0Exec is what a recording router saw for the last statement.
type r0Exec struct {
	calls  int
	routed bool
	plan   string
	locald *r0LocalDRecord
}

// r0Router routes pgwire's SELECTs to the coordinator (the DAG arms) or to the
// localD seam, and records each call, so the test can assert the statement
// really reached the coordinator and whether it routed.
type r0Router struct {
	c      *Coordinator
	locald bool
	mu     sync.Mutex
	last   r0Exec
}

func (r *r0Router) ExecuteSQL(ctx context.Context, sql string) (queryroute.Result, error) {
	if r.locald {
		res, rec, err := r0LocalD(r.c, ctx, sql)
		r.mu.Lock()
		r.last.calls++
		r.last.locald = rec
		r.last.plan = rec.DText
		r.mu.Unlock()
		return routedResult{res: res}, err
	}
	before, _ := r0RouteSum(r.c)
	res, err := r.c.ExecuteSQL(ctx, sql)
	after, _ := r0RouteSum(r.c)
	r.mu.Lock()
	r.last.calls++
	r.last.routed = after != before
	if res != nil {
		r.last.plan = r0PlanText(res.Plan)
	}
	r.mu.Unlock()
	return routedResult{res: res}, err
}

func (r *r0Router) EnforcesABAC() bool { return r.c.EnforcesABAC() }

func (r *r0Router) take() r0Exec {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.last
	r.last = r0Exec{}
	return e
}

var r0Arms = []string{"single", "spilled512k", "dag", "dag-shuffled", "dag-morsel4", "localD"}

func r0IsDAGArm(a string) bool { return strings.HasPrefix(a, "dag") }

type r0Door struct {
	name   string
	conn   *pgconn.PgConn
	router *r0Router
}

func r0Embedded(t *testing.T, ctx context.Context, fx string, budget int64) *wadjet.DB {
	t.Helper()
	cfg := wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"}
	if budget > 0 {
		cfg.MemoryBudget = budget
		cfg.SpillDir = t.TempDir()
	}
	db, err := wadjet.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	tables, deletes := r0Fixture(fx)
	for _, tb := range tables {
		if err := db.CreateTable(ctx, tb.name, tb.schema, nil); err != nil {
			t.Fatal(err)
		}
		ing := db.NewIngester(tb.name, tb.schema, nil, ingest.Config{MaxBufferRows: len(tb.rows) + 1, RowGroupSize: 4096})
		if err := ing.Ingest(ctx, tb.rows); err != nil {
			t.Fatal(err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for _, d := range deletes {
		if _, err := db.Execute(ctx, d); err != nil {
			t.Fatalf("%s: %v", d, err)
		}
	}
	return db
}

func r0Cluster(t *testing.T, ctx context.Context, fx string, wcfg func(*worker.Config), opts ...func(*Config)) *Coordinator {
	t.Helper()
	tables, deletes := r0Fixture(fx)
	infra := tmdInfra(t, ctx)
	tmdWriteTableList(t, ctx, infra, nil, tables)
	if len(deletes) > 0 {
		db, err := wadjet.Open(ctx, wadjet.Config{Store: infra.store, Bucket: "test", MetaKV: infra.kv, Logger: infra.logger})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		for _, d := range deletes {
			if _, err := db.Execute(ctx, d); err != nil {
				t.Fatalf("%s: %v", d, err)
			}
		}
	}
	c := tmdCoordinatorWithWorkers(t, ctx, infra, wcfg, opts...)
	if c.localFastPathBytes() != 0 {
		t.Fatalf("fast path not disabled: %d", c.localFastPathBytes())
	}
	return c
}

func r0Doors(t *testing.T, ctx context.Context, fx string) []r0Door {
	t.Helper()
	serve := func(db *wadjet.DB, r *r0Router) *pgconn.PgConn {
		srv := pgwire.NewServer(db, pgwire.Config{}, nil)
		if r != nil {
			srv.SetRouter(r)
		}
		if err := srv.Start("127.0.0.1:0"); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { srv.Shutdown() })
		conn, err := pgconn.Connect(ctx, "postgres://wadjet@"+srv.Addr()+"/test?sslmode=disable")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close(context.Background()) })
		return conn
	}
	single := r0Embedded(t, ctx, fx, 0)
	spilled := r0Embedded(t, ctx, fx, 512*1024)
	dag := &r0Router{c: r0Cluster(t, ctx, fx, nil)}
	shuf := &r0Router{c: r0Cluster(t, ctx, fx, nil, func(c *Config) { c.BroadcastBytesOverride = 1 })}
	morsel := &r0Router{c: r0Cluster(t, ctx, fx, func(w *worker.Config) { w.MorselWorkers = 4 })}
	locald := &r0Router{c: r0Cluster(t, ctx, fx, nil), locald: true}
	// The DAG doors' own DB serves only what pgwire does not route; the
	// recording router asserts every corpus statement was routed.
	return []r0Door{
		{"single", serve(single, nil), nil},
		{"spilled512k", serve(spilled, nil), nil},
		{"dag", serve(single, dag), dag},
		{"dag-shuffled", serve(single, shuf), shuf},
		{"dag-morsel4", serve(single, morsel), morsel},
		{"localD", serve(single, locald), locald},
	}
}

// --- PostgreSQL answers, pins ---

const r0PGPath = "testdata/r0_corpus_pg17.tsv"
const r0PinsPath = "testdata/r0_corpus_pins.tsv"

// r0ReadPG reads fixture<TAB>name<TAB>json.
func r0ReadPG(t *testing.T) map[string]r0Result {
	t.Helper()
	f, err := os.Open(r0PGPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := map[string]r0Result{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<22), 1<<22)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		p := strings.SplitN(line, "\t", 3)
		var r r0Result
		if err := json.Unmarshal([]byte(p[2]), &r); err != nil {
			t.Fatalf("%s: %v", line, err)
		}
		if _, dup := out[p[0]+"|"+p[1]]; dup {
			t.Fatalf("%s: duplicate PostgreSQL answer for %s/%s (a later line would silently override)", r0PGPath, p[0], p[1])
		}
		out[p[0]+"|"+p[1]] = r
	}
	return out
}

type r0Pin struct {
	verdict, class, note string
}

// r0ReadPins reads fixture<TAB>name<TAB>arm<TAB>verdict<TAB>class<TAB>note.
func r0ReadPins(t *testing.T) map[string]r0Pin {
	t.Helper()
	f, err := os.Open(r0PinsPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := map[string]r0Pin{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		p := strings.Split(line, "\t")
		if len(p) != 6 {
			t.Fatalf("malformed pin %q", line)
		}
		if _, dup := out[p[0]+"|"+p[1]+"|"+p[2]]; dup {
			t.Fatalf("%s: duplicate pin for %s/%s/%s (a later line would silently override)", r0PinsPath, p[0], p[1], p[2])
		}
		out[p[0]+"|"+p[1]+"|"+p[2]] = r0Pin{p[3], p[4], p[5]}
	}
	return out
}

// r0ErrorDomain names the statements whose outcome is an error-domain
// question (D4, maintainer's ruling 2026-11-21): r2's 18 and the judge's
// error pairs. Their PostgreSQL result is kept exactly; their eventual
// disposition is undecided.
func r0ErrorDomain(c r0Cell) bool {
	if strings.Contains(c.sources, "r2") {
		return true
	}
	switch c.name {
	case "cte_error_filter", "t2_logical", "t2_composed", "error_project_before_filter", "error_project_after_filter":
		return true
	}
	return false
}

// r0Class tags a non-right cell mechanically (the rules are the pin file's
// header).
func r0Class(c r0Cell, pg r0Result, arm string, got map[string]r0Result, verdicts map[string]string) string {
	v := verdicts[arm]
	if v == "right" || v == "routed-right" {
		return "-"
	}
	allRefuse := true
	for _, a := range r0Arms {
		if got[a].Err == "" || got[a].Err != got[r0Arms[0]].Err {
			allRefuse = false
		}
	}
	if allRefuse && got[arm].Err != pg.Err && got[arm].Err != "22012" {
		return "capability"
	}
	if r0ErrorDomain(c) && (pg.Err == "22012" || got[arm].Err == "22012") {
		return "error-domain"
	}
	if r0SemiAntiResidual[c.name] && (!r0IsDAGArm(arm) || verdicts["localD"] == v) {
		return "executor"
	}
	switch {
	case r0IsDAGArm(arm) && strings.HasPrefix(v, "routed"):
		return "logical"
	case r0IsDAGArm(arm) && verdicts["localD"] == "right":
		return "emission"
	case arm == "spilled512k" && verdicts["single"] == "right":
		return "executor"
	}
	return "logical"
}

// r0SemiAntiResidual names the statements whose single-process answer is
// wrong through the semi/anti join residual, which evaluates its predicate on
// raw typed data with no NULL test (physical/join_sources.go:572-581, :599):
// an EXECUTOR defect, #1681's line, not a logical-plan one (ADR-0051 §5).
var r0SemiAntiResidual = map[string]bool{"semi": true, "anti": true, "semi_rows": true, "anti_rows": true, "semi_base": true}

type r0CellOutcome struct {
	cell     r0Cell
	fx       string
	got      map[string]r0Result
	verdicts map[string]string
	locald   *r0LocalDRecord
	dagPlan  string
}

// r0RunFixture runs every cell of one fixture on the six doors.
func r0RunFixture(t *testing.T, fx string) {
	r0RequireWriterUsage(t, "R0_PINS_WRITE", "R0_REPORT")
	if testing.Short() {
		t.Skip("-short: six arms over the R0 corpus")
	}
	cells := r0ReadCells(t)
	pg := r0ReadPG(t)
	pins := r0ReadPins(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	doors := r0Doors(t, ctx, fx)
	var outcomes []r0CellOutcome
	for _, c := range cells {
		if !containsString(c.fixtures, fx) {
			continue
		}
		want, ok := pg[fx+"|"+c.name]
		if !ok {
			t.Fatalf("%s/%s has no PostgreSQL answer: re-measure (TestR0CorpusMeasurePostgres)", fx, c.name)
		}
		o := r0CellOutcome{cell: c, fx: fx, got: map[string]r0Result{}, verdicts: map[string]string{}}
		var wg sync.WaitGroup
		var mu sync.Mutex
		execs := map[string]r0Exec{}
		for _, d := range doors {
			wg.Add(1)
			go func() {
				defer wg.Done()
				r := r0Query(ctx, d.conn, c.sql)
				var e r0Exec
				if d.router != nil {
					e = d.router.take()
				}
				mu.Lock()
				o.got[d.name] = r
				execs[d.name] = e
				mu.Unlock()
			}()
		}
		wg.Wait()
		for _, d := range doors {
			e := execs[d.name]
			if d.router != nil && e.calls != 1 {
				t.Errorf("%s/%s on %s: the router saw %d calls, want 1 (the statement did not reach the coordinator)", fx, c.name, d.name, e.calls)
			}
			o.verdicts[d.name] = r0Verdict(want, o.got[d.name], c.mode, e.routed)
		}
		o.locald = execs["localD"].locald
		o.dagPlan = execs["dag"].plan
		// The seam's drift guard: localD executed the D the DAG arm planned.
		if o.locald != nil && o.locald.DText != "" && !execs["dag"].routed && o.dagPlan != "" && o.dagPlan != o.locald.DText {
			t.Errorf("%s/%s: localD planned a different D than the dag arm:\nlocalD:\n%s\ndag:\n%s", fx, c.name, o.locald.DText, o.dagPlan)
		}
		// The guard must not go vacuous: a dag arm that answered unrouted
		// reports the plan it ran, so an empty one means SQLResult.Plan
		// disappeared and the comparison above stopped comparing.
		if o.locald != nil && o.locald.DText != "" && !execs["dag"].routed && o.got["dag"].Err == "" && o.dagPlan == "" {
			t.Errorf("%s/%s: the dag arm answered without reporting its plan: the localD drift guard has nothing to compare", fx, c.name)
		}
		if o.locald != nil && o.locald.Frozen != nil && o.locald.BoundRefs != 0 {
			t.Errorf("%s/%s: the plan localD executed carries %d binder stamps: it is not the unbound D", fx, c.name, o.locald.BoundRefs)
		}
		if o.locald != nil && o.locald.Frozen != nil {
			if o.locald.FrozenText != o.locald.DText {
				t.Errorf("%s/%s: the frozen copy of D changed during the run", fx, c.name)
			}
			if len(o.locald.SharedPointers) > 0 {
				t.Errorf("%s/%s: the frozen copy shares state with the executed D: %v", fx, c.name, o.locald.SharedPointers)
			}
		}
		outcomes = append(outcomes, o)
	}
	// Pins.
	var pinLines []string
	for _, o := range outcomes {
		for _, a := range r0Arms {
			key := o.fx + "|" + o.cell.name + "|" + a
			v := o.verdicts[a]
			cls := r0Class(o.cell, pg[o.fx+"|"+o.cell.name], a, o.got, o.verdicts)
			note := "-"
			if a == "localD" && o.locald != nil {
				note = "status=" + o.locald.Status
				if o.locald.DAGRoute != "" {
					note += ";dag-routes"
				}
			}
			pinLines = append(pinLines, strings.Join([]string{o.fx, o.cell.name, a, v, cls, note}, "\t"))
			p, ok := pins[key]
			switch {
			case !ok:
				t.Errorf("%s: no pin (measured %s %s %s)", key, v, cls, note)
			case strings.HasPrefix(p.verdict, "flap:"):
				if why := r0FlapAccepts(p, v, cls, note, o.got[a]); why != "" {
					t.Errorf("%s: %s\n  got  %s\n  want %s (PostgreSQL 17.11)", key, why,
						r0Canon(o.got[a], o.cell.mode), r0Canon(pg[o.fx+"|"+o.cell.name], o.cell.mode))
				} else {
					t.Logf("%s: bounded flap (%s); measured %s %q", key, p.note, v, o.got[a].Msg)
				}
			case p.verdict != v || p.class != cls || p.note != note:
				t.Errorf("%s: verdict %s class %s note %s, pinned %s %s %s\n  got  %s\n  want %s (PostgreSQL 17.11)", key, v, cls, note,
					p.verdict, p.class, p.note, r0Canon(o.got[a], o.cell.mode), r0Canon(pg[o.fx+"|"+o.cell.name], o.cell.mode))
			}
		}
	}
	r0WriteArtifact(t, "R0_PINS_WRITE", "r0_corpus_pins_"+fx+".tsv", strings.Join(pinLines, "\n")+"\n")
	if r0Writer(t, "R0_REPORT") {
		r0WriteArtifact(t, "R0_REPORT", "r0_report_"+fx+".tsv", r0Report(outcomes, pg))
	}
}

func containsString(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

// R0's writer switches (epic #1678, ADR-0051; the third independent review of
// R0, section B). A writer takes NO path: the caller only turns it on (=1) and
// the test chooses the destination, a fresh uniquely named directory it makes
// under os.TempDir() with a fixed file name inside, created O_CREATE|O_EXCL,
// and logs the full path. No spelling of any path (relative, absolute,
// symbolic or hard link, a link followed by "..") is accepted, so no existing
// file, committed or not, is ever opened for writing. Re-pinning is a
// deliberate two-step act: run with the switch set to 1, review the artifact
// at the logged path, copy it into testdata/ by hand.

// r0WriterUsage is the refusal's explanation of the switch's usage.
const r0WriterUsage = "an R0 writer takes no path; set it to 1 (or leave it unset). " +
	"With %s=1 the test writes its artifact into a fresh directory it creates under os.TempDir() " +
	"and logs the full path; review the artifact there, then copy it into testdata/ by hand"

// r0WriterOn reports whether writer switch name is on: unset or empty is off,
// "1" is on, and any other value (a path left over from the old usage, "true",
// "0") is refused with the usage, never read as a path or as off.
func r0WriterOn(name string) (bool, error) {
	switch v := os.Getenv(name); v {
	case "":
		return false, nil
	case "1":
		return true, nil
	default:
		return false, fmt.Errorf("R0 writer switch %s=%q refused: %s", name, v, fmt.Sprintf(r0WriterUsage, name))
	}
}

// r0RequireWriterUsage fails the test at its start, before any measurement or
// connection, when any named writer switch holds a value other than 1.
func r0RequireWriterUsage(t *testing.T, names ...string) {
	t.Helper()
	for _, n := range names {
		if _, err := r0WriterOn(n); err != nil {
			t.Fatal(err)
		}
	}
}

// r0Writer reports whether switch name is on, failing the test on a value
// other than 1.
func r0Writer(t *testing.T, name string) bool {
	t.Helper()
	on, err := r0WriterOn(name)
	if err != nil {
		t.Fatal(err)
	}
	return on
}

// r0NewArtifact creates a fresh, uniquely named directory under os.TempDir()
// and the file named file inside it, O_CREATE|O_EXCL: it can only create a new
// file, never open an existing one. It returns the open file and its absolute
// path.
func r0NewArtifact(name, file string) (*os.File, string, error) {
	dir, err := os.MkdirTemp("", "wadjet-r0-"+strings.ToLower(name)+"-")
	if err != nil {
		return nil, "", fmt.Errorf("%s: making the artifact directory: %w", name, err)
	}
	path, err := filepath.Abs(filepath.Join(dir, file))
	if err != nil {
		return nil, "", fmt.Errorf("%s: %w", name, err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, "", fmt.Errorf("%s: creating the artifact: %w", name, err)
	}
	return f, path, nil
}

// r0WriteArtifact writes s as switch name's artifact when the switch is on: a
// new file named file in a fresh directory (r0NewArtifact), its path logged.
func r0WriteArtifact(t *testing.T, name, file, s string) {
	t.Helper()
	if !r0Writer(t, name) {
		return
	}
	f, path, err := r0NewArtifact(name, file)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(s); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("%s: wrote the artifact %s (review it, then copy it into testdata/ by hand)", name, path)
}

// r0DiffClass is ADR-0051 §4.2's classification of one DAG arm against
// localD, both judged against PostgreSQL.
func r0DiffClass(dag, locald string, ldStatus string, dagDigest, ldDigest string) string {
	switch {
	case strings.HasPrefix(dag, "routed"):
		return "routed"
	case ldStatus != "established":
		return "localD-not-established(" + ldStatus + ")"
	case dag == "right" && locald == "right":
		return "agree"
	case dag != "right" && locald == "right":
		return "emission"
	case dag == "right" && locald != "right":
		return "dag-right-localD-wrong"
	case dagDigest == ldDigest:
		return "logical(same answer)"
	}
	return "both(different answers)"
}

func r0Report(outcomes []r0CellOutcome, pg map[string]r0Result) string {
	var b strings.Builder
	for _, o := range outcomes {
		st := "-"
		if o.locald != nil {
			st = o.locald.Status
		}
		ld := o.verdicts["localD"]
		var classes []string
		for _, a := range []string{"dag", "dag-shuffled", "dag-morsel4"} {
			classes = append(classes, a+"="+r0DiffClass(o.verdicts[a], ld, st,
				r0Digest(o.got[a], o.cell.mode), r0Digest(o.got["localD"], o.cell.mode)))
		}
		var verdicts []string
		for _, a := range r0Arms {
			verdicts = append(verdicts, a+"="+o.verdicts[a])
		}
		flat := strings.NewReplacer("\t", " ", "\n", " ")
		var msgs []string
		wrong := false
		for _, a := range r0Arms {
			r := o.got[a]
			switch {
			case r.Err != "":
				msgs = append(msgs, a+": "+r.Err+" "+flat.Replace(r.Msg))
			case o.verdicts[a] != "right" && o.verdicts[a] != "routed-right":
				msgs = append(msgs, a+": "+r0Canon(r, o.cell.mode))
			}
			wrong = wrong || o.verdicts[a] != "right"
		}
		if wrong {
			msgs = append(msgs, "PostgreSQL: "+r0Canon(pg[o.fx+"|"+o.cell.name], o.cell.mode))
		}
		fmt.Fprintf(&b, "corpus\t%s\t%s\tlocalD-status=%s\t%s\t%s\t%s\n", o.fx, o.cell.name, st,
			strings.Join(verdicts, " "), strings.Join(classes, " "), strings.Join(msgs, " | "))
	}
	return b.String()
}

func TestR0CorpusOwn(t *testing.T)  { r0RunFixture(t, "own") }
func TestR0CorpusUniq(t *testing.T) { r0RunFixture(t, "uniq") }
func TestR0CorpusDup(t *testing.T)  { r0RunFixture(t, "dup") }

// TestR0CorpusMeasurePostgres measures every cell on PostgreSQL (WADJET_PG_DSN)
// over the fixtures above and writes R0_PG_WRITE in r0_corpus_pg17.tsv's
// format, with the server's version, locale and collation in its header.
func TestR0CorpusMeasurePostgres(t *testing.T) {
	r0RequireWriterUsage(t, "R0_PG_WRITE") // before connecting: a writer takes no path
	dsn := os.Getenv("WADJET_PG_DSN")
	if dsn == "" || !r0Writer(t, "R0_PG_WRITE") {
		t.Skip("WADJET_PG_DSN unset or R0_PG_WRITE not 1")
	}
	ctx := context.Background()
	conn, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	one := func(sql string) string {
		r := r0Query(ctx, conn, sql)
		if r.Err != "" || len(r.Rows) != 1 || r.Rows[0][0] == nil {
			t.Fatalf("%s: %+v", sql, r)
		}
		return *r.Rows[0][0]
	}
	var b strings.Builder
	fmt.Fprintf(&b, "# PostgreSQL answers for testdata/r0_corpus_cells.tsv, measured by TestR0CorpusMeasurePostgres.\n")
	fmt.Fprintf(&b, "# server_version=%s datcollate=%s datctype=%s server_encoding=%s (collation of the text columns: none; the fixture has only bigint and double precision)\n",
		one("SHOW server_version"), one("SELECT datcollate FROM pg_database WHERE datname = current_database()"),
		one("SELECT datctype FROM pg_database WHERE datname = current_database()"), one("SHOW server_encoding"))
	fmt.Fprintf(&b, "# version() = %s\n", one("SELECT version()"))
	fmt.Fprintf(&b, "# fixture<TAB>name<TAB>result (cols, declared types as name(typmod), rows as text, or err SQLSTATE + message)\n")
	cells := r0ReadCells(t)
	for _, fx := range []string{"own", "uniq", "dup"} {
		for _, s := range r0PGFixture(fx) {
			if r := r0Query(ctx, conn, s); r.Err != "" {
				t.Fatalf("%s: %s %s", s, r.Err, r.Msg)
			}
		}
		for _, c := range cells {
			if !containsString(c.fixtures, fx) {
				continue
			}
			r := r0Query(ctx, conn, c.sql)
			j, _ := json.Marshal(r)
			fmt.Fprintf(&b, "%s\t%s\t%s\n", fx, c.name, j)
		}
	}
	r0WriteArtifact(t, "R0_PG_WRITE", "r0_corpus_pg17.tsv", b.String())
}

// TestR0ComparatorRejectsEachDifference is the comparator's own negative
// control: each of the five comparisons (names, declared types, value text,
// order where the mode orders, SQLSTATE) fails on a doctored expected answer.
func TestR0ComparatorRejectsEachDifference(t *testing.T) {
	s := func(x string) *string { return &x }
	base := r0Result{Cols: []string{"id", "r"}, Types: []string{"int8", "numeric(12,2)"},
		Rows: [][]*string{{s("1"), s("1.00")}, {s("2"), nil}}}
	if why := r0Compare(base, base, "ordered"); why != "" {
		t.Fatalf("identical results compare %q", why)
	}
	swapped := base
	swapped.Rows = [][]*string{base.Rows[1], base.Rows[0]}
	if why := r0Compare(base, swapped, "bag"); why != "" {
		t.Fatalf("a permuted bag compares %q", why)
	}
	doctor := func(f func(*r0Result)) r0Result {
		j, _ := json.Marshal(base)
		var r r0Result
		_ = json.Unmarshal(j, &r)
		f(&r)
		return r
	}
	cases := []struct {
		name, mode, want string
		got              r0Result
		expected         r0Result
	}{
		// Multiplicity and width (the R0 review's item 4): a bag counts
		// duplicates, and a row or column more or fewer is a difference.
		{"duplicate row", "bag", "values", base, doctor(func(r *r0Result) { r.Rows = append(r.Rows, r.Rows[0]) })},
		{"missing row", "bag", "values", base, doctor(func(r *r0Result) { r.Rows = r.Rows[:1] })},
		{"extra column", "bag", "names", base, doctor(func(r *r0Result) { r.Cols = append(r.Cols, "extra") })},
		{"missing column", "bag", "names", base, doctor(func(r *r0Result) { r.Cols = r.Cols[:1] })},
		{"missing cell", "bag", "values", base, doctor(func(r *r0Result) { r.Rows[0] = r.Rows[0][:1] })},
		{"names", "bag", "names", base, doctor(func(r *r0Result) { r.Cols[1] = "z" })},
		{"types", "bag", "types", base, doctor(func(r *r0Result) { r.Types[1] = "numeric(12,3)" })},
		{"value text (display scale)", "bag", "values", base, doctor(func(r *r0Result) { r.Rows[0][1] = s("1.0") })},
		{"NULL versus a value", "bag", "values", base, doctor(func(r *r0Result) { r.Rows[1][1] = s("0.00") })},
		{"order", "ordered", "order", swapped, base},
		{"sqlstate", "bag", "sqlstate", r0Result{Err: "22012"}, r0Result{Err: "22003"}},
		{"sqlstate vs rows", "bag", "sqlstate", base, r0Result{Err: "22012"}},
		{"refused", "bag", "refused", r0Result{Err: "0A000"}, base},
	}
	for _, c := range cases {
		if why := r0Compare(c.expected, c.got, c.mode); why != c.want {
			t.Errorf("%s: compare says %q, want %q", c.name, why, c.want)
		}
	}
	if v := r0Verdict(base, swapped, "ordered", false); !strings.HasPrefix(v, "wrong:") {
		t.Errorf("a permuted ordered answer is %q, want wrong", v)
	}
	if v := r0Verdict(base, base, "ordered", true); v != "routed-right" {
		t.Errorf("a routed right answer is %q", v)
	}
}
