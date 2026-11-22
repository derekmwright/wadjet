// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/server/pgwire"
	"github.com/derekmwright/wadjet/internal/worker"
)

// R0's guards on its own measurement (epic #1678, ADR-0051), added after the
// independent review of R0 (1678-r0_evidence/codex/REVIEW_R0.md): the zero-row
// declaration survives capture, the bounded flap admits nothing but
// PostgreSQL's answer or its one named refusal, the corpus cannot shrink
// silently, the fixtures Wadjet reads are the rows PostgreSQL read, each
// configured arm engages the condition it is named for, a refusal shared by
// two paths is not an agreement, and the localD seam's copied preamble cannot
// drift from ExecuteSQL's by an added call without this file noticing.

// r0ServeEmbedded serves an embedded database with fixture fx over pgwire.
func r0ServeEmbedded(t *testing.T, ctx context.Context, fx string) *pgconn.PgConn {
	t.Helper()
	srv := pgwire.NewServer(r0Embedded(t, ctx, fx, 0), pgwire.Config{}, nil)
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

func r0CellByName(t *testing.T, name string) r0Cell {
	t.Helper()
	for _, c := range r0ReadCells(t) {
		if c.name == name {
			return c
		}
	}
	t.Fatalf("no corpus cell %s", name)
	return r0Cell{}
}

// TestR0ZeroRowWireDeclaration: a zero-row result's RowDescription (names,
// type OIDs, typmods, column count) is part of the answer (ADR-0051 O2). The
// oracle row keeps PostgreSQL's (client_name numeric(12,2), measured on
// 17.11), the capture keeps the one the wire sends with no row behind it, and
// a zero-row answer differing in any one of the four fails, with no row
// payload to fail on.
func TestR0ZeroRowWireDeclaration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	want := r0ReadPG(t)["own|zero_row_decl"]
	if want.Err != "" || len(want.Rows) != 0 || !reflect.DeepEqual(want.Cols, []string{"client_name"}) || !reflect.DeepEqual(want.Types, []string{"numeric(12,2)"}) {
		t.Fatalf("the oracle row of zero_row_decl is %+v, want PostgreSQL 17.11's RowDescription client_name numeric(12,2) and no row", want)
	}
	cell := r0CellByName(t, "zero_row_decl")
	conn := r0ServeEmbedded(t, ctx, "own")
	got := r0Query(ctx, conn, cell.sql)
	if len(got.Cols) != 1 || len(got.Rows) != 0 {
		t.Fatalf("captured %+v over the wire: the zero-row RowDescription was lost", got)
	}
	if why := r0Compare(want, got, cell.mode); why != "" {
		t.Errorf("zero_row_decl over the wire compares %q: got %+v, want %+v", why, got, want)
	}
	// Real wire answers, each with zero rows, each differing from the oracle
	// in exactly one part of the declaration.
	for _, c := range []struct{ what, sql, why string }{
		{"name", "SELECT CAST(id AS numeric(12,2)) AS other_name FROM tb_d WHERE false", "names"},
		{"type OID", "SELECT CAST(id AS bigint) AS client_name FROM tb_d WHERE false", "types"},
		{"typmod", "SELECT CAST(id AS numeric(12,3)) AS client_name FROM tb_d WHERE false", "types"},
		{"column count", "SELECT CAST(id AS numeric(12,2)) AS client_name, CAST(id AS numeric(12,2)) AS client_name FROM tb_d WHERE false", "names"},
	} {
		g := r0Query(ctx, conn, c.sql)
		if g.Err != "" || len(g.Rows) != 0 || len(g.Cols) == 0 {
			t.Fatalf("%s: %s answered %+v, want a zero-row result with a RowDescription", c.what, c.sql, g)
		}
		if why := r0Compare(want, g, cell.mode); why != c.why {
			t.Errorf("a zero-row answer differing in its %s compares %q, want %q (%+v)", c.what, why, c.why, g)
		}
		if v := r0Verdict(want, g, cell.mode, false); !strings.HasPrefix(v, "wrong:") {
			t.Errorf("a zero-row answer differing in its %s is %q, want wrong", c.what, v)
		}
	}
	// The same four on doctored zero-row results (no wire): the comparator's
	// own control, independent of what the server declares.
	for _, c := range []struct {
		what string
		got  r0Result
		why  string
	}{
		{"name", r0Result{Cols: []string{"z"}, Types: want.Types}, "names"},
		{"type OID", r0Result{Cols: want.Cols, Types: []string{"int8"}}, "types"},
		{"typmod", r0Result{Cols: want.Cols, Types: []string{"numeric(12,3)"}}, "types"},
		{"column count", r0Result{Cols: append(append([]string{}, want.Cols...), "client_name"), Types: append(append([]string{}, want.Types...), "numeric(12,2)")}, "names"},
		{"no declaration", r0Result{}, "names"},
	} {
		if why := r0Compare(want, c.got, cell.mode); why != c.why {
			t.Errorf("doctored zero-row %s compares %q, want %q", c.what, why, c.why)
		}
	}
}

// TestR0FlapSuccessfulAnswerStillMatchesPostgres: each bounded-flap pin admits
// exactly PostgreSQL's answer or its one named resource refusal (42000, the
// memory budget's message class); a wrong successful value, an unrelated
// refusal, or the right SQLSTATE with another message, fails.
func TestR0FlapSuccessfulAnswerStillMatchesPostgres(t *testing.T) {
	pg := r0ReadPG(t)
	cells := map[string]r0Cell{}
	for _, c := range r0ReadCells(t) {
		cells[c.name] = c
	}
	s := func(x string) *string { return &x }
	n := 0
	for key, p := range r0ReadPins(t) {
		if !strings.HasPrefix(p.verdict, "flap:") {
			continue
		}
		n++
		parts := strings.Split(key, "|")
		fx, name, arm := parts[0], parts[1], parts[2]
		c, want := cells[name], pg[fx+"|"+name]
		judge := func(got r0Result) string {
			all := map[string]r0Result{}
			verdicts := map[string]string{}
			for _, a := range r0Arms {
				all[a], verdicts[a] = want, "right"
			}
			all[arm] = got
			verdicts[arm] = r0Verdict(want, got, c.mode, false)
			return r0FlapAccepts(p, verdicts[arm], r0Class(c, want, arm, all, verdicts), "-", got)
		}
		budget := "building physical plan: building hash table: hash join build: query: memory budget exceeded (used=656404, requested=230400, budget=524288)"
		for _, ok := range []struct {
			what string
			got  r0Result
		}{
			{"PostgreSQL's answer", want},
			{"the named memory-budget refusal", r0Result{Err: "42000", Msg: budget}},
		} {
			if why := judge(ok.got); why != "" {
				t.Errorf("%s: %s is rejected: %s", key, ok.what, why)
			}
		}
		wrong := r0Result{Cols: want.Cols, Types: want.Types, Rows: [][]*string{{s("999999")}}}
		for _, bad := range []struct {
			what string
			got  r0Result
		}{
			{"a wrong successful value", wrong},
			{"a wrong declared type", r0Result{Cols: want.Cols, Types: []string{"numeric"}, Rows: want.Rows}},
			{"an unrelated refusal (22012)", r0Result{Err: "22012", Msg: "division by zero"}},
			{"an unrelated refusal (XX000)", r0Result{Err: "XX000", Msg: "internal error: memory budget exceeded"}},
			{"42000 with another message", r0Result{Err: "42000", Msg: "hash join build: unsupported key type"}},
		} {
			if why := judge(bad.got); why == "" {
				t.Errorf("%s: %s is accepted by the flap pin", key, bad.what)
			}
		}
	}
	if n != 2 {
		t.Errorf("%d bounded-flap pins, want the 2 measured (h2_base2_control on spilled512k, uniq and dup)", n)
	}
}

// r0MandatoryNames are the corpus rows ADR-0051 §4.1 and the R0 brief require
// by name, with the alias each must keep.
var r0MandatoryNames = []string{
	"outer_nonstrict_rows", "join2_outer_coalesce", "group_expr", "group_negate", "union_cast_values", "cascade",
	"cascade_reverse", "ctes_limit_set", "S1", "C2L", "pd_two_arm", "window_arm", "gk_outer_expression",
	"gk_outer_having", "window_nested_order", "cte_error_filter", "t2_logical", "t2_composed",
	"error_project_before_filter", "error_project_after_filter", "one_arm_respelling", "within_tautology",
	"within_count", "zero_row_decl", "S2", "S3", "S4", "h2_base2_control", "e_derived_limit", "l_error_over_limit3",
	"e_conj_order_a", "e_cte_twice",
}

var r0MandatoryAliases = map[string]string{"cte_error_filter": "e_cte_once", "S1": "h1_i_cte2", "C2L": "k_left"}

// TestR0CorpusMembershipAndPinCompleteness: the corpus cannot shrink without
// this test failing (a removed mandatory row, alias or source statement), and
// every statement-fixture row has exactly one PostgreSQL answer and every
// cell exactly one pin, with no stale or duplicate entry (r0ReadPG and
// r0ReadPins refuse a duplicate key).
func TestR0CorpusMembershipAndPinCompleteness(t *testing.T) {
	cells := r0ReadCells(t)
	pg := r0ReadPG(t)
	pins := r0ReadPins(t)
	byName := map[string]r0Cell{}
	perSource := map[string]int{}
	rows := 0
	for _, c := range cells {
		byName[c.name] = c
		for _, s := range strings.Split(c.sources, ",") {
			perSource[s]++
		}
		rows += len(c.fixtures)
	}
	for _, n := range r0MandatoryNames {
		if _, ok := byName[n]; !ok {
			t.Errorf("mandatory corpus row %s is missing", n)
		}
	}
	for n, alias := range r0MandatoryAliases {
		if !containsString(strings.Split(byName[n].aliases, ","), alias) {
			t.Errorf("%s lost its alias %s", n, alias)
		}
	}
	// The source statement sets the brief names, each distinct statement once
	// (the deduplicated names live on as aliases).
	wantSources := map[string]int{"r7": 58, "challenge": 54, "r1": 17, "judge": 20, "r2": 18, "judge-formal": 6}
	if !reflect.DeepEqual(perSource, wantSources) {
		t.Errorf("corpus rows per source %v, want %v", perSource, wantSources)
	}
	if len(cells) != 172 || rows != 230 {
		t.Errorf("%d statements on %d statement-fixture rows, want 172 on 230", len(cells), rows)
	}
	wantPG := map[string]bool{}
	wantPins := map[string]bool{}
	for _, c := range cells {
		for _, fx := range c.fixtures {
			wantPG[fx+"|"+c.name] = true
			if _, ok := pg[fx+"|"+c.name]; !ok {
				t.Errorf("%s/%s has no PostgreSQL answer", fx, c.name)
			}
			for _, a := range r0Arms {
				wantPins[fx+"|"+c.name+"|"+a] = true
				if _, ok := pins[fx+"|"+c.name+"|"+a]; !ok {
					t.Errorf("%s/%s/%s has no pin", fx, c.name, a)
				}
			}
		}
	}
	for k := range pg {
		if !wantPG[k] {
			t.Errorf("stale PostgreSQL answer %s (no such statement-fixture row)", k)
		}
	}
	for k, p := range pins {
		if !wantPins[k] {
			t.Errorf("stale pin %s", k)
		}
		if strings.HasPrefix(p.verdict, "flap:") && !strings.HasSuffix(k, "|h2_base2_control|spilled512k") {
			t.Errorf("%s: a bounded-flap pin outside the two measured cells", k)
		}
	}
	if len(pins) != 6*rows || len(pg) != rows {
		t.Errorf("%d pins and %d PostgreSQL answers, want %d and %d", len(pins), len(pg), 6*rows, rows)
	}
}

const r0FixturePGPath = "testdata/r0_fixture_pg17.tsv"

// r0FixtureTables names each fixture's tables.
func r0FixtureTables(fx string) []string {
	tables, _ := r0Fixture(fx)
	var out []string
	for _, tb := range tables {
		out = append(out, tb.name)
	}
	return out
}

// r0FixtureDigest is a table's full content as one line: row count and the
// SHA-256 of its canonical bag text (names, declared types, every value's
// text, duplicates counted).
func r0FixtureDigest(r r0Result) string {
	h := sha256.Sum256([]byte(r0Canon(r, "bag")))
	return fmt.Sprintf("rows=%d sha256=%s %s", len(r.Rows), hex.EncodeToString(h[:]), strings.Join(append(append([]string{}, r.Cols...), r.Types...), ","))
}

// TestR0FixtureRowsMatchPostgres: the rows every arm reads are the rows
// PostgreSQL read. testdata/r0_fixture_pg17.tsv holds each fixture table's
// full content digest as measured on PostgreSQL 17.11 after the same load and
// DELETEs (WADJET_PG_DSN + R0_FIXTURE_WRITE=<path> re-measures it); every one
// of the six arms must read the same bag. With WADJET_PG_DSN set the digests
// are also re-measured live and compared.
func TestR0FixtureRowsMatchPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: six arms per fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	if dsn, out := os.Getenv("WADJET_PG_DSN"), os.Getenv("R0_FIXTURE_WRITE"); dsn != "" {
		conn, err := pgconn.Connect(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close(ctx)
		var b strings.Builder
		b.WriteString("# The R0 fixtures as PostgreSQL holds them after load and DELETEs, measured by TestR0FixtureRowsMatchPostgres\n")
		b.WriteString("# (PostgreSQL 17.11, postgres:17-alpine, initdb --locale=C --encoding=UTF8): fixture/table<TAB>row count, SHA-256 of the canonical bag text, names and types.\n")
		live := map[string]string{}
		for _, fx := range []string{"own", "uniq", "dup"} {
			for _, s := range r0PGFixture(fx) {
				if r := r0Query(ctx, conn, s); r.Err != "" {
					t.Fatalf("%s: %s %s", s, r.Err, r.Msg)
				}
			}
			for _, tb := range r0FixtureTables(fx) {
				d := r0FixtureDigest(r0Query(ctx, conn, "SELECT id, v FROM "+tb))
				live[fx+"/"+tb] = d
				fmt.Fprintf(&b, "%s/%s\t%s\n", fx, tb, d)
			}
		}
		if out != "" {
			if err := os.WriteFile(out, []byte(b.String()), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		for k, d := range live {
			if want := r0ReadKV(t, r0FixturePGPath)[k]; want != d {
				t.Errorf("PostgreSQL now holds %s as %s, the committed digest is %s", k, d, want)
			}
		}
	}
	want := r0ReadKV(t, r0FixturePGPath)
	checked := 0
	for _, fx := range []string{"own", "uniq", "dup"} {
		doors := r0Doors(t, ctx, fx)
		for _, tb := range r0FixtureTables(fx) {
			w, ok := want[fx+"/"+tb]
			if !ok {
				t.Fatalf("%s/%s has no PostgreSQL digest", fx, tb)
			}
			for _, d := range doors {
				got := r0FixtureDigest(r0Query(ctx, d.conn, "SELECT id, v FROM "+tb))
				if d.router != nil {
					d.router.take()
				}
				if got != w {
					t.Errorf("%s/%s on %s: %s, PostgreSQL holds %s", fx, tb, d.name, got, w)
				}
				checked++
			}
		}
	}
	if checked != 54 {
		t.Errorf("%d table-arm bags checked, want 54 (3 fixtures x 3 tables x 6 arms)", checked)
	}
}

// TestR0ConfiguredArmsActuallyEngage: each configured arm engages the
// condition it is named for on a designated statement (uniq/pd_two_arm,
// chosen by measurement: 1678-r0_evidence/r5/engagement_probe.log), and the
// arm it is configured against does not, so a pin on the arm is evidence
// about that condition: spilled512k evicts hash-join partitions
// (exec.JoinPartitionsEvicted) where single does not; dag-morsel4 runs
// breakers morsel-parallel (worker.MorselParallelBreakerRuns) where dag does
// not; dag-shuffled's stage graph repartitions both join inputs where dag's
// broadcasts, and its statement reaches the coordinator unrouted.
func TestR0ConfiguredArmsActuallyEngage(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: six arms")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	cell := r0CellByName(t, "pd_two_arm")
	doors := map[string]r0Door{}
	for _, d := range r0Doors(t, ctx, "uniq") {
		doors[d.name] = d
	}
	run := func(arm string) (r0Result, r0Exec) {
		d := doors[arm]
		r := r0Query(ctx, d.conn, cell.sql)
		var e r0Exec
		if d.router != nil {
			e = d.router.take()
		}
		if r.Err != "" {
			t.Fatalf("%s on %s refused %s %s", cell.name, arm, r.Err, r.Msg)
		}
		return r, e
	}
	delta := func(c interface{ Load() int64 }, arm string) int64 {
		before := c.Load()
		run(arm)
		return c.Load() - before
	}
	if n := delta(&exec.JoinPartitionsEvicted, "spilled512k"); n == 0 {
		t.Errorf("spilled512k: %s evicted no hash-join partition: the 512 KiB budget did not engage a spill", cell.name)
	}
	if n := delta(&exec.JoinPartitionsEvicted, "single"); n != 0 {
		t.Errorf("single: %s evicted %d hash-join partitions with no budget", cell.name, n)
	}
	if n := delta(&worker.MorselParallelBreakerRuns, "dag-morsel4"); n == 0 {
		t.Errorf("dag-morsel4: %s ran no breaker morsel-parallel", cell.name)
	}
	if n := delta(&worker.MorselParallelBreakerRuns, "dag"); n != 0 {
		t.Errorf("dag: %s ran %d breakers morsel-parallel with MorselWorkers unset", cell.name, n)
	}
	count := func(arm, typ string) int {
		_, rec, _ := r0Statement(doors[arm].router.c, ctx, cell.sql, false, false)
		if rec.DAGRoute != "" || rec.Frozen == nil {
			t.Fatalf("%s on %s: planned %v, route %q", cell.name, arm, rec.Frozen != nil, rec.DAGRoute)
		}
		n := 0
		for _, s := range rec.Stages {
			if s.Type == typ {
				n++
			}
		}
		return n
	}
	if n := count("dag-shuffled", "exchange-repartition"); n < 2 {
		t.Errorf("dag-shuffled: %s plans %d repartition exchanges, want both join inputs shuffled", cell.name, n)
	}
	if n := count("dag", "exchange-repartition"); n != 0 {
		t.Errorf("dag: %s plans %d repartition exchanges, want a broadcast build (the control)", cell.name, n)
	}
	if _, e := run("dag-shuffled"); e.calls != 1 || e.routed {
		t.Errorf("dag-shuffled: %s reached the coordinator %d times, routed %v: the shuffled graph did not run", cell.name, e.calls, e.routed)
	}
}

// TestR0TPCHCommonRefusalFails: in the TPC-H differential a refusal the DAG
// and localD share is classified "refused-alike", never "agree", so a query
// that starts refusing on both paths moves its pin.
func TestR0TPCHCommonRefusalFails(t *testing.T) {
	ok := r0Rendered{cols: []string{"n"}, types: []string{"2"}, rows: [][]string{{"1"}}}
	refusal := r0Rendered{err: "ERR 42000 memory budget exceeded"}
	cases := []struct {
		what    string
		dag, ld r0Rendered
		want    string
	}{
		{"equal answers", ok, ok, "agree"},
		{"a refusal shared by both paths", refusal, refusal, "refused-alike"},
		{"one path refuses", refusal, ok, "differ(error)"},
		{"the other path refuses", ok, refusal, "differ(error)"},
		{"two different refusals", refusal, r0Rendered{err: "ERR 22012 division by zero"}, "differ(error)"},
	}
	for _, c := range cases {
		if got := r0TPCHClass(false, "established", c.dag, c.ld, false); got != c.want {
			t.Errorf("%s: classified %s, want %s", c.what, got, c.want)
		}
	}
	if got := r0TPCHClass(true, "established", refusal, refusal, false); got != "routed" {
		t.Errorf("a routed query is %s", got)
	}
}

// r0CallSequence lists, in source order, the names of the calls in function
// fn of file path up to and including the first call named stop.
func r0CallSequence(t *testing.T, path, fn, stop string) []string {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Name.Name != fn || fd.Body == nil {
			continue
		}
		var out []string
		done := false
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			if done {
				return false
			}
			c, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch f := c.Fun.(type) {
			case *ast.SelectorExpr:
				out = append(out, f.Sel.Name)
			case *ast.Ident:
				out = append(out, f.Name)
			}
			done = len(out) > 0 && out[len(out)-1] == stop
			return true
		})
		if !done {
			t.Fatalf("%s: %s never calls %s", path, fn, stop)
		}
		return out
	}
	t.Fatalf("%s: no function %s", path, fn)
	return nil
}

// r0PreambleOmitted are the calls ExecuteSQL makes before planStatement that
// the localD seam deliberately does not copy, each with its reason. A call not
// listed here and not in the seam is drift.
var r0PreambleOmitted = map[string]string{
	"isLeaderOrStandalone":    "leadership: a test cluster is standalone",
	"CurrentLeader":           "leadership",
	"StartSpan":               "telemetry",
	"String":                  "telemetry span attributes",
	"End":                     "telemetry",
	"handleCreateSnapshotSQL": "not a SELECT (the seam reports not-select)",
	"handleCreateAlertSQL":    "not a SELECT",
	"handleDropAlertSQL":      "not a SELECT",
	"handleAlterAlertSQL":     "not a SELECT",
	"EmptyResultColumns":      "the empty-column result guard: an answer-shape guard on the result, not planning state",
	"Error":                   "the empty-column guard's message",
	"Close":                   "the incarnation defer closing a result the seam does not hold yet",
	"statementStep":           "test-observability step markers",
	"Add":                     "the veto refusal counter",
	"build":                   "L for the fast path: the seam plans D, and the fast path is off on every R0 cluster",
	"tryLocalFastPath":        "the fast path (off on every R0 cluster: r0Cluster asserts it)",
	"localFastPathBytes":      "the fast path",
	"finalPlanningError":      "L's build failure on the fast path",
	"Debug":                   "logging on the fast path",
}

// r0PreambleGeneric are standard-library helpers both bodies call for their
// own bookkeeping (errors, clocks, ids, lengths, the recover builtin); they
// carry no planning state and are compared on neither side.
var r0PreambleGeneric = map[string]bool{"recover": true, "Errorf": true, "Done": true, "Err": true, "Now": true, "New": true, "String": true, "len": true, "Since": true}

// r0PreambleDrift compares the two call sequences under the two lists and
// returns the differences ("" when the seam copies every production call in
// order).
func r0PreambleDrift(prod, seam []string) string {
	keep := func(xs []string, drop map[string]string) []string {
		var out []string
		for _, x := range xs {
			if _, ok := drop[x]; !ok {
				out = append(out, x)
			}
		}
		return out
	}
	generic := map[string]string{}
	for k := range r0PreambleGeneric {
		generic[k] = "generic"
	}
	p, s := keep(keep(prod, r0PreambleOmitted), generic), keep(seam, generic)
	if reflect.DeepEqual(p, s) {
		return ""
	}
	return fmt.Sprintf("ExecuteSQL's preamble calls\n  %v\nthe seam's\n  %v", p, s)
}

// TestR0LocalDPreambleParity strengthens the localD seam's drift guard as far
// as a test-only check can: the seam (r0Statement) copies ExecuteSQL's
// preamble, and this test reads both function bodies from source and requires
// the same calls, in the same order, up to planStatement, except the calls
// listed as deliberately omitted (with their reasons) and the generic
// standard-library helpers. Adding a
// planning step to ExecuteSQL's preamble without copying it into the seam
// fails here. What it cannot see (ADR-0051, "Stage R0, as landed"): a changed
// argument or a changed callee body under the same name, and behaviour under
// an enabled authorization provider, a policy reload, a re-created table or
// the ErrInputLost rerun, none of which the R0 fixtures exercise.
func TestR0LocalDPreambleParity(t *testing.T) {
	prod := r0CallSequence(t, "coordinator.go", "ExecuteSQL", "planStatement")
	seam := r0CallSequence(t, "r0_locald_test.go", "r0Statement", "planStatement")
	if d := r0PreambleDrift(prod, seam); d != "" {
		t.Errorf("the localD seam's copied preamble has drifted from ExecuteSQL's:\n%s", d)
	}
	// Its own negative controls: a production call the seam lacks, and a seam
	// call production lacks, are each reported.
	if d := r0PreambleDrift(append(append([]string{}, prod[:len(prod)-1]...), "enforceNewPolicy", "planStatement"), seam); d == "" {
		t.Error("an added production preamble call is not reported")
	}
	if d := r0PreambleDrift(prod, append([]string{"extraSeamStep"}, seam...)); d == "" {
		t.Error("an added seam call is not reported")
	}
	sorted := append([]string{}, prod...)
	sort.Strings(sorted)
	t.Logf("ExecuteSQL preamble: %d calls (%d distinct names)", len(prod), len(r0Uniq(sorted)))
}

func r0Uniq(sorted []string) []string {
	var out []string
	for i, x := range sorted {
		if i == 0 || x != sorted[i-1] {
			out = append(out, x)
		}
	}
	return out
}
