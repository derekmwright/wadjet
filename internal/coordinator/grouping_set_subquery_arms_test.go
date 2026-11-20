// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// A SUBQUERY WHOSE BODY GROUPS BY ROLLUP, CUBE OR GROUPING SETS, IN EVERY
// POSITION A SUBQUERY CAN TAKE, ON ALL FIVE ARMS (#1700).
//
// The per-row re-run of a correlated subquery rebuilds the body's text from
// its parts (plansql.RebuildSQLForRerun), and the rebuild rendered the GROUP
// BY key list as a plain GROUP BY, dropping the sets. This table is the
// measurement: grouping construct x subquery position x outer input, over a
// fixture where a construct's rows and the plain GROUP BY's differ in a way
// the position can show (ROLLUP adds a grand-total row, and over an outer row
// that matches nothing it still answers one row where GROUP BY answers none).
// Every want is PostgreSQL 17.11's (testdata/grouping_set_subquery_pg17.tsv,
// regenerated with GS_PG_DSN=<dsn> -run TestGroupingSetSubqueryGenerate). A
// RESIDUAL cell is pinned at this engine's answer in gsKept.
//
//	gs_i: k int | a int | b int   (1,10,100) (1,10,200) (1,20,100) (2,30,300)
//	gs_o: k int                   1, 2  (rows that match)
//	gs_n: k int                   3, 4  (rows that match nothing)

func gsTables() []tmdTable {
	i32 := func(v int) any { return int32(v) }
	iSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt32, Nullable: true},
		{Name: "a", Type: parquet.TypeInt32, Nullable: true},
		{Name: "b", Type: parquet.TypeInt32, Nullable: true},
	}}
	kSchema := parquet.Schema{Columns: []parquet.Column{{Name: "k", Type: parquet.TypeInt32, Nullable: true}}}
	var iRows []map[string]any
	for _, r := range [][3]int{{1, 10, 100}, {1, 10, 200}, {1, 20, 100}, {2, 30, 300}} {
		iRows = append(iRows, map[string]any{"k": i32(r[0]), "a": i32(r[1]), "b": i32(r[2])})
	}
	return []tmdTable{
		{"gs_i", iSchema, iRows},
		{"gs_o", kSchema, []map[string]any{{"k": i32(1)}, {"k": i32(2)}}},
		{"gs_n", kSchema, []map[string]any{{"k": i32(3)}, {"k": i32(4)}}},
	}
}

const gsPGFixture = `DROP TABLE IF EXISTS gs_i; DROP TABLE IF EXISTS gs_o; DROP TABLE IF EXISTS gs_n;
CREATE TABLE gs_i (k integer, a integer, b integer);
CREATE TABLE gs_o (k integer);
CREATE TABLE gs_n (k integer);
INSERT INTO gs_i VALUES (1,10,100),(1,10,200),(1,20,100),(2,30,300);
INSERT INTO gs_o VALUES (1),(2);
INSERT INTO gs_n VALUES (3),(4);`

type gsCell struct{ name, sql string }

// gsCells is the enumeration: grouping construct x position x outer input.
func gsCells() []gsCell {
	forms := []struct {
		name, items, grp string
		single           bool // one output column: the body can stand as a scalar / IN operand itself
	}{
		{"plain", "count(*) AS c", "a", true},
		{"nogroup", "count(*) AS c", "", true},
		{"rollupA", "count(*) AS c", "ROLLUP (a)", true},
		{"rollupAB", "count(*) AS c", "ROLLUP (a, b)", true},
		{"cubeAB", "count(*) AS c", "CUBE (a, b)", true},
		{"setsABE", "count(*) AS c", "GROUPING SETS ((a), (b), ())", true},
		{"setsEE", "count(*) AS c", "GROUPING SETS ((), ())", true},
		{"groupingFn", "GROUPING(a) AS g, count(*) AS c", "ROLLUP (a)", false},
		{"rollupOrdinal", "a, count(*) AS c", "ROLLUP (1)", false},
		{"rollupAlias", "a AS aa, count(*) AS c", "ROLLUP (aa)", false},
	}
	inputs := []struct{ name, outer, key, probe, slack string }{
		{"match", "gs_o", "1", "3", "0"},
		{"nomatch", "gs_n", "3", "0", "3"},
	}
	type pos struct {
		name, tmpl string
		singleOnly bool
	}
	// {B} the correlated body, {C} the body on a constant key, {O} the outer
	// table, {P} a probe value, {D} a slack that lets a nomatch row compare.
	positions := []pos{
		{"scalarCount", "SELECT o.k, (SELECT count(*) FROM ({B}) s) AS n FROM {O} o", false},
		{"scalarSum", "SELECT o.k, (SELECT sum(c) FROM ({B}) s) AS n FROM {O} o", false},
		{"scalarDirect", "SELECT o.k, ({B} ORDER BY c DESC LIMIT 1) AS n FROM {O} o", true},
		{"existsSelect", "SELECT o.k, EXISTS ({B} HAVING count(*) = 0 OR count(*) >= 3) AS e FROM {O} o", false},
		{"existsWhere", "SELECT o.k FROM {O} o WHERE EXISTS ({B} HAVING count(*) = 0 OR count(*) >= 3)", false},
		{"inDirect", "SELECT o.k, {P} IN ({B}) AS m FROM {O} o", true},
		{"inWhere", "SELECT o.k FROM {O} o WHERE {P} IN (SELECT s.c FROM ({B}) s)", false},
		{"whereCompare", "SELECT o.k FROM {O} o WHERE {P} = ({B} ORDER BY c DESC LIMIT 1)", true},
		{"lateral", "SELECT o.k, s.c FROM {O} o, LATERAL ({B}) s", false},
		{"lateralLeft", "SELECT o.k, s.c FROM {O} o LEFT JOIN LATERAL ({B}) s ON true", false},
		{"uncorrScalar", "SELECT (SELECT count(*) FROM ({C}) s) AS n, (SELECT sum(c) FROM ({C}) s) AS m", false},
		{"derived", "SELECT s.c FROM ({C}) s", false},
		{"cteTwice", "WITH s AS ({C}) SELECT (SELECT count(*) FROM s) AS n, (SELECT sum(c) FROM s) AS m", false},
		{"cteCorrelated", "WITH s AS ({C}) SELECT o.k, (SELECT count(*) FROM s WHERE s.c + {D} >= o.k) AS n FROM {O} o", false},
	}
	var out []gsCell
	for _, f := range forms {
		for _, p := range positions {
			if p.singleOnly && !f.single {
				continue
			}
			for _, in := range inputs {
				body := func(cond string) string {
					s := "SELECT " + f.items + " FROM gs_i x WHERE " + cond
					if f.grp != "" {
						s += " GROUP BY " + f.grp
					}
					return s
				}
				sql := strings.NewReplacer(
					"{B}", body("x.k = o.k"), "{C}", body("x.k = "+in.key),
					"{O}", in.outer, "{P}", in.probe, "{D}", in.slack,
				).Replace(p.tmpl)
				out = append(out, gsCell{f.name + "/" + p.name + "/" + in.name, sql})
			}
		}
	}
	return out
}

// gsRender is an answer as sorted rows joined by " | ", NULL spelled out,
// (none) for no rows.
func gsRender(rows [][]any) string {
	if len(rows) == 0 {
		return "(none)"
	}
	out := make([]string, len(rows))
	for i, r := range rows {
		parts := make([]string, len(r))
		for j, v := range r {
			switch x := v.(type) {
			case nil:
				parts[j] = "NULL"
			case bool:
				// t / f, PostgreSQL's text spelling and ssRender's.
				parts[j] = map[bool]string{true: "t", false: "f"}[x]
			default:
				parts[j] = fmt.Sprint(v)
			}
		}
		out[i] = strings.Join(parts, ",")
	}
	sort.Strings(out)
	return strings.Join(out, " | ")
}

// TestGroupingSetSubqueryGenerate measures PostgreSQL 17.11 for every cell
// (GS_PG_DSN=postgres://...) and writes the answer table.
func TestGroupingSetSubqueryGenerate(t *testing.T) {
	dsn := os.Getenv("GS_PG_DSN")
	if dsn == "" {
		t.Skip("GS_PG_DSN unset")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "SET statement_timeout = '30s'"); err != nil {
		t.Fatal(err)
	}
	for _, stmt := range strings.Split(gsPGFixture, ";") {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	var b strings.Builder
	b.WriteString("# name\tPostgreSQL 17.11 answer (sorted rows joined by \" | \"; ERR <SQLSTATE> for a refusal)\n")
	for _, c := range gsCells() {
		ans := func() string {
			rows, err := conn.Query(ctx, c.sql)
			if err != nil {
				return gsPGErr(err)
			}
			defer rows.Close()
			var all [][]any
			for rows.Next() {
				v, err := rows.Values()
				if err != nil {
					return gsPGErr(err)
				}
				for j := range v {
					// NUMERIC (sum over bigint) as its text.
					if n, ok := v[j].(pgtype.Numeric); ok {
						if n.Valid {
							dv, _ := n.Value()
							v[j] = dv
						} else {
							v[j] = nil
						}
					}
				}
				all = append(all, v)
			}
			if err := rows.Err(); err != nil {
				return gsPGErr(err)
			}
			return gsRender(all)
		}()
		fmt.Fprintf(&b, "%s\t%s\n", c.name, ans)
	}
	if err := os.WriteFile("testdata/grouping_set_subquery_pg17.tsv", []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

func gsPGErr(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return "ERR " + pe.Code
	}
	return "ERR " + err.Error()
}

func gsPGAnswers(t *testing.T) map[string]string {
	t.Helper()
	f, err := os.Open("testdata/grouping_set_subquery_pg17.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := map[string]string{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, want, ok := strings.Cut(line, "\t")
		if !ok {
			t.Fatalf("malformed answer line %q", line)
		}
		out[name] = want
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func gsStandalone(t *testing.T, ctx context.Context) *wadjet.DB {
	t.Helper()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test",
		MemoryBudget: 1 << 20, SpillDir: t.TempDir()})
	if err != nil {
		t.Fatalf("open standalone: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	for _, tbl := range gsTables() {
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
	}
	return db
}

type gsArm struct {
	name string
	run  func(sql string) string
}

func gsArms(t *testing.T, ctx context.Context) []gsArm {
	t.Helper()
	db := gsStandalone(t, ctx)
	single := func(sql string) (out string) {
		defer func() {
			if r := recover(); r != nil {
				out = fmt.Sprintf("PANIC %v", r)
			}
		}()
		res, err := db.Query(ctx, sql)
		if err != nil {
			return "ERR " + sqlerr.StateOf(err)
		}
		var rows [][]any
		for i := range res.Rows {
			rows = append(rows, res.Cells(i))
		}
		return gsRender(rows)
	}
	spilled := func(sql string) string {
		defer exec.ForceAggDrainEvery(exec.ForceAggDrainEvery(1))
		return single(sql)
	}
	stand := func(opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, gsTables())
		return tmdCoordinator(t, ctx, infra, opts...)
	}
	dag := func(c *Coordinator) func(string) string {
		return func(sql string) string {
			got, err := ssRunDAG(ctx, c, sql, false)
			if err != nil {
				return "ERR " + sqlerr.StateOf(err)
			}
			// ssRender's `{classes} rows=N r1 | r2` → gsRender's rows.
			_, rest, _ := strings.Cut(got, "} rows=")
			n, rows, _ := strings.Cut(rest, " ")
			if n == "0" {
				return "(none)"
			}
			return rows
		}
	}
	coord := stand()
	coordB := stand(func(c *Config) { c.BroadcastBytesOverride = 1 })
	infraM := tmdInfra(t, ctx)
	tmdWriteTableList(t, ctx, infraM, nil, gsTables())
	coordM := tmdCoordinatorWithWorkers(t, ctx, infraM, func(w *worker.Config) { w.MorselWorkers = 4 })
	return []gsArm{
		{"single", single}, {"spilled", spilled},
		{"dag", dag(coord)}, {"dag-shuffled", dag(coordB)}, {"dag-morsel4", dag(coordM)},
	}
}

// TestGroupingSetSubqueryEveryArm is the table on five arms. GS_MEASURE=1
// logs every cell instead of asserting.
func TestGroupingSetSubqueryEveryArm(t *testing.T) {
	// GS_ARMS=1 runs it under -short too, for a host whose rules keep this
	// package to -short.
	if testing.Short() && os.Getenv("GS_ARMS") == "" {
		t.Skip("-short: five arms over the grouping-set subquery table (GS_ARMS=1 runs it)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	answers := gsPGAnswers(t)
	arms := gsArms(t, ctx)
	measure := os.Getenv("GS_MEASURE") != ""
	before := exec.ForcedRawRowDrains.Load()
	only := os.Getenv("GS_ONLY")
	for _, c := range gsCells() {
		if only != "" && !strings.Contains(c.name, only) {
			continue
		}
		pg, ok := answers[c.name]
		if !ok {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure the table", c.name)
		}
		want, why := pg, ""
		if k, ok := gsKept[c.name]; ok {
			want, why = k.want, k.why
		}
		for _, arm := range arms {
			if k, ok := gsKept[c.name]; ok && k.dagOnly && !strings.HasPrefix(arm.name, "dag") {
				want, why = pg, ""
			}
			got := arm.run(c.sql)
			if measure {
				status := "RIGHT"
				if got != pg {
					status = "DIFF"
				}
				t.Logf("CELL\t%s\t%s\t%s\tgot=%s\tpg=%s", status, c.name, arm.name, got, pg)
				continue
			}
			if got != want {
				if why != "" {
					t.Errorf("%s [%s]: %s\n got    %s\n pinned %s (a residual: %s; PostgreSQL 17.11 answers %s)",
						c.name, arm.name, c.sql, got, want, why, pg)
				} else {
					t.Errorf("%s [%s]: %s\n got  %s\n want %s (PostgreSQL 17.11)", c.name, arm.name, c.sql, got, want)
				}
			}
		}
	}
	if exec.ForcedRawRowDrains.Load() == before {
		t.Error("no raw-row drain was forced: the spilled arm did not exercise the drain")
	}
}

// gsKept is a cell this engine answers differently from PostgreSQL 17.11 at
// the base and at the fix alike: pinned at this engine's answer so the day it
// changes the pin is revisited. PostgreSQL's answer for each is in
// testdata/grouping_set_subquery_pg17.tsv and is printed beside a pin that
// moves. Every pin holds on all five arms.
var gsKept = map[string]struct {
	dagOnly   bool
	want, why string
}{
	"plain/scalarCount/match":           {want: "ERR 0A000", why: gsWhyDerived},
	"plain/scalarCount/nomatch":         {want: "ERR 0A000", why: gsWhyDerived},
	"plain/scalarSum/match":             {want: "ERR 0A000", why: gsWhyDerived},
	"plain/scalarSum/nomatch":           {want: "ERR 0A000", why: gsWhyDerived},
	"plain/inWhere/match":               {want: "ERR 0A000", why: gsWhyDerived},
	"plain/inWhere/nomatch":             {want: "ERR 0A000", why: gsWhyDerived},
	"nogroup/scalarCount/match":         {want: "ERR 0A000", why: gsWhyDerived},
	"nogroup/scalarCount/nomatch":       {want: "ERR 0A000", why: gsWhyDerived},
	"nogroup/scalarSum/match":           {want: "ERR 0A000", why: gsWhyDerived},
	"nogroup/scalarSum/nomatch":         {want: "ERR 0A000", why: gsWhyDerived},
	"nogroup/inWhere/match":             {want: "ERR 0A000", why: gsWhyDerived},
	"nogroup/inWhere/nomatch":           {want: "ERR 0A000", why: gsWhyDerived},
	"rollupA/scalarCount/match":         {want: "ERR 0A000", why: gsWhyDerived},
	"rollupA/scalarCount/nomatch":       {want: "ERR 0A000", why: gsWhyDerived},
	"rollupA/scalarSum/match":           {want: "ERR 0A000", why: gsWhyDerived},
	"rollupA/scalarSum/nomatch":         {want: "ERR 0A000", why: gsWhyDerived},
	"rollupA/inWhere/match":             {want: "ERR 0A000", why: gsWhyDerived},
	"rollupA/inWhere/nomatch":           {want: "ERR 0A000", why: gsWhyDerived},
	"rollupA/lateral/match":             {want: "(none)", why: gsWhy1702},
	"rollupA/lateral/nomatch":           {want: "(none)", why: gsWhy1702},
	"rollupA/lateralLeft/match":         {want: "1,NULL | 2,NULL", why: gsWhy1702},
	"rollupA/lateralLeft/nomatch":       {want: "3,NULL | 4,NULL", why: gsWhy1702},
	"rollupAB/scalarCount/match":        {want: "ERR 0A000", why: gsWhyDerived},
	"rollupAB/scalarCount/nomatch":      {want: "ERR 0A000", why: gsWhyDerived},
	"rollupAB/scalarSum/match":          {want: "ERR 0A000", why: gsWhyDerived},
	"rollupAB/scalarSum/nomatch":        {want: "ERR 0A000", why: gsWhyDerived},
	"rollupAB/inWhere/match":            {want: "ERR 0A000", why: gsWhyDerived},
	"rollupAB/inWhere/nomatch":          {want: "ERR 0A000", why: gsWhyDerived},
	"rollupAB/lateral/match":            {want: "(none)", why: gsWhy1702},
	"rollupAB/lateral/nomatch":          {want: "(none)", why: gsWhy1702},
	"rollupAB/lateralLeft/match":        {want: "1,NULL | 2,NULL", why: gsWhy1702},
	"rollupAB/lateralLeft/nomatch":      {want: "3,NULL | 4,NULL", why: gsWhy1702},
	"cubeAB/scalarCount/match":          {want: "ERR 0A000", why: gsWhyDerived},
	"cubeAB/scalarCount/nomatch":        {want: "ERR 0A000", why: gsWhyDerived},
	"cubeAB/scalarSum/match":            {want: "ERR 0A000", why: gsWhyDerived},
	"cubeAB/scalarSum/nomatch":          {want: "ERR 0A000", why: gsWhyDerived},
	"cubeAB/inWhere/match":              {want: "ERR 0A000", why: gsWhyDerived},
	"cubeAB/inWhere/nomatch":            {want: "ERR 0A000", why: gsWhyDerived},
	"cubeAB/lateral/match":              {want: "(none)", why: gsWhy1702},
	"cubeAB/lateral/nomatch":            {want: "(none)", why: gsWhy1702},
	"cubeAB/lateralLeft/match":          {want: "1,NULL | 2,NULL", why: gsWhy1702},
	"cubeAB/lateralLeft/nomatch":        {want: "3,NULL | 4,NULL", why: gsWhy1702},
	"setsABE/scalarCount/match":         {want: "ERR 0A000", why: gsWhyDerived},
	"setsABE/scalarCount/nomatch":       {want: "ERR 0A000", why: gsWhyDerived},
	"setsABE/scalarSum/match":           {want: "ERR 0A000", why: gsWhyDerived},
	"setsABE/scalarSum/nomatch":         {want: "ERR 0A000", why: gsWhyDerived},
	"setsABE/inWhere/match":             {want: "ERR 0A000", why: gsWhyDerived},
	"setsABE/inWhere/nomatch":           {want: "ERR 0A000", why: gsWhyDerived},
	"setsABE/lateral/match":             {want: "(none)", why: gsWhy1702},
	"setsABE/lateral/nomatch":           {want: "(none)", why: gsWhy1702},
	"setsABE/lateralLeft/match":         {want: "1,NULL | 2,NULL", why: gsWhy1702},
	"setsABE/lateralLeft/nomatch":       {want: "3,NULL | 4,NULL", why: gsWhy1702},
	"setsEE/scalarCount/match":          {want: "ERR 0A000", why: gsWhyDerived},
	"setsEE/scalarCount/nomatch":        {want: "ERR 0A000", why: gsWhyDerived},
	"setsEE/scalarSum/match":            {want: "ERR 0A000", why: gsWhyDerived},
	"setsEE/scalarSum/nomatch":          {want: "ERR 0A000", why: gsWhyDerived},
	"setsEE/inWhere/match":              {want: "ERR 0A000", why: gsWhyDerived},
	"setsEE/inWhere/nomatch":            {want: "ERR 0A000", why: gsWhyDerived},
	"setsEE/lateral/match":              {want: "1,0 | 2,0", why: gsWhy1702},
	"setsEE/lateral/nomatch":            {want: "3,0 | 4,0", why: gsWhy1702},
	"setsEE/lateralLeft/match":          {want: "1,0 | 2,0", why: gsWhy1702},
	"setsEE/lateralLeft/nomatch":        {want: "3,0 | 4,0", why: gsWhy1702},
	"groupingFn/scalarCount/match":      {want: "ERR 0A000", why: gsWhyDerived},
	"groupingFn/scalarCount/nomatch":    {want: "ERR 0A000", why: gsWhyDerived},
	"groupingFn/scalarSum/match":        {want: "ERR 0A000", why: gsWhyDerived},
	"groupingFn/scalarSum/nomatch":      {want: "ERR 0A000", why: gsWhyDerived},
	"groupingFn/inWhere/match":          {want: "ERR 0A000", why: gsWhyDerived},
	"groupingFn/inWhere/nomatch":        {want: "ERR 0A000", why: gsWhyDerived},
	"groupingFn/lateral/match":          {want: "(none)", why: gsWhy1702},
	"groupingFn/lateral/nomatch":        {want: "(none)", why: gsWhy1702},
	"groupingFn/lateralLeft/match":      {want: "1,NULL | 2,NULL", why: gsWhy1702},
	"groupingFn/lateralLeft/nomatch":    {want: "3,NULL | 4,NULL", why: gsWhy1702},
	"rollupOrdinal/scalarCount/match":   {want: "ERR 0A000", why: gsWhyDerived},
	"rollupOrdinal/scalarCount/nomatch": {want: "ERR 0A000", why: gsWhyDerived},
	"rollupOrdinal/scalarSum/match":     {want: "ERR 0A000", why: gsWhyDerived},
	"rollupOrdinal/scalarSum/nomatch":   {want: "ERR 0A000", why: gsWhyDerived},
	"rollupOrdinal/inWhere/match":       {want: "ERR 0A000", why: gsWhyDerived},
	"rollupOrdinal/inWhere/nomatch":     {want: "ERR 0A000", why: gsWhyDerived},
	"rollupOrdinal/lateral/match":       {want: "(none)", why: gsWhy1702},
	"rollupOrdinal/lateral/nomatch":     {want: "(none)", why: gsWhy1702},
	"rollupOrdinal/lateralLeft/match":   {want: "1,NULL | 2,NULL", why: gsWhy1702},
	"rollupOrdinal/lateralLeft/nomatch": {want: "3,NULL | 4,NULL", why: gsWhy1702},
	"rollupAlias/scalarCount/match":     {want: "ERR 0A000", why: gsWhyDerived},
	"rollupAlias/scalarCount/nomatch":   {want: "ERR 0A000", why: gsWhyDerived},
	"rollupAlias/scalarSum/match":       {want: "ERR 0A000", why: gsWhyDerived},
	"rollupAlias/scalarSum/nomatch":     {want: "ERR 0A000", why: gsWhyDerived},
	"rollupAlias/inWhere/match":         {want: "ERR 0A000", why: gsWhyDerived},
	"rollupAlias/inWhere/nomatch":       {want: "ERR 0A000", why: gsWhyDerived},
	"rollupAlias/lateral/match":         {want: "(none)", why: gsWhy1702},
	"rollupAlias/lateral/nomatch":       {want: "(none)", why: gsWhy1702},
	"rollupAlias/lateralLeft/match":     {want: "1,NULL | 2,NULL", why: gsWhy1702},
	"rollupAlias/lateralLeft/nomatch":   {want: "3,NULL | 4,NULL", why: gsWhy1702},
}

const (
	// The whereCompare cells (#1701) are no longer kept: a grouped body
	// declines the WHERE-comparison decorrelation and a COUNT reads 0 over an
	// empty match (where_compare_scalar_arms_test.go).
	//
	// #1702: LATERAL decorrelation adds the correlation key to the GROUP BY
	// list but not to the grouping sets, so every set emits the key as NULL
	// and the join matches nothing (INNER: no rows; LEFT: NULL pads).
	gsWhy1702 = "#1702, LATERAL decorrelation leaves the correlation key out of the grouping sets"
	// A subquery reading a derived table that holds the correlation is
	// refused with 0A000 for every body, the plain GROUP BY and ungrouped
	// controls included; the renderer is not reached. A loud refusal at the
	// base and at this fix alike, owned by neither #1701 nor #1702.
	gsWhyDerived = "a correlated derived table inside a subquery is refused 0A000 (every body, controls included)"
)
