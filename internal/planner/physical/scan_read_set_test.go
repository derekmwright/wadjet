package physical

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A SCAN'S READ SET IS A PLAN-TIME FACT (#776).
//
// `Stage.Columns` on a scan is what the fragment asks the parquet reader for,
// and every model of "what does this stage emit" reads it — `stageEmitted
// Columns`, and through it `emittedThroughPassThrough`, `gatherOutputSources`
// and `stageStreamColumns`. A name in that list that the TABLE does not have
// is therefore not a slow scan, it is a LIE the reachability check, the
// hidden-sort-key pass and the derived-alias passes all believe:
//
//   - `SELECT x.w FROM (SELECT g*3 AS w FROM typemx ORDER BY w LIMIT 5) x`
//     put `w` into typemx's read set, the sort-key pass saw it in the stream
//     and skipped the materialization, and the task failed with
//     `sort: key column "w" does not exist in the input schema` (#807);
//   - a window one level up failed with `column "w" does not exist in the
//     input schema` out of the projection the same phantom let through
//     (#776);
//   - and the worker's own last line of defence, the
//     `parquet projection narrowed: requested columns missing from schema`
//     warning in `worker/stream_source.go`, fires on exactly this — which is
//     why this test is the claim that no PLANNED query reaches it.
//
// The source was `logical.sanitizeScanNeeds`: a derived table's alias BECOMES
// the scan's `TableAlias`, so `x.w` matched the qualifier branch and was kept
// as the bare `w` whether or not the schema had it. The CTE spelling of the
// same query is the control that proved it — `c.w` does not match the alias,
// the name was dropped, and that plan was REFUSED where the derived spelling
// failed loud.
//
// The assertion is over both corpora, because the two find different things:
// TPC-H is breadth over real plans (a regression that widens every scan shows
// there), and the derived-alias corpus is the shapes that MINT a name a table
// does not have.
func TestAScanRequestsOnlyColumnsItsTableHas(t *testing.T) {
	type probe struct {
		name   string
		cat    *catalog.Catalog
		ctx    context.Context
		sql    string
		schema map[string][]string
	}

	tcat, tctx := setupTPCHCatalog(t)
	tpchSchema := map[string][]string{}
	for name, s := range tpchSchemas {
		for _, c := range s.Columns {
			tpchSchema[name] = append(tpchSchema[name], c.Name)
		}
	}
	dcat, dctx := derivedAliasCatalog(t)
	derivedSchema := map[string][]string{}
	for name, s := range derivedAliasSchemas() {
		for _, c := range s.Columns {
			derivedSchema[name] = append(derivedSchema[name], c.Name)
		}
	}

	var probes []probe
	for q := 1; q <= 22; q++ {
		sql, ok := tpchPlanQueryMap[q]
		if !ok {
			continue
		}
		probes = append(probes, probe{fmt.Sprintf("tpch/Q%02d", q), tcat, tctx, sql, tpchSchema})
	}
	for _, c := range derivedAliasReadSetShapes() {
		probes = append(probes, probe{"derived/" + c.name, dcat, dctx, c.sql, derivedSchema})
	}

	// A shape whose PLAN is refused still has a read set, and the refusal is
	// exactly what the phantom used to prevent — so the LOGICAL check runs
	// for every shape and the STAGE check only where a plan came back.
	// Skipping the refused ones is how this gate would pass against the
	// pre-#776 pruner: those are the shapes that mint the name.
	checkedAScanThatCouldCarryOne := 0

	for _, p := range probes {
		t.Run(p.name, func(t *testing.T) {
			lp := optimizedPlanForReadSet(t, p.cat, p.ctx, p.sql)
			n := assertScanNeedsAreDeclared(t, lp, p.schema, p.sql)
			checkedAScanThatCouldCarryOne += n

			stages, err := planStagesForReadSet(t, p.cat, p.ctx, p.sql)
			if err != nil {
				// A refused plan is a legitimate disposition — the
				// coordinator routes the query to its local engine and
				// answers it — and it emits no stage list to check.
				return
			}
			for _, s := range stages {
				if s.Type != StageScan || s.TableName == "" {
					continue
				}
				declared := p.schema[s.TableName]
				if len(declared) == 0 {
					continue
				}
				if phantom := notDeclared(s.Columns, declared); len(phantom) > 0 {
					t.Errorf("scan stage %s over %s requests %v, and the table has none of them.\n"+
						"  table columns: %v\n"+
						"  A read set is what every emitted-set model reads, so a name no file\n"+
						"  has is believed by the reachability check and by both derived-alias\n"+
						"  key passes (#776).\n  SQL: %s",
						s.ID, s.TableName, phantom, declared, p.sql)
				}
			}
		})
	}

	if checkedAScanThatCouldCarryOne == 0 {
		t.Fatal("no corpus shape produced a scan with a known schema and a non-empty " +
			"read set, so this gate cannot fail")
	}
}

// assertScanNeedsAreDeclared checks the LOGICAL half — logical.Node.
// RequiredColumns, which is what physical stage emission copies onto a scan
// stage — and reports how many scans it was able to judge.
func assertScanNeedsAreDeclared(t *testing.T, n *logical.Node, schema map[string][]string, sql string) int {
	t.Helper()
	if n == nil {
		return 0
	}
	checked := 0
	if n.Type == logical.NodeScan && n.TableName != "" && len(n.RequiredColumns) > 0 {
		if declared := schema[n.TableName]; len(declared) > 0 {
			checked++
			if phantom := notDeclared(n.RequiredColumns, declared); len(phantom) > 0 {
				t.Errorf("scan of %s (alias %q) needs %v, and the table has none of them.\n"+
					"  table columns: %v\n"+
					"  A derived table's alias BECOMES the scan's alias, so its own SELECT-list\n"+
					"  alias arrives here qualified and matching — and keeping it writes a name\n"+
					"  no file has into the read set (#776, logical.sanitizeScanNeeds).\n  SQL: %s",
					n.TableName, n.TableAlias, phantom, declared, sql)
			}
		}
	}
	for _, c := range n.Children {
		checked += assertScanNeedsAreDeclared(t, c, schema, sql)
	}
	return checked
}

// notDeclared lists the requested names the table does not have. The row-count
// sentinel is not a column, and a hidden slot is materialized by a projection
// rather than read from a file — the worker's own projection guard gives those
// their derived-column semantics.
func notDeclared(requested, declared []string) []string {
	have := make(map[string]bool, len(declared))
	for _, c := range declared {
		have[strings.ToLower(c)] = true
	}
	var out []string
	for _, c := range requested {
		if c == "" || strings.EqualFold(c, logical.RowCountOnlyColumn) || strings.HasPrefix(c, "__") {
			continue
		}
		if !have[strings.ToLower(stripQualifier(c))] {
			out = append(out, c)
		}
	}
	sort.Strings(out)
	return out
}

// derivedAliasShape is one corpus entry. The first eight reference a derived
// table's own SELECT-list alias QUALIFIED by that derived table's alias — the
// spelling the pruner used to write into the scan below it — and the rest are
// controls that must keep resolving.
type derivedAliasShape struct {
	name string
	sql  string
}

func derivedAliasReadSetShapes() []derivedAliasShape {
	return []derivedAliasShape{
		{"sort_over_computed_alias", `SELECT x.w FROM (SELECT g*3 AS w FROM typemx ORDER BY w LIMIT 5) x ORDER BY x.w`},
		{"group_over_computed_alias", `SELECT x.w, COUNT(*) AS n FROM (SELECT g*3 AS w FROM typemx ORDER BY w LIMIT 100) x GROUP BY x.w ORDER BY x.w`},
		{"window_partition_over_computed_alias", `SELECT z.id, z.gk, SUM(z.v) OVER (PARTITION BY z.gk) AS s FROM (SELECT id, g*2 AS gk, id AS v FROM typemx WHERE id < 6) z ORDER BY z.id`},
		{"window_order_over_computed_alias", `SELECT z.id, z.gk, SUM(z.v) OVER (ORDER BY z.gk) AS s FROM (SELECT id, g*2 AS gk, id AS v FROM typemx WHERE id < 6) z ORDER BY z.id`},
		{"nested_wrapped_window", `SELECT z.id AS zid, z.w AS zw, z.s AS s FROM (SELECT x.id, x.w, x.plain + 1 AS s FROM (SELECT id, plain, SUM(plain) OVER () + 0 AS w FROM wintab0) x) z ORDER BY z.id`},
		{"wrapped_window_one_level", `SELECT x.id AS zid, x.w AS zw, x.plain + 1 AS s FROM (SELECT id, plain, SUM(plain) OVER () + 0 AS w FROM wintab0) x ORDER BY x.id`},
		{"join_over_computed_alias", `SELECT p.id, p.w FROM (SELECT id, SUM(plain) OVER () + 0 AS w FROM wintab0) p JOIN wintab0 q ON p.id = q.id ORDER BY p.id`},
		{"outer_expr_over_computed_alias", `SELECT CASE WHEN x.s = '1.50' THEN x.v ELSE 0 END AS v FROM (SELECT s, a * 2 AS v FROM decpair) x`},
		// Controls. A plain RENAME and the CTE spelling both have to keep
		// answering; the qualified reference to a REAL column must survive.
		{"ctl_plain_rename", `SELECT x.w FROM (SELECT g AS w FROM typemx ORDER BY w LIMIT 5) x ORDER BY x.w`},
		{"ctl_cte_spelling", `WITH c AS (SELECT g*3 AS w FROM typemx ORDER BY w LIMIT 100) SELECT c.w, COUNT(*) AS n FROM c GROUP BY c.w ORDER BY c.w`},
		{"ctl_qualified_real_column", `SELECT x.g, x.id FROM typemx x WHERE x.id < 5 ORDER BY x.id`},
		{"ctl_self_join_qualified", `SELECT a.id, b.g FROM typemx a JOIN typemx b ON a.id = b.id WHERE a.id < 5 ORDER BY a.id`},
	}
}

func derivedAliasSchemas() map[string]parquet.Schema {
	return map[string]parquet.Schema{
		typematrix.Table: typematrix.Schema(),
		"wintab0": {Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "__win_0", Type: parquet.TypeInt64, Nullable: true},
			{Name: "plain", Type: parquet.TypeInt64, Nullable: true},
		}},
		"decpair": {Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "a", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
			{Name: "b", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true},
			{Name: "s", Type: parquet.TypeString, Nullable: true},
		}},
	}
}

// derivedAliasCatalog is a plan-only catalog over the three fixtures the
// derived-alias corpus names. It stores no bytes: every assertion here is
// about the PLAN.
func derivedAliasCatalog(t *testing.T) (*catalog.Catalog, context.Context) {
	t.Helper()
	ctx := context.Background()
	cat := catalog.NewWithStore(objstore.NewMemStore(), "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("catalog init: %v", err)
	}
	names := make([]string, 0, len(derivedAliasSchemas()))
	for name := range derivedAliasSchemas() {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := cat.CreateTable(ctx, name, derivedAliasSchemas()[name], nil); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		files := make([]catalog.FileEntry, 4)
		for i := range files {
			files[i] = catalog.FileEntry{
				Path:      fmt.Sprintf("tables/%s/chunk_%04d.parquet", name, i),
				SizeBytes: 1 << 20, NumRows: 1250,
			}
		}
		if err := cat.AddFiles(ctx, name, map[string]string{}, "tables/"+name+"/", files); err != nil {
			t.Fatalf("add files %s: %v", name, err)
		}
	}
	return cat, ctx
}

func optimizedPlanForReadSet(t *testing.T, cat *catalog.Catalog, ctx context.Context, sql string) *logical.Node {
	t.Helper()
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	lp, err := logical.BuildFromSelect(info)
	if err != nil {
		t.Fatalf("logical plan: %v", err)
	}
	ann := func(p *logical.Node) { NewPlanner(cat).AnnotateScanColumns(ctx, p) }
	ann(lp)
	return logical.Optimize(lp, ann)
}

func planStagesForReadSet(t *testing.T, cat *catalog.Catalog, ctx context.Context, sql string) ([]Stage, error) {
	t.Helper()
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	lp, err := logical.BuildFromSelect(info)
	if err != nil {
		t.Fatalf("logical plan: %v", err)
	}
	ann := func(p *logical.Node) { NewPlanner(cat).AnnotateScanColumns(ctx, p) }
	ann(lp)
	lp = logical.Optimize(lp, ann)
	pl := NewPlanner(cat)
	pl.WorkerCount = 3
	return pl.PlanDistributed(ctx, lp)
}
