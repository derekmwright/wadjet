package physical

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/exec"
	"github.com/citc-tech/wadjet/internal/planner/logical"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// The bushy suite constructs logical join trees with a JOIN SUBTREE ON THE
// BUILD SIDE — the exact shape bushy join enumeration (Layer B of
// docs/design/bushy-join-cbo.md) will emit — and executes them against
// in-memory fixtures. It exists because the May 2026 bushy attempt produced
// wrong rows at SF0.01: these tests pin the resolution layer at unit level
// so the failure class is caught without a benchmark run.

// setupBushyTables builds a mini star schema: a fact table and a dimension
// chain (supplier → nation → region), with nation designed for self-joins.
func setupBushyTables(t *testing.T) *catalog.Catalog {
	t.Helper()
	ctx := context.Background()
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}

	writeTable := func(name string, cols []parquet.Column, rows []map[string]any) {
		schema := parquet.Schema{Columns: cols}
		if err := cat.CreateTable(ctx, name, schema, nil); err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		cfg := parquet.DefaultWriterConfig()
		cfg.Compression = parquet.CompressionNone
		w, err := parquet.NewWriter(&buf, schema, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.WriteRows(rows); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		data := buf.Bytes()
		path := fmt.Sprintf("tables/%s/chunk_0000.parquet", name)
		if _, err := store.Put(ctx, "test", path, bytes.NewReader(data), int64(len(data)), ""); err != nil {
			t.Fatal(err)
		}
		if err := cat.AddFiles(ctx, name, map[string]string{}, "tables/"+name+"/", []catalog.FileEntry{{
			Path:      path,
			SizeBytes: int64(len(data)),
			NumRows:   int64(len(rows)),
		}}); err != nil {
			t.Fatal(err)
		}
	}

	// 4 regions; 12 nations (3 per region); 40 suppliers (round-robin
	// nations); 200 fact rows (round-robin suppliers).
	regionRows := make([]map[string]any, 4)
	for i := range regionRows {
		regionRows[i] = map[string]any{"r_rk": int64(i), "r_name": fmt.Sprintf("region%d", i)}
	}
	writeTable("b_region", []parquet.Column{
		{Name: "r_rk", Type: parquet.TypeInt64},
		{Name: "r_name", Type: parquet.TypeString},
	}, regionRows)

	nationRows := make([]map[string]any, 12)
	for i := range nationRows {
		nationRows[i] = map[string]any{"n_nk": int64(i), "n_name": fmt.Sprintf("nation%d", i), "n_rk": int64(i % 4)}
	}
	writeTable("b_nation", []parquet.Column{
		{Name: "n_nk", Type: parquet.TypeInt64},
		{Name: "n_name", Type: parquet.TypeString},
		{Name: "n_rk", Type: parquet.TypeInt64},
	}, nationRows)

	supplierRows := make([]map[string]any, 40)
	for i := range supplierRows {
		supplierRows[i] = map[string]any{"s_sk": int64(i), "s_name": fmt.Sprintf("supp%d", i), "s_nk": int64(i % 12)}
	}
	writeTable("b_supplier", []parquet.Column{
		{Name: "s_sk", Type: parquet.TypeInt64},
		{Name: "s_name", Type: parquet.TypeString},
		{Name: "s_nk", Type: parquet.TypeInt64},
	}, supplierRows)

	factRows := make([]map[string]any, 200)
	for i := range factRows {
		factRows[i] = map[string]any{"l_ok": int64(i), "l_sk": int64(i % 40), "l_qty": int64(i % 7)}
	}
	writeTable("b_fact", []parquet.Column{
		{Name: "l_ok", Type: parquet.TypeInt64},
		{Name: "l_sk", Type: parquet.TypeInt64},
		{Name: "l_qty", Type: parquet.TypeInt64},
	}, factRows)

	return cat
}

func bushyScan(table, alias string) *logical.Node {
	return &logical.Node{Type: logical.NodeScan, TableName: table, TableAlias: alias}
}

func bushyJoin(jt, cond string, left, right *logical.Node) *logical.Node {
	return &logical.Node{
		Type:     logical.NodeJoin,
		JoinType: jt,
		JoinCond: cond,
		Children: []*logical.Node{left, right},
	}
}

// planTree runs a hand-built logical tree through scan annotation and the
// local physical planner — bypassing the optimizer's (left-deep) join
// reorder, exactly as a bushy-enumerating reorder would hand its output on.
func planTree(t *testing.T, cat *catalog.Catalog, root *logical.Node) *PhysicalPlan {
	t.Helper()
	ctx := context.Background()
	planner := NewPlanner(cat)
	planner.AnnotateScanColumns(ctx, root)
	plan, err := planner.Plan(ctx, root)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	return plan
}

func runPlanRows(t *testing.T, plan *PhysicalPlan) []map[string]any {
	t.Helper()
	if err := plan.Pipeline.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	sink, ok := plan.Pipeline.Sink.(interface{ ToRows() []map[string]any })
	if !ok {
		t.Fatalf("sink %T does not expose rows", plan.Pipeline.Sink)
	}
	return sink.ToRows()
}

// rowKeySet flattens rows to sorted "col=val|col=val" strings over the given
// columns for order-independent multiset comparison.
func rowKeySet(t *testing.T, rows []map[string]any, cols ...string) []string {
	t.Helper()
	keys := make([]string, 0, len(rows))
	for _, r := range rows {
		s := ""
		for _, c := range cols {
			v, ok := r[c]
			if !ok {
				t.Fatalf("row missing column %q: %v", c, r)
			}
			s += fmt.Sprintf("%s=%v|", c, v)
		}
		keys = append(keys, s)
	}
	sort.Strings(keys)
	return keys
}

// TestBushyBuild_DimensionChain joins the fact table against a BUSHY build:
// supplier ⋈ (nation ⋈ region). The equivalent left-deep SQL is ground
// truth. The runtime key-repair tripwires must not fire — plan-time
// ownership resolution has to get every pair right on the first try.
func TestBushyBuild_DimensionChain(t *testing.T) {
	cat := setupBushyTables(t)

	repairsBefore := exec.KeyAssignmentRepairs.Load()

	bushy := bushyJoin("inner", "l_sk = s_sk",
		bushyScan("b_fact", ""),
		bushyJoin("inner", "s_nk = n_nk",
			bushyScan("b_supplier", ""),
			bushyJoin("inner", "n_rk = r_rk",
				bushyScan("b_nation", ""),
				bushyScan("b_region", ""))))

	rows := runPlanRows(t, planTree(t, cat, bushy))
	if len(rows) != 200 {
		t.Fatalf("bushy plan returned %d rows, want 200", len(rows))
	}

	ldPlan := planSQL(t, cat,
		"SELECT l_ok, s_name, n_name, r_name FROM b_fact "+
			"JOIN b_supplier ON l_sk = s_sk "+
			"JOIN b_nation ON s_nk = n_nk "+
			"JOIN b_region ON n_rk = r_rk", 0)
	ldRows := runPlanRows(t, ldPlan)

	got := rowKeySet(t, rows, "l_ok", "s_name", "n_name", "r_name")
	want := rowKeySet(t, ldRows, "l_ok", "s_name", "n_name", "r_name")
	if len(got) != len(want) {
		t.Fatalf("row count mismatch: bushy %d vs left-deep %d", len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("row %d differs:\nbushy:     %s\nleft-deep: %s", i, got[i], want[i])
		}
	}

	if d := exec.KeyAssignmentRepairs.Load() - repairsBefore; d != 0 {
		t.Fatalf("runtime key repair fired %d time(s) — plan-time side assignment missed pairs", d)
	}
}

// TestBushyBuild_ReversedKeysInCondition is the same shape with every join
// condition written build-side-first ("s_sk = l_sk"). parseJoinKeys assigns
// positionally, so every pair needs a plan-time swap — against a MULTI-TABLE
// build subtree that the old membership test could not resolve.
func TestBushyBuild_ReversedKeysInCondition(t *testing.T) {
	cat := setupBushyTables(t)

	repairsBefore := exec.KeyAssignmentRepairs.Load()

	bushy := bushyJoin("inner", "s_sk = l_sk",
		bushyScan("b_fact", ""),
		bushyJoin("inner", "n_nk = s_nk",
			bushyScan("b_supplier", ""),
			bushyJoin("inner", "r_rk = n_rk",
				bushyScan("b_nation", ""),
				bushyScan("b_region", ""))))

	rows := runPlanRows(t, planTree(t, cat, bushy))
	if len(rows) != 200 {
		t.Fatalf("bushy plan with reversed conditions returned %d rows, want 200", len(rows))
	}
	if d := exec.KeyAssignmentRepairs.Load() - repairsBefore; d != 0 {
		t.Fatalf("runtime key repair fired %d time(s)", d)
	}
}

// TestBushyBuild_SelfJoinOriginQualification puts a SECOND nation scan (n2)
// inside the build subtree BEHIND region, so the build's first-DFS scan
// alias is b_region — the single BuildTableAlias is wrong for n2's columns.
// The duplicate n_name/n_nk/n_rk columns must qualify under their OWNING
// alias (n2.*) via BuildColOrigins, not under b_region.*.
func TestBushyBuild_SelfJoinOriginQualification(t *testing.T) {
	cat := setupBushyTables(t)

	// Probe: fact ⋈ supplier ⋈ nation n1 (left-deep spine, bare n_name).
	// Build: region ⋈ nation n2 (region FIRST — findScanAlias returns it).
	probe := bushyJoin("inner", "s_nk = n1.n_nk",
		bushyJoin("inner", "l_sk = s_sk",
			bushyScan("b_fact", ""),
			bushyScan("b_supplier", "")),
		bushyScan("b_nation", "n1"))
	build := bushyJoin("inner", "r_rk = n2.n_rk",
		bushyScan("b_region", ""),
		bushyScan("b_nation", "n2"))
	root := bushyJoin("inner", "n1.n_rk = n2.n_rk", probe, build)

	rows := runPlanRows(t, planTree(t, cat, root))
	if len(rows) == 0 {
		t.Fatal("self-join bushy plan returned 0 rows")
	}
	// Every nation (n1) pairs with the 3 nations (n2) of its region:
	// 200 fact rows × 3 = 600.
	if len(rows) != 600 {
		t.Fatalf("self-join bushy plan returned %d rows, want 600", len(rows))
	}
	// The build's duplicate nation columns must be reachable under the n2
	// alias — b_region.* qualification (the old single-alias stamp) or a
	// dropped column would break this.
	sample := rows[0]
	if _, ok := sample["n2.n_name"]; !ok {
		cols := make([]string, 0, len(sample))
		for k := range sample {
			cols = append(cols, k)
		}
		sort.Strings(cols)
		t.Fatalf("build-side self-join column not qualified by owning alias; output columns: %v", cols)
	}
	// Probe-side n1 copy stays bare (probe columns keep their names).
	if _, ok := sample["n_name"]; !ok {
		t.Fatal("probe-side nation column lost its bare name")
	}
	// Correctness: each row's n1 and n2 nations must share a region.
	for _, r := range rows[:10] {
		if r["n_rk"] != r["n2.n_rk"] {
			t.Fatalf("joined nations differ in region: %v vs %v", r["n_rk"], r["n2.n_rk"])
		}
	}
}

// TestBushyBuild_DistributedStageShape runs a bushy tree through walkStages
// and asserts the DAG wiring the executor depends on: the parent join's
// build dependency is itself a join stage, BuildColOrigins is set (the build
// spans multiple tables), and the stage list passes DAG-shape validation.
func TestBushyBuild_DistributedStageShape(t *testing.T) {
	cat := setupBushyTables(t)
	ctx := context.Background()

	bushy := bushyJoin("inner", "l_sk = s_sk",
		bushyScan("b_fact", ""),
		bushyJoin("inner", "s_nk = n_nk",
			bushyScan("b_supplier", ""),
			bushyScan("b_nation", "")))

	planner := NewPlanner(cat)
	planner.WorkerCount = 3
	planner.AnnotateScanColumns(ctx, bushy)
	stages, err := planner.PlanDistributed(ctx, bushy)
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}

	stageByID := make(map[string]*Stage, len(stages))
	for i := range stages {
		stageByID[stages[i].ID] = &stages[i]
	}
	isJoinType := func(s *Stage) bool {
		return s.Type == StageHashJoin || s.Type == StageBroadcastJoin || s.Type == StageSortMergeJoin
	}

	// Find the parent join: a join stage whose build dep chain (through
	// exchanges) reaches another join stage.
	var parent *Stage
	for i := range stages {
		s := &stages[i]
		if !isJoinType(s) || s.RightDepStage == "" {
			continue
		}
		cur := stageByID[s.RightDepStage]
		for hop := 0; cur != nil && !isJoinType(cur) && hop < 3; hop++ {
			if len(cur.Dependencies) == 0 {
				cur = nil
				break
			}
			cur = stageByID[cur.Dependencies[0]]
		}
		if cur != nil && isJoinType(cur) {
			parent = s
			break
		}
	}
	if parent == nil {
		types := make([]string, len(stages))
		for i, s := range stages {
			types[i] = s.ID + ":" + s.Type
		}
		t.Fatalf("no join stage with a join-shaped build dependency; stages: %v", types)
	}
	if parent.BuildColOrigins == nil {
		t.Fatal("parent join over a multi-table build must carry BuildColOrigins")
	}
	if got := parent.BuildColOrigins["s_name"]; got != "b_supplier" {
		t.Fatalf("BuildColOrigins[s_name] = %q, want b_supplier", got)
	}
	if got := parent.BuildColOrigins["n_name"]; got != "b_nation" {
		t.Fatalf("BuildColOrigins[n_name] = %q, want b_nation", got)
	}
}
