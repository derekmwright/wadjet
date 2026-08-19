package physical

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/config"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// --- PrettyPrint ---

func TestPrettyPrint_EmptyStages(t *testing.T) {
	plan := &PhysicalPlan{}
	got := plan.PrettyPrint()
	if got != "Single-stage local execution" {
		t.Errorf("PrettyPrint() = %q, want 'Single-stage local execution'", got)
	}
}

func TestPrettyPrint_MultipleStages(t *testing.T) {
	plan := &PhysicalPlan{
		Stages: []Stage{
			{ID: "scan-0", Type: "scan", Tasks: 2},
			{ID: "agg-1", Type: "aggregate", Tasks: 1, Dependencies: []string{"scan-0"}},
		},
	}
	got := plan.PrettyPrint()
	if got == "" {
		t.Error("PrettyPrint() returned empty string for multi-stage plan")
	}
	// Should contain stage information
	if !contains(got, "scan-0") || !contains(got, "agg-1") {
		t.Errorf("PrettyPrint() missing stage IDs: %q", got)
	}
	if !contains(got, "depends on") {
		t.Errorf("PrettyPrint() missing dependency info: %q", got)
	}
}

func TestPrettyPrint_StageWithNoDeps(t *testing.T) {
	plan := &PhysicalPlan{
		Stages: []Stage{
			{ID: "scan-0", Type: "scan", Tasks: 3},
		},
	}
	got := plan.PrettyPrint()
	if contains(got, "depends on") {
		t.Errorf("PrettyPrint() should not show deps for stage without them: %q", got)
	}
}

// --- EstimateCost ---

func TestEstimateCost_ScanStages(t *testing.T) {
	stages := []Stage{
		{Type: "scan", EstimatedBytes: 1000, EstimatedRows: 100, ScanFiles: []string{"a.parquet", "b.parquet"}},
		{Type: "scan", EstimatedBytes: 2000, EstimatedRows: 200, ScanFiles: []string{"c.parquet"}},
		{Type: "aggregate"}, // non-scan stages should be ignored
	}
	node := logical.NewScan("t", "")
	cost := EstimateCost(stages, node)
	if cost.TotalBytes != 3000 {
		t.Errorf("TotalBytes = %d, want 3000", cost.TotalBytes)
	}
	if cost.TotalRows != 300 {
		t.Errorf("TotalRows = %d, want 300", cost.TotalRows)
	}
	if cost.TotalFiles != 3 {
		t.Errorf("TotalFiles = %d, want 3", cost.TotalFiles)
	}
}

func TestEstimateCost_WithFilter(t *testing.T) {
	stages := []Stage{{Type: "scan", EstimatedBytes: 1000, EstimatedRows: 100}}
	scan := logical.NewScan("t", "")
	filter := logical.NewFilter(scan, []logical.Predicate{{Raw: "x > 1"}})

	cost := EstimateCost(stages, filter)
	if !cost.HasFilter {
		t.Error("expected HasFilter=true with filter node")
	}
}

func TestEstimateCost_WithLimit(t *testing.T) {
	stages := []Stage{{Type: "scan", EstimatedBytes: 1000, EstimatedRows: 100}}
	scan := logical.NewScan("t", "")
	limit := logical.NewLimit(scan, 10, 0)

	cost := EstimateCost(stages, limit)
	if !cost.HasLimit {
		t.Error("expected HasLimit=true with limit node")
	}
}

func TestEstimateCost_WithPartitionFilter(t *testing.T) {
	stages := []Stage{{Type: "scan", EstimatedBytes: 1000, EstimatedRows: 100}}
	scan := logical.NewScan("t", "")
	scan.PartitionFilter = map[string]string{"year": "2026"}

	cost := EstimateCost(stages, scan)
	if !cost.HasFilter {
		t.Error("expected HasFilter=true with partition filter")
	}
}

func TestEstimateCost_WithScanPredicates(t *testing.T) {
	stages := []Stage{{Type: "scan", EstimatedBytes: 1000, EstimatedRows: 100}}
	scan := logical.NewScan("t", "")
	scan.ScanPredicates = []logical.Predicate{{Column: "x", Op: "=", Value: 1}}

	cost := EstimateCost(stages, scan)
	if !cost.HasFilter {
		t.Error("expected HasFilter=true with scan predicates")
	}
}

// --- hasFilterOrPartition ---

func TestHasFilterOrPartition_NilNode(t *testing.T) {
	if hasFilterOrPartition(nil) {
		t.Error("expected false for nil node")
	}
}

func TestHasFilterOrPartition_DeepFilter(t *testing.T) {
	scan := logical.NewScan("t", "")
	filter := logical.NewFilter(scan, nil)
	project := logical.NewProject(filter, nil)

	if !hasFilterOrPartition(project) {
		t.Error("expected true for deeply nested filter")
	}
}

// --- hasLimit ---

func TestHasLimit_NilNode(t *testing.T) {
	if hasLimit(nil) {
		t.Error("expected false for nil node")
	}
}

func TestHasLimit_NoLimit(t *testing.T) {
	scan := logical.NewScan("t", "")
	if hasLimit(scan) {
		t.Error("expected false for scan without limit")
	}
}

func TestHasLimit_DeepLimit(t *testing.T) {
	scan := logical.NewScan("t", "")
	limit := logical.NewLimit(scan, 10, 0)
	sort := logical.NewSort(limit, nil)

	if !hasLimit(sort) {
		t.Error("expected true for deeply nested limit")
	}
}

// --- formatBytes ---

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{500, "500B"},
		{1 << 20, "1.0MB"},
		{5 * (1 << 20), "5.0MB"},
		{1 << 30, "1.0GB"},
		{3 * (1 << 30), "3.0GB"},
		{1 << 40, "1.0TB"},
		{2 * (1 << 40), "2.0TB"},
	}
	for _, tt := range tests {
		got := formatBytes(tt.input)
		if got != tt.want {
			t.Errorf("formatBytes(%d) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

// --- enforceQueryLimits ---

func TestEnforceQueryLimits_NilLimits(t *testing.T) {
	cat, _ := setupCatalog(t)
	planner := NewPlanner(cat)
	// No query limits — should pass
	err := planner.enforceQueryLimits(nil, nil)
	if err != nil {
		t.Errorf("expected nil error with nil limits, got %v", err)
	}
}

func TestEnforceQueryLimits_MaxScanBytes(t *testing.T) {
	cat, _ := setupCatalog(t)
	planner := NewPlanner(cat)
	planner.QueryLimits = &config.QueryLimits{MaxScanBytes: 500}

	stages := []Stage{{Type: "scan", EstimatedBytes: 1000, EstimatedRows: 10}}
	scan := logical.NewScan("t", "")

	err := planner.enforceQueryLimits(stages, scan)
	if err == nil {
		t.Error("expected error for exceeding MaxScanBytes")
	}
}

func TestEnforceQueryLimits_MaxScanRows(t *testing.T) {
	cat, _ := setupCatalog(t)
	planner := NewPlanner(cat)
	planner.QueryLimits = &config.QueryLimits{MaxScanRows: 50}

	stages := []Stage{{Type: "scan", EstimatedBytes: 100, EstimatedRows: 100}}
	scan := logical.NewScan("t", "")

	err := planner.enforceQueryLimits(stages, scan)
	if err == nil {
		t.Error("expected error for exceeding MaxScanRows")
	}
}

func TestEnforceQueryLimits_MaxScanFiles(t *testing.T) {
	cat, _ := setupCatalog(t)
	planner := NewPlanner(cat)
	planner.QueryLimits = &config.QueryLimits{MaxScanFiles: 1}

	stages := []Stage{{Type: "scan", ScanFiles: []string{"a", "b", "c"}}}
	scan := logical.NewScan("t", "")

	err := planner.enforceQueryLimits(stages, scan)
	if err == nil {
		t.Error("expected error for exceeding MaxScanFiles")
	}
}

func TestEnforceQueryLimits_RequireFilterAboveBytes(t *testing.T) {
	cat, _ := setupCatalog(t)
	planner := NewPlanner(cat)
	planner.QueryLimits = &config.QueryLimits{RequireFilterAboveBytes: 100}

	stages := []Stage{{Type: "scan", EstimatedBytes: 200}}
	scan := logical.NewScan("t", "") // no filter

	err := planner.enforceQueryLimits(stages, scan)
	if err == nil {
		t.Error("expected error for missing filter above byte threshold")
	}

	// With filter should pass
	filter := logical.NewFilter(scan, []logical.Predicate{{Raw: "x=1"}})
	err = planner.enforceQueryLimits(stages, filter)
	if err != nil {
		t.Errorf("expected no error with filter, got %v", err)
	}
}

func TestEnforceQueryLimits_RequireLimitAboveRows(t *testing.T) {
	cat, _ := setupCatalog(t)
	planner := NewPlanner(cat)
	planner.QueryLimits = &config.QueryLimits{RequireLimitAboveRows: 50}

	stages := []Stage{{Type: "scan", EstimatedRows: 100}}
	scan := logical.NewScan("t", "") // no limit

	err := planner.enforceQueryLimits(stages, scan)
	if err == nil {
		t.Error("expected error for missing limit above row threshold")
	}

	// With limit should pass
	limit := logical.NewLimit(scan, 10, 0)
	err = planner.enforceQueryLimits(stages, limit)
	if err != nil {
		t.Errorf("expected no error with limit, got %v", err)
	}
}

func TestEnforceQueryLimits_UnderLimits(t *testing.T) {
	cat, _ := setupCatalog(t)
	planner := NewPlanner(cat)
	planner.QueryLimits = &config.QueryLimits{
		MaxScanBytes: 5000,
		MaxScanRows:  500,
		MaxScanFiles: 10,
	}

	stages := []Stage{{Type: "scan", EstimatedBytes: 1000, EstimatedRows: 100, ScanFiles: []string{"a"}}}
	scan := logical.NewScan("t", "")

	err := planner.enforceQueryLimits(stages, scan)
	if err != nil {
		t.Errorf("expected nil error under all limits, got %v", err)
	}
}

// --- mapJoinType ---

func TestMapJoinType(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"inner", "inner"},
		{"INNER JOIN", "inner"},
		{"left", "left"},
		{"LEFT OUTER JOIN", "left"},
		{"right", "right"},
		{"RIGHT JOIN", "right"},
		{"full", "full"},
		{"FULL OUTER JOIN", "full"},
		{"cross", "cross"},
		{"CROSS JOIN", "cross"},
		{"semi", "semi"},
		{"SEMI JOIN", "semi"},
		{"anti", "anti"},
		{"ANTI JOIN", "anti"},
		{"join", "inner"},
		{"", "inner"},
	}
	for _, tt := range tests {
		got := mapJoinType(tt.input)
		if got != tt.want {
			t.Errorf("mapJoinType(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

// --- mapExecJoinType ---

func TestMapExecJoinType(t *testing.T) {
	tests := []struct {
		input string
		want  exec.JoinType
	}{
		{"left", exec.LeftJoin},
		{"right", exec.RightJoin},
		{"full", exec.FullOuterJoin},
		{"cross", exec.CrossJoin},
		{"semi", exec.SemiJoin},
		{"anti", exec.AntiJoin},
		{"inner", exec.InnerJoin},
		{"unknown", exec.InnerJoin},
	}
	for _, tt := range tests {
		got := mapExecJoinType(tt.input)
		if got != tt.want {
			t.Errorf("mapExecJoinType(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

// --- parseJoinKeys ---

func TestParseJoinKeys(t *testing.T) {
	tests := []struct {
		cond     string
		leftN    int
		rightN   int
		residual int
	}{
		{cond: "e.user_id = u.user_id", leftN: 1, rightN: 1},
		{cond: "e.a = u.a AND e.b = u.b", leftN: 2, rightN: 2},
		{cond: "a = b", leftN: 1, rightN: 1},
		{cond: "no equals here", residual: 1},
		// The `1 = 1` sentinel extractJoinCondPredicates writes when every
		// ON conjunct has been pushed to a child: ON TRUE, not a refusal.
		{cond: "1 = 1", leftN: 1, rightN: 1},
	}
	for _, tt := range tests {
		left, right, residual := parseJoinKeys(tt.cond)
		if len(left) != tt.leftN || len(right) != tt.rightN || len(residual) != tt.residual {
			t.Errorf("parseJoinKeys(%q) = left=%v, right=%v, residual=%v, want %d left, %d right, %d residual",
				tt.cond, left, right, residual, tt.leftN, tt.rightN, tt.residual)
		}
	}
}

// TestParseJoinKeysRefusesUnrepresentable is #351: the key parser used to
// split the condition TEXT on "=" and hand whatever fell either side to the
// executor as a column name. An operand that is not a bare column resolves to
// nothing there, and an unresolvable key hashes as a constant — so the join
// matched no rows when one side was real and every row when neither was.
// Each of these must come back as a residual the caller refuses, never as a
// key.
func TestParseJoinKeysRefusesUnrepresentable(t *testing.T) {
	tests := []struct {
		name string
		cond string
	}{
		{"expression on the right", "n.n_regionkey = r.r_regionkey + 3"},
		{"expression on the left", "n.n_regionkey + 3 = r.r_regionkey"},
		{"expression on both sides", "n.n_regionkey + 1 = r.r_regionkey + 2"},
		{"function operand", "n.n_regionkey = ABS(r.r_regionkey)"},
		{"literal operand", "n.n_regionkey = 1"},
		// The lexical split found the "=" INSIDE these operators and
		// produced the column name "a.x <".
		{"less-or-equal", "a.x <= b.y"},
		{"greater-or-equal", "a.x >= b.y"},
		{"not-equal", "a.x != b.y"},
		{"disjunction", "a.x = b.y OR a.z = b.w"},
		// A key conjunct alongside an unrepresentable one: the good half
		// must not launder the bad half through.
		{"mixed with a real key", "a.k = b.k AND a.x = b.y + 1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, residual := parseJoinKeys(tt.cond)
			if len(residual) == 0 {
				left, right, _ := parseJoinKeys(tt.cond)
				t.Fatalf("parseJoinKeys(%q) reported no residual; it produced keys left=%v right=%v, "+
					"which the executor would look up as column names", tt.cond, left, right)
			}
		})
	}
}

// --- matchesPartitionFilter ---

func TestMatchesPartitionFilter(t *testing.T) {
	tests := []struct {
		name     string
		partVals map[string]string
		filter   map[string]string
		want     bool
	}{
		{"match", map[string]string{"year": "2026"}, map[string]string{"year": "2026"}, true},
		{"no match", map[string]string{"year": "2025"}, map[string]string{"year": "2026"}, false},
		{"filter key missing from partition", map[string]string{}, map[string]string{"year": "2026"}, true},
		{"multi key match", map[string]string{"year": "2026", "month": "03"}, map[string]string{"year": "2026", "month": "03"}, true},
		{"partial mismatch", map[string]string{"year": "2026", "month": "04"}, map[string]string{"year": "2026", "month": "03"}, false},
		{"empty filter", map[string]string{"year": "2026"}, map[string]string{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := matchesPartitionFilter(tt.partVals, tt.filter)
			if got != tt.want {
				t.Errorf("matchesPartitionFilter(%v, %v) = %v, want %v", tt.partVals, tt.filter, got, tt.want)
			}
		})
	}
}

// --- evalFilterTyped ---

func TestEvalFilterTyped(t *testing.T) {
	const (
		opNE = iota
		opGT
		opLT
		opGE
		opLE
		opEQ
	)

	// Test int32 comparisons
	pv := batch.NewVector(batch.TypeInt32, 2)
	bv := batch.NewVector(batch.TypeInt32, 2)
	pv.Int32Data[0] = 10
	pv.Int32Data[1] = 20
	bv.Int32Data[0] = 20
	bv.Int32Data[1] = 20
	if !evalFilterTyped(pv, bv, 0, 0, opNE) {
		t.Error("10 != 20 should be true")
	}
	if evalFilterTyped(pv, bv, 1, 1, opNE) {
		t.Error("20 != 20 should be false")
	}
	if !evalFilterTyped(pv, bv, 0, 0, opLT) {
		t.Error("10 < 20 should be true")
	}
	if !evalFilterTyped(pv, bv, 1, 1, opEQ) {
		t.Error("20 == 20 should be true")
	}

	// Test string comparisons via fallback
	sv := batch.NewVector(batch.TypeString, 2)
	sv.BytesData.Set(0, []byte("a"))
	sv.BytesData.Set(1, []byte("b"))
	tv := batch.NewVector(batch.TypeString, 2)
	tv.BytesData.Set(0, []byte("b"))
	tv.BytesData.Set(1, []byte("b"))
	if !evalFilterTyped(sv, tv, 0, 0, opNE) {
		t.Error("a != b should be true")
	}
	if evalFilterTyped(sv, tv, 1, 1, opNE) {
		t.Error("b != b should be false")
	}
	if !evalFilterTyped(sv, tv, 0, 0, opLT) {
		t.Error("a < b should be true")
	}
}

// --- mapPredOp ---

func TestMapPredOp(t *testing.T) {
	tests := []struct {
		op   string
		want exec.CompareOp
	}{
		{"=", exec.OpEq},
		{"!=", exec.OpNe},
		{"<>", exec.OpNe},
		{"<", exec.OpLt},
		{"<=", exec.OpLe},
		{">", exec.OpGt},
		{">=", exec.OpGe},
		{"like", exec.CompareOp(-1)},
		{"in", exec.CompareOp(-1)},
	}
	for _, tt := range tests {
		got := mapPredOp(tt.op)
		if got != tt.want {
			t.Errorf("mapPredOp(%q) = %d, want %d", tt.op, got, tt.want)
		}
	}
}

// --- decimalFromBytes ---

func TestDecimalFromBytes(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		wantZ bool // true if we expect zero
	}{
		{"empty", nil, true},
		{"zero", []byte{0}, false},
		{"small positive", []byte{0x00, 0x0A}, false},                 // 10
		{"small negative", []byte{0xFF, 0xF6}, false},                 // -10 (sign-extended)
		{"large positive", []byte{0, 0, 0, 0, 0, 0, 0, 0, 42}, false}, // 42 in 9 bytes
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := decimalFromBytes(tt.input)
			if tt.wantZ && (got.Hi != 0 || got.Lo != 0) {
				t.Errorf("decimalFromBytes(%v) = {Hi:%d, Lo:%d}, want zero", tt.input, got.Hi, got.Lo)
			}
		})
	}
}

// --- Distributed plan stage generation ---

func TestPlanDistributed_Distinct(t *testing.T) {
	cat, ctx := setupCatalog(t)
	planner := NewPlanner(cat)

	scan := logical.NewScan("events", "e")
	distinct := logical.NewDistinct(scan)
	stages, err := planner.PlanDistributed(ctx, distinct)
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}
	// Distinct should pass through to children
	if len(stages) == 0 {
		t.Fatal("expected at least 1 stage")
	}
	if stages[0].Type != "scan" {
		t.Errorf("expected scan stage, got %q", stages[0].Type)
	}
}

func TestPlanDistributed_Project(t *testing.T) {
	cat, ctx := setupCatalog(t)
	planner := NewPlanner(cat)

	scan := logical.NewScan("events", "e")
	proj := logical.NewProject(scan, []logical.Projection{{Column: "event_id", Alias: "id"}})
	stages, err := planner.PlanDistributed(ctx, proj)
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}
	if len(stages) == 0 {
		t.Fatal("expected at least 1 stage")
	}
	if stages[0].Type != "scan" {
		t.Errorf("expected scan stage from project passthrough, got %q", stages[0].Type)
	}
}

func TestPlanDistributed_Filter(t *testing.T) {
	cat, ctx := setupCatalog(t)
	planner := NewPlanner(cat)

	scan := logical.NewScan("events", "e")
	filter := logical.NewFilter(scan, []logical.Predicate{
		{Raw: "year = '2026'"},
	})
	stages, err := planner.PlanDistributed(ctx, filter)
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}
	if len(stages) == 0 {
		t.Fatal("expected at least 1 stage")
	}
}

func TestPlanDistributed_LimitSort(t *testing.T) {
	cat, ctx := setupCatalog(t)
	planner := NewPlanner(cat)

	scan := logical.NewScan("events", "e")
	sort := logical.NewSort(scan, []logical.OrderExpr{{Column: "ts", Desc: true}})
	limit := logical.NewLimit(sort, 10, 0)

	stages, err := planner.PlanDistributed(ctx, limit)
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}

	// Native-DAG: scan + sort (with limit fused) + exchange-gather.
	// merge_sort is collapsed by collapseRedundantFinalMergeSort.
	if len(stages) < 3 {
		t.Fatalf("expected at least 3 stages, got %d", len(stages))
	}

	foundSort := false
	for _, s := range stages {
		if s.Type == "sort" {
			foundSort = true
			if s.Limit != 10 {
				t.Errorf("expected sort stage limit=10, got %d", s.Limit)
			}
		}
	}
	if !foundSort {
		t.Error("expected a sort stage")
	}
}

// This used to assert only "2 scan stages for a union", which is exactly the
// #346 shape: both arms planned, nothing merging them, and the gather left
// reading whichever arm came last. A union must now emit a stage that
// consumes BOTH arms.
func TestPlanDistributed_UnionSetOp(t *testing.T) {
	cat, ctx := setupCatalogWithUsers(t)
	planner := NewPlanner(cat)

	// Both arms scan `users`: a set operation's arms must agree on arity,
	// and events/users do not.
	left := logical.NewScan("users", "u1")
	right := logical.NewScan("users", "u2")
	planner.AnnotateScanColumns(ctx, left)
	planner.AnnotateScanColumns(ctx, right)
	union := logical.NewUnion(left, right, true)
	stages, err := planner.PlanDistributed(ctx, union)
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}
	scanCount := 0
	var unionStage *Stage
	for i, s := range stages {
		if s.Type == StageScan {
			scanCount++
		}
		if s.Type == StageUnion {
			unionStage = &stages[i]
		}
	}
	if scanCount != 2 {
		t.Errorf("expected 2 scan stages for union, got %d", scanCount)
	}
	if unionStage == nil {
		t.Fatal("no union stage: both arms were planned and nothing merged them")
	}
	if len(unionStage.Dependencies) != 2 {
		t.Errorf("union stage depends on %v, want both arms", unionStage.Dependencies)
	}
}

// INTERSECT and EXCEPT need set semantics across the arms, which the stage
// DAG has no operator for. They used to plan "successfully" and answer with
// one arm's rows; refusing is the fix (#346).
func TestPlanDistributed_IntersectSetOp(t *testing.T) {
	cat, ctx := setupCatalogWithUsers(t)
	planner := NewPlanner(cat)

	left := logical.NewScan("events", "e")
	right := logical.NewScan("users", "u")
	intersect := logical.NewIntersect(left, right, false)
	if _, err := planner.PlanDistributed(ctx, intersect); err == nil {
		t.Fatal("INTERSECT planned on the stage DAG; it must be refused, not answered with one arm")
	} else if !strings.Contains(err.Error(), "INTERSECT") {
		t.Errorf("error %q does not name the operation", err)
	}
}

func TestPlanDistributed_ExceptSetOp(t *testing.T) {
	cat, ctx := setupCatalogWithUsers(t)
	planner := NewPlanner(cat)

	left := logical.NewScan("events", "e")
	right := logical.NewScan("users", "u")
	except := logical.NewExcept(left, right, true)
	if _, err := planner.PlanDistributed(ctx, except); err == nil {
		t.Fatal("EXCEPT ALL planned on the stage DAG; it must be refused, not answered with one arm")
	} else if !strings.Contains(err.Error(), "EXCEPT ALL") {
		t.Errorf("error %q does not name the operation", err)
	}
}

// --- isURL / isGlob helpers ---

func TestIsURL(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"http://example.com", true},
		{"https://example.com/data.json", true},
		{"s3://bucket/key", false},
		{"/tmp/data.json", false},
		{"data.json", false},
	}
	for _, tt := range tests {
		got := isURL(tt.input)
		if got != tt.want {
			t.Errorf("isURL(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestIsGlob(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"*.json", true},
		{"data?.csv", true},
		{"data[0-9].csv", true},
		{"data.json", false},
		{"/path/to/file.parquet", false},
	}
	for _, tt := range tests {
		got := isGlob(tt.input)
		if got != tt.want {
			t.Errorf("isGlob(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

// --- dbScanSource ---

func TestDBScanSource_Close_NilFields(t *testing.T) {
	s := &dbScanSource{}
	err := s.Close()
	if err != nil {
		t.Errorf("Close on nil fields should not error, got %v", err)
	}
}

// --- CSV named args ---

func TestBuildTableFunctionSource_CSV_SepArg(t *testing.T) {
	source, err := buildTableFunctionSource("read_csv", []string{"/tmp/test.csv"}, map[string]string{"sep": "|"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	csvSrc, ok := source.(*csvTableFuncSource)
	if !ok {
		t.Fatalf("expected *csvTableFuncSource, got %T", source)
	}
	if csvSrc.namedArgs["sep"] != "|" {
		t.Errorf("expected sep=|, got %v", csvSrc.namedArgs)
	}
}

func TestBuildTableFunctionSource_ReadJSONAuto(t *testing.T) {
	source, err := buildTableFunctionSource("read_json_auto", []string{"/tmp/data.json"}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := source.(*jsonTableFuncSource); !ok {
		t.Errorf("expected *jsonTableFuncSource for read_json_auto, got %T", source)
	}
}

func TestBuildTableFunctionSource_ReadCSVAuto(t *testing.T) {
	source, err := buildTableFunctionSource("read_csv_auto", []string{"/tmp/data.csv"}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := source.(*csvTableFuncSource); !ok {
		t.Errorf("expected *csvTableFuncSource for read_csv_auto, got %T", source)
	}
}

// helpers

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstring(s, substr))
}

func containsSubstring(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// --- resolveNullsLast ---

func TestResolveNullsLast(t *testing.T) {
	yes, no := true, false
	cases := []struct {
		name string
		ob   logical.OrderExpr
		want bool
	}{
		// The engine default is NULLS LAST in BOTH directions — DuckDB's
		// default_null_order, and the placement this engine has always
		// emitted. It used to be declared here as `!ob.Desc` (PostgreSQL's
		// rule) but no query ever saw that: the DESC comparator negated the
		// kernel's null handling along with its values, so a nominal NULLS
		// FIRST for DESC left the engine as NULLS LAST. #343 removed the
		// negation, and this constant records what the engine emits rather
		// than what the comment claimed — the stored DuckDB fingerprints in
		// benchmarks/tpch pin the DESC default to NULLS LAST.
		{"ASC default", logical.OrderExpr{Column: "id"}, true},
		{"DESC default", logical.OrderExpr{Column: "id", Desc: true}, true},
		// An explicit clause wins in BOTH directions. DESC was the broken
		// pair — both spellings came out inverted (#343).
		{"ASC NULLS FIRST", logical.OrderExpr{Column: "id", NullsFirst: &yes}, false},
		{"ASC NULLS LAST", logical.OrderExpr{Column: "id", NullsFirst: &no}, true},
		{"DESC NULLS FIRST", logical.OrderExpr{Column: "id", Desc: true, NullsFirst: &yes}, false},
		{"DESC NULLS LAST", logical.OrderExpr{Column: "id", Desc: true, NullsFirst: &no}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveNullsLast(tc.ob); got != tc.want {
				t.Errorf("resolveNullsLast = %v, want %v", got, tc.want)
			}
		})
	}
}

// --- extractFilterBuildColumns ---

func TestExtractFilterBuildColumns(t *testing.T) {
	// Empty
	cols := extractFilterBuildColumns("")
	if cols != nil {
		t.Errorf("expected nil for empty, got %v", cols)
	}

	// Simple equality
	cols = extractFilterBuildColumns("e.id = u.id")
	if len(cols) != 1 {
		t.Fatalf("expected 1 column, got %d: %v", len(cols), cols)
	}
	// The right side of "e.id = u.id" is "u.id" → cleanExpr → "id"
	if cols[0] != "id" {
		t.Errorf("expected 'id', got %q", cols[0])
	}

	// Multiple conditions with AND
	cols = extractFilterBuildColumns("a.x = b.x AND a.y = b.y")
	if len(cols) != 2 {
		t.Fatalf("expected 2 columns, got %d: %v", len(cols), cols)
	}

	// No operator matches
	cols = extractFilterBuildColumns("unknown stuff")
	if len(cols) != 0 {
		t.Errorf("expected 0 columns for unrecognized filter, got %v", cols)
	}
}

// --- cleanExpr ---

func TestCleanExpr(t *testing.T) {
	if cleanExpr("t.col") != "col" {
		t.Error("expected 'col' for 't.col'")
	}
	if cleanExpr("col") != "col" {
		t.Error("expected 'col' for 'col'")
	}
	if cleanExpr("  t.col  ") != "col" {
		t.Error("expected 'col' for '  t.col  '")
	}
}

// --- PlanDistributed: additional node types ---

func TestPlanDistributed_Window(t *testing.T) {
	cat, ctx := setupCatalog(t)
	p := NewPlanner(cat)

	scan := logical.NewScan("events", "e")
	window := logical.NewWindow(scan, []logical.WindowExpr{
		{
			Func:        "row_number",
			OutputCol:   "rn",
			PartitionBy: []string{"src_ip"},
			OrderBy:     []logical.OrderExpr{{Column: "ts"}},
		},
	})

	stages, err := p.PlanDistributed(ctx, window)
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}
	if len(stages) == 0 {
		t.Error("expected at least 1 stage for window")
	}
}

func TestPlanDistributed_Aggregate(t *testing.T) {
	cat, ctx := setupCatalog(t)
	p := NewPlanner(cat)

	scan := logical.NewScan("events", "e")
	agg := logical.NewAggregate(scan, []string{"src_ip"}, []logical.AggExpr{
		{Func: "count", InputCol: "*", OutputCol: "cnt"},
	})

	stages, err := p.PlanDistributed(ctx, agg)
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}
	if len(stages) == 0 {
		t.Error("expected at least 1 stage for aggregate")
	}
}

func TestPlanDistributed_Sort(t *testing.T) {
	cat, ctx := setupCatalog(t)
	p := NewPlanner(cat)

	scan := logical.NewScan("events", "e")
	sort := logical.NewSort(scan, []logical.OrderExpr{{Column: "ts"}})

	stages, err := p.PlanDistributed(ctx, sort)
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}
	if len(stages) < 2 {
		t.Fatalf("expected at least 2 stages (sort + merge_sort), got %d", len(stages))
	}
	// Native-DAG: last stage is exchange-gather, penultimate is sort
	// (merge_sort collapsed by collapseRedundantFinalMergeSort).
	sortStage := stages[len(stages)-2]
	gatherStage := stages[len(stages)-1]
	if sortStage.Type != "sort" {
		t.Errorf("expected sort stage, got %q", sortStage.Type)
	}
	if gatherStage.Type != StageExchangeGather {
		t.Errorf("expected exchange-gather stage, got %q", gatherStage.Type)
	}
	if len(gatherStage.Dependencies) != 1 || gatherStage.Dependencies[0] != sortStage.ID {
		t.Errorf("exchange-gather should depend on sort stage, got %v", gatherStage.Dependencies)
	}
}

func TestPlanDistributed_Join(t *testing.T) {
	cat, ctx := setupCatalogWithUsers(t)
	p := NewPlanner(cat)

	left := logical.NewScan("events", "e")
	right := logical.NewScan("users", "u")
	join := logical.NewJoin(left, right, "inner", "e.user_id = u.id")

	stages, err := p.PlanDistributed(ctx, join)
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}
	if len(stages) == 0 {
		t.Error("expected at least 1 stage for join")
	}
}
