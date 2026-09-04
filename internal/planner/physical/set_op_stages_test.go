package physical

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// planSetOpStages runs one query through the distributed planner and returns
// its stages, or the planning error. Unlike sqlToStages it does not Fatal on
// a planner error, because refusing to plan IS what several of these cases
// assert.
func planSetOpStages(t *testing.T, cat *catalog.Catalog, ctx context.Context, sql string) ([]Stage, error) {
	t.Helper()
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	selectInfo, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("extract %q: %v", sql, err)
	}
	plan, err := logical.BuildFromSelect(selectInfo)
	if err != nil {
		t.Fatalf("logical plan %q: %v", sql, err)
	}
	annotate := func(n *logical.Node) { NewPlanner(cat).AnnotateScanColumns(ctx, n) }
	annotate(plan)
	plan = logical.Optimize(plan, annotate)

	p := NewPlanner(cat)
	p.WorkerCount = 3
	return p.PlanDistributed(ctx, plan)
}

// stageTypeIDs renders a stage list as "id(type)" for failure messages.
func stageTypeIDs(stages []Stage) []string {
	ids := make([]string, len(stages))
	for i, s := range stages {
		ids[i] = s.ID + "(" + s.Type + ")"
	}
	return ids
}

func onlyStageOfType(t *testing.T, stages []Stage, typ string) Stage {
	t.Helper()
	var found []Stage
	for _, s := range stages {
		if s.Type == typ {
			found = append(found, s)
		}
	}
	if len(found) != 1 {
		t.Fatalf("want exactly 1 %q stage, got %d in %v", typ, len(found), stageTypeIDs(stages))
	}
	return found[0]
}

// TestSetOpUnionAllEmitsMergeStage is the #346 regression at the plan level:
// a UNION ALL must produce a stage that DEPENDS ON BOTH ARMS, and the
// terminal gather must read that stage rather than one arm's scan.
//
// Before the fix walkStages emitted the two scans and nothing else, so the
// gather's sole dependency was scan-1 — the query's answer was whichever arm
// happened to be emitted last, at half the rows and its table's full width.
func TestSetOpUnionAllEmitsMergeStage(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	stages, err := planSetOpStages(t, cat, ctx,
		"SELECT r_regionkey FROM region UNION ALL SELECT r_regionkey FROM region")
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}

	union := onlyStageOfType(t, stages, StageUnion)
	if len(union.Dependencies) != 2 {
		t.Fatalf("union stage %s depends on %v, want both arms", union.ID, union.Dependencies)
	}
	if union.Tasks != 2 {
		t.Errorf("union stage has %d tasks, want one per arm (2)", union.Tasks)
	}
	if len(union.UnionArms) != len(union.Dependencies) {
		t.Fatalf("union stage has %d arms and %d dependencies — dispatch pairs them by index",
			len(union.UnionArms), len(union.Dependencies))
	}
	for i, arm := range union.UnionArms {
		if union.UnionArmDep(i) != union.Dependencies[i] {
			t.Errorf("arm %d names producer %q but Dependencies[%d] is %q",
				i, union.UnionArmDep(i), i, union.Dependencies[i])
		}
		// The projection is what stops the arm's raw parquet pass-through
		// (r_regionkey, r_name, r_comment) reaching the client.
		if len(arm.Projections) != 1 || arm.Projections[0].Name != "r_regionkey" {
			t.Errorf("arm %d projects %+v, want exactly the result column r_regionkey", i, arm.Projections)
		}
	}

	gather := onlyStageOfType(t, stages, StageExchangeGather)
	if len(gather.Dependencies) != 1 || gather.Dependencies[0] != union.ID {
		t.Errorf("gather depends on %v, want the union stage %s — reading an arm directly IS the bug",
			gather.Dependencies, union.ID)
	}
	// Both arms must be consumed by something. An orphan scan is the shape
	// the gather used to skip past.
	consumed := map[string]bool{}
	for _, s := range stages {
		for _, d := range s.Dependencies {
			consumed[d] = true
		}
	}
	for _, s := range stages {
		if s.Type == StageScan && !consumed[s.ID] {
			t.Errorf("scan stage %s feeds nothing — an arm was dropped from the plan", s.ID)
		}
	}
}

// TestSetOpUnionEmitsDedup: UNION without ALL needs the dedup to run ACROSS
// the arms, so it sits above the concatenation, not inside either arm.
func TestSetOpUnionEmitsDedup(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	stages, err := planSetOpStages(t, cat, ctx,
		"SELECT n_regionkey FROM nation WHERE n_nationkey < 5 UNION "+
			"SELECT n_regionkey FROM nation WHERE n_nationkey >= 5")
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}
	union := onlyStageOfType(t, stages, StageUnion)

	var dedup *Stage
	for i := range stages {
		if stages[i].GroupByAll {
			dedup = &stages[i]
		}
	}
	if dedup == nil {
		t.Fatalf("no GroupByAll dedup stage in %v — a bare UNION must deduplicate", stageTypeIDs(stages))
	}
	if len(dedup.Dependencies) != 1 || dedup.Dependencies[0] != union.ID {
		t.Errorf("dedup stage depends on %v, want the union stage %s (dedup must see BOTH arms)",
			dedup.Dependencies, union.ID)
	}
	if dedup.Tasks != 1 {
		t.Errorf("dedup stage has %d tasks; the whole distinct set must land in one", dedup.Tasks)
	}
}

// TestSetOpUnionAllOmitsDedup is the control: UNION ALL keeps duplicates, so
// a dedup stage there would be a wrong answer of the opposite kind.
func TestSetOpUnionAllOmitsDedup(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	stages, err := planSetOpStages(t, cat, ctx,
		"SELECT r_regionkey FROM region UNION ALL SELECT r_regionkey FROM region")
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}
	for _, s := range stages {
		if s.GroupByAll {
			t.Errorf("stage %s deduplicates a UNION ALL", s.ID)
		}
	}
}

// TestSetOpRefusals: the shapes the DAG cannot lower must FAIL, loudly and by
// name. Returning one arm — which is what the code did before #346 — is a
// wrong answer dressed as a right one.
func TestSetOpRefusals(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	cases := []struct {
		name string
		sql  string
		want []string // substrings the error must carry
		// pg marks a refusal PostgreSQL also makes: it carries PostgreSQL's
		// SQLSTATE and its message alone, with no distributed-planning
		// preamble.
		pg bool
	}{
		{
			// The arm's aggregate stage names its own output; the union
			// stage's projection would have to guess that name.
			name: "AggregateArm",
			sql:  "SELECT COUNT(*) AS c FROM region UNION ALL SELECT COUNT(*) AS c FROM nation",
			want: []string{"UNION ALL", "aggregate"},
		},
		{
			// Text and a number do not widen into one another, and coercing
			// either would answer a different question. PostgreSQL refuses it
			// too, so the refusal is ITS message and SQLSTATE and carries no
			// distributed-planning preamble (#648).
			name: "IrreconcilableTypes",
			sql:  "SELECT r_name AS v FROM region UNION ALL SELECT n_nationkey AS v FROM nation",
			want: []string{"UNION types text and integer cannot be matched", "\"v\""},
			pg:   true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stages, err := planSetOpStages(t, cat, ctx, tc.sql)
			if err == nil {
				t.Fatalf("planned %d stages (%v) — this shape must be refused, not answered with one arm",
					len(stages), stageTypeIDs(stages))
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not mention %q", err, want)
				}
			}
			if tc.pg {
				if got := sqlerr.StateOf(err); got != "42804" {
					t.Errorf("error %q carries SQLSTATE %q, want PostgreSQL's 42804", err, got)
				}
				if strings.Contains(err.Error(), "#346") {
					t.Errorf("a refusal PostgreSQL also makes carries a distributed-planning "+
						"preamble: %q", err)
				}
				return
			}
			if !strings.Contains(err.Error(), "#346") {
				t.Errorf("error %q does not point at the issue", err)
			}
		})
	}
}

// TestSetOpArmTypesReconciled: the arms' outputs are separate .wshf files
// read as one stream, so a column declared FLOAT64 by one arm and INT32 by
// another is a decoding error rather than a union. The narrower arm is cast.
//
// Two rungs of the widening ladder, both live since #445/#472 threaded the
// integer-preserving-arithmetic rule (#297) through every computed-column
// call site:
//
//   - float64 widening: `+ 100.5` forces FLOAT64 regardless of the strict-int
//     rule (no integer literal reconciles with it), so this rung stays
//     exercised whatever the arithmetic rule declares for `+ 100` alone.
//   - int64 widening: `r_regionkey + 100` on the strict-int r_regionkey now
//     declares INT64 like PostgreSQL does, not FLOAT64 — the case #445 could
//     not name a working int-only repro for it dropped when it switched to
//     100.5. Now that #472 closed the last call site still passing
//     strictInt=nil, the INT32→INT64 rung is reachable and worth pinning in
//     its own right, alongside the FLOAT64 one rather than instead of it.
func TestSetOpArmTypesReconciled(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		// want is the type both arms must agree on after reconciliation.
		want parquet.TypeID
		// castArm is the union-arm index (nation's arm, always the narrower
		// INT32 one here) that must carry a CAST to want.
		castArm int
	}{
		{
			// A genuine FLOAT column, because `+ 100.5` no longer forces
			// FLOAT64: a numeric literal's carrier is its spelling, so
			// `int + 100.5` is DECIMAL — which is what PostgreSQL answers
			// too (`pg_typeof(1::int + 100.5)` is numeric, verified live).
			// The DECIMAL rung is pinned in its own case below.
			name:    "float64 widening",
			sql:     "SELECT o_totalprice + 1 AS k FROM orders UNION ALL SELECT n_nationkey AS k FROM nation",
			want:    parquet.TypeFloat64,
			castArm: 1,
		},
		{
			name:    "int64 widening",
			sql:     "SELECT r_regionkey + 100 AS k FROM region UNION ALL SELECT n_nationkey AS k FROM nation",
			want:    parquet.TypeInt64,
			castArm: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cat, ctx := setupTPCHCatalog(t)
			stages, err := planSetOpStages(t, cat, ctx, tc.sql)
			if err != nil {
				t.Fatalf("PlanDistributed: %v", err)
			}
			union := onlyStageOfType(t, stages, StageUnion)
			if len(union.UnionArms) != 2 {
				t.Fatalf("union has %d arms, want 2", len(union.UnionArms))
			}
			for i, arm := range union.UnionArms {
				if len(arm.Projections) != 1 {
					t.Fatalf("arm %d projects %d columns, want 1", i, len(arm.Projections))
				}
				if arm.Projections[0].Type != tc.want {
					t.Errorf("arm %d emits column k as %s; both arms must agree on the type",
						i, arm.Projections[0].Type)
				}
			}
			if got := union.UnionArms[tc.castArm].Projections[0].Expr; !strings.Contains(strings.ToUpper(got), "CAST") {
				t.Errorf("the narrower arm's expression is %q — it must be cast to the reconciled type", got)
			}
		})
	}
}

// TestSetOpDecimalRungReconcilesAnIntegerArm is the ladder's DECIMAL rung,
// newly reachable now that `int + 100.5` resolves DECIMAL rather than FLOAT64
// (#555, ADR-0024 item 3 — a numeric literal's carrier is its spelling, and
// `pg_typeof(1::int + 100.5)` is numeric on postgres:17.11).
//
// A DECIMAL target is reconciled by MOVING each arm's carrier
// (DecimalCoercion), never by a CAST — the cast evaluator's DECIMAL
// destination cannot preserve an exact value, which is ADR-0012 item 12's
// reason — so this rung asserts the coercion and the declared (p,s) rather
// than a CAST in the expression text. The declared spec matters as much as the
// coercion: the worker builds the arm's output vector from it, and a spec left
// at the zero value builds a DECIMAL at scale 0.
func TestSetOpDecimalRungReconcilesAnIntegerArm(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	stages, err := planSetOpStages(t, cat, ctx,
		"SELECT r_regionkey + 100.5 AS k FROM region UNION ALL SELECT n_nationkey AS k FROM nation")
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}
	union := onlyStageOfType(t, stages, StageUnion)
	if len(union.UnionArms) != 2 {
		t.Fatalf("union has %d arms, want 2", len(union.UnionArms))
	}
	// r_regionkey is INT32, so the computed arm is DECIMAL(10,0) + DECIMAL(4,1)
	// = DECIMAL(max(0,1) + max(10,3) + 1, 1) = DECIMAL(12,1); the integer arm
	// is DECIMAL(10,0). Their common type keeps the widest integer part (11)
	// at the widest scale (1).
	want := logical.DecimalMeta{Precision: 12, Scale: 1}
	// Each arm declares its OWN type, not the reconciled one: the declaration
	// is what the worker builds THAT arm's output vector from, and the
	// coercion runs after that vector exists. Declaring the target on the
	// integer arm made its projection build a DECIMAL vector and the checked
	// writer refuse the int box — "integer value 100 reached a DECIMAL(scale
	// 1) column as a raw unscaled carrier" — before DecimalCoerce could
	// convert it. A BARE column arm never showed it (a DirectCopy types
	// itself from the input and ignores the spec); a COMPUTED one does.
	computed := union.UnionArms[0].Projections[0]
	if computed.Type != parquet.TypeDecimal || !computed.TypeKnown {
		t.Errorf("the computed arm declares %s (known=%v), want DECIMAL",
			computed.Type, computed.TypeKnown)
	}
	if computed.Precision != want.Precision || computed.Scale != want.Scale {
		t.Errorf("the computed arm declares DECIMAL(%d,%d), want DECIMAL(%d,%d)",
			computed.Precision, computed.Scale, want.Precision, want.Scale)
	}
	if intArm := union.UnionArms[1].Projections[0]; intArm.Type != parquet.TypeInt32 {
		t.Errorf("the integer arm declares %s, want INT32 — it must ARRIVE as an integer "+
			"for the coercion below to have anything to convert", intArm.Type)
	}
	// The INTEGER arm has no carrier at the result scale and must be moved.
	if got := union.UnionArms[1].DecimalCoercions; len(got) != 1 ||
		got[0].Precision != want.Precision || got[0].Scale != want.Scale {
		t.Errorf("the integer arm carries %v; it must be coerced to DECIMAL(%d,%d)",
			got, want.Precision, want.Scale)
	}
}

// TestSetOpNestedChain: `a UNION ALL b UNION ALL c` parses left-deep, so the
// outer union's first arm is another union carrying no SELECT list of its
// own. The result names come from the leftmost arm of the whole chain, and
// the outer union reads its nested arm through the names that arm emits.
func TestSetOpNestedChain(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	stages, err := planSetOpStages(t, cat, ctx,
		"SELECT r_regionkey FROM region UNION ALL SELECT n_regionkey FROM nation "+
			"UNION ALL SELECT r_regionkey FROM region")
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}
	var unions []Stage
	for _, s := range stages {
		if s.Type == StageUnion {
			unions = append(unions, s)
		}
	}
	if len(unions) != 2 {
		t.Fatalf("want 2 union stages for a 3-arm chain, got %d in %v", len(unions), stageTypeIDs(stages))
	}
	for _, u := range unions {
		for i, arm := range u.UnionArms {
			if len(arm.Projections) != 1 || arm.Projections[0].Name != "r_regionkey" {
				t.Errorf("union %s arm %d projects %+v; every arm must land on the leftmost arm's name",
					u.ID, i, arm.Projections)
			}
		}
	}
	// The outer union consumes the inner one, so the inner is not a leaf.
	outer, inner := unions[1], unions[0]
	found := false
	for _, d := range outer.Dependencies {
		if d == inner.ID {
			found = true
		}
	}
	if !found {
		t.Errorf("outer union %s depends on %v, not on the inner union %s", outer.ID, outer.Dependencies, inner.ID)
	}
}

// TestSetOpFilterReachesUnionStage: a WHERE above a set operation is pushed
// onto the stage walkStages just emitted, which is now the union. The
// fragment builder turns FilterExprs into an OpFilter; if the predicate did
// not land here it would be dropped and the whole concatenation returned.
func TestSetOpFilterReachesUnionStage(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	stages, err := planSetOpStages(t, cat, ctx,
		"SELECT k FROM (SELECT r_regionkey AS k FROM region UNION ALL "+
			"SELECT n_nationkey AS k FROM nation) u WHERE k < 3")
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}
	union := onlyStageOfType(t, stages, StageUnion)
	if len(union.FilterExprs) == 0 {
		t.Fatalf("union stage %s carries no filter; the WHERE above the set operation was dropped", union.ID)
	}
}

// TestValidateNativeDAGShapeUnion: dispatch pairs arm i with Dependencies[i],
// so the two lists have to be the same LENGTH. They can no longer disagree
// about a producer — arm i's producer IS Dependencies[i], read through
// Stage.UnionArmDep — which is why the "names producer" check is gone along
// with the stored copy it checked (#715).
func TestValidateNativeDAGShapeUnion(t *testing.T) {
	base := func() []Stage {
		return []Stage{
			{ID: "scan-0", Type: StageScan},
			{ID: "scan-1", Type: StageScan},
			{
				ID: "union-2", Type: StageUnion, Dependencies: []string{"scan-0", "scan-1"},
				UnionArms: []UnionArm{{}, {}},
			},
		}
	}
	if err := ValidateNativeDAGShape(base()); err != nil {
		t.Fatalf("a well-formed union plan was rejected: %v", err)
	}

	// TWO ARMS READING ONE PRODUCER is legal and has to stay so: a CTE
	// referenced twice, and two identical sorted subqueries, both lower to it
	// (#660, #715).
	shared := base()
	shared[2].Dependencies = []string{"scan-0", "scan-0"}
	if err := ValidateNativeDAGShape(shared); err != nil {
		t.Errorf("two arms reading the same producer were rejected: %v", err)
	}

	short := base()
	short[2].UnionArms = short[2].UnionArms[:1]
	if err := ValidateNativeDAGShape(short); err == nil {
		t.Error("a union with fewer arms than dependencies accepted")
	}

	single := base()
	single[2].Dependencies = []string{"scan-0"}
	single[2].UnionArms = single[2].UnionArms[:1]
	if err := ValidateNativeDAGShape(single); err == nil {
		t.Error("a one-armed union accepted — that is the pre-fix shape")
	}
}

// TestAlignSetOpRows covers the single-process half: set-operation arms
// correspond by POSITION, but the pipeline's rows are name-keyed maps. Before
// the alignment, `SELECT a FROM t UNION SELECT b FROM u` deduped nothing (no
// two maps shared a key) and the batch built from the first arm's schema read
// the second arm's values under names it does not carry, writing NULLs.
func TestAlignSetOpRows(t *testing.T) {
	left := []parquet.Column{{Name: "a", Type: parquet.TypeInt64}, {Name: "b", Type: parquet.TypeString}}
	right := []parquet.Column{{Name: "x", Type: parquet.TypeInt64}, {Name: "y", Type: parquet.TypeString}}
	rows := []map[string]any{{"x": int64(1), "y": "one"}, {"x": int64(2), "y": "two"}}

	got := alignSetOpRows(left, right, rows)
	if len(got) != 2 {
		t.Fatalf("got %d rows, want 2", len(got))
	}
	if got[0]["a"] != int64(1) || got[0]["b"] != "one" {
		t.Errorf("row 0 = %v, want the right arm's values under the left arm's names", got[0])
	}
	if _, stale := got[1]["x"]; stale {
		t.Errorf("row 1 = %v still carries the right arm's own column names", got[1])
	}

	// Identical schemas are returned untouched (same backing slice).
	same := alignSetOpRows(left, left, rows)
	if &same[0] != &rows[0] {
		t.Error("matching schemas were needlessly re-keyed")
	}
	// A width mismatch is malformed, not something to paper over.
	if got := alignSetOpRows(left, right[:1], rows); &got[0] != &rows[0] {
		t.Error("mismatched widths were re-keyed")
	}
}

// TestSetOpIntersectExceptPlanShape: INTERSECT and EXCEPT lower onto the DAG
// as grouped counting (#346). Both arms concatenate through a StageUnion
// whose per-arm projections append two literal tag columns (arm 0 tags
// (1,0), arm 1 tags (0,1)); a grouped final_aggregate GROUP BYs the full
// result row and SUMs the tags, so each distinct row arrives at exactly one
// task carrying (countA, countB); the stage's SetOp marker makes the
// fragment emit rows per the operation's count rule. The distribution pass
// must insert an exchange-repartition on the full row between the two —
// co-partitioning is what makes each partition independently answerable.
func TestSetOpIntersectExceptPlanShape(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	cases := []struct {
		name    string
		sql     string
		op      string
		all     bool
		outCols []string
	}{
		{
			name:    "Intersect",
			sql:     "SELECT r_regionkey FROM region INTERSECT SELECT n_regionkey FROM nation",
			op:      "intersect",
			outCols: []string{"r_regionkey"},
		},
		{
			name:    "IntersectAll",
			sql:     "SELECT r_regionkey FROM region INTERSECT ALL SELECT n_regionkey FROM nation",
			op:      "intersect",
			all:     true,
			outCols: []string{"r_regionkey"},
		},
		{
			name:    "Except",
			sql:     "SELECT r_regionkey FROM region EXCEPT SELECT n_regionkey FROM nation",
			op:      "except",
			outCols: []string{"r_regionkey"},
		},
		{
			name:    "ExceptAll",
			sql:     "SELECT r_regionkey FROM region EXCEPT ALL SELECT n_regionkey FROM nation",
			op:      "except",
			all:     true,
			outCols: []string{"r_regionkey"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stages, err := planSetOpStages(t, cat, ctx, tc.sql)
			if err != nil {
				t.Fatalf("PlanDistributed refused: %v", err)
			}
			union := onlyStageOfType(t, stages, StageUnion)
			if len(union.UnionArms) != 2 {
				t.Fatalf("union has %d arms, want 2", len(union.UnionArms))
			}
			// Each arm carries the result columns plus the two tag columns,
			// and the tags are complementary constants: SUMming them per
			// group yields (rows from arm A, rows from arm B).
			for i, arm := range union.UnionArms {
				wantProj := len(tc.outCols) + 2
				if len(arm.Projections) != wantProj {
					t.Fatalf("arm %d projects %d columns, want %d (result cols + 2 tags): %+v",
						i, len(arm.Projections), wantProj, arm.Projections)
				}
				lTag := arm.Projections[len(tc.outCols)]
				rTag := arm.Projections[len(tc.outCols)+1]
				if lTag.Name != SetOpLeftCountCol || rTag.Name != SetOpRightCountCol {
					t.Fatalf("arm %d tag columns are (%q, %q), want (%q, %q)",
						i, lTag.Name, rTag.Name, SetOpLeftCountCol, SetOpRightCountCol)
				}
				wantL, wantR := "1", "0"
				if i == 1 {
					wantL, wantR = "0", "1"
				}
				if lTag.Expr != wantL || rTag.Expr != wantR {
					t.Errorf("arm %d tags are (%s, %s), want (%s, %s) — swapped tags turn EXCEPT into its mirror",
						i, lTag.Expr, rTag.Expr, wantL, wantR)
				}
			}

			// The counting aggregate: grouped on the full result row, SetOp
			// marker set, raw input (the exchange ships raw tagged rows, not
			// partials).
			var agg *Stage
			for i := range stages {
				if stages[i].SetOp != "" {
					if agg != nil {
						t.Fatalf("two SetOp stages in %v", stageTypeIDs(stages))
					}
					agg = &stages[i]
				}
			}
			if agg == nil {
				t.Fatalf("no SetOp counting stage in %v", stageTypeIDs(stages))
			}
			if agg.SetOp != tc.op || agg.SetOpAll != tc.all {
				t.Errorf("counting stage is (%s, all=%v), want (%s, all=%v)", agg.SetOp, agg.SetOpAll, tc.op, tc.all)
			}
			if got := agg.GroupByCols; !keysEqual(got, tc.outCols) {
				t.Errorf("counting stage groups by %v, want the full result row %v", got, tc.outCols)
			}
			if !agg.RawInputAggregate {
				t.Error("counting stage is not RawInputAggregate — merge mode would remap the SUM specs")
			}
			if len(agg.AggSpecs) != 2 ||
				agg.AggSpecs[0].InputCol != SetOpLeftCountCol || agg.AggSpecs[1].InputCol != SetOpRightCountCol {
				t.Errorf("counting stage aggregates %+v, want SUM over the two tag columns", agg.AggSpecs)
			}

			// Co-partitioning: an exchange-repartition on exactly the result
			// columns must sit between the union and the counting stage, so
			// equal rows (NULLs included — the hash marks NULL
			// deterministically) meet in one partition.
			var exch *Stage
			for i := range stages {
				if stages[i].Type == StageExchangeRepartition &&
					len(stages[i].Dependencies) == 1 && stages[i].Dependencies[0] == union.ID {
					exch = &stages[i]
				}
			}
			if exch == nil {
				t.Fatalf("no exchange-repartition over the union in %v — the arms are not co-partitioned", stageTypeIDs(stages))
			}
			if exch.Exchange == nil || !keysEqual(exch.Exchange.Keys, tc.outCols) {
				t.Errorf("exchange keys %v, want the full result row %v", exch.Exchange, tc.outCols)
			}
			if len(agg.Dependencies) != 1 || agg.Dependencies[0] != exch.ID {
				t.Errorf("counting stage depends on %v, want the exchange %s", agg.Dependencies, exch.ID)
			}

			// The gather answers from the counting stage, not an arm.
			gather := onlyStageOfType(t, stages, StageExchangeGather)
			if len(gather.Dependencies) != 1 || gather.Dependencies[0] != agg.ID {
				t.Errorf("gather depends on %v, want the counting stage %s", gather.Dependencies, agg.ID)
			}
		})
	}
}
