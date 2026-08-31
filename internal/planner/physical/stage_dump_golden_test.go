package physical

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// The 22-query STAGE DUMP, compared to a committed golden.
//
// TestTPCH_EnsureDistribution_Snapshot beside this one records a stage's ID,
// TYPE and DISTRIBUTION, and it is what caught a Q17 plan regression during
// the #700 arc. It records nothing about a stage's COLUMNS — so a change that
// widens a join's OutputFilter or an exchange's payload MANIFEST passes it
// untouched, and "the snapshot is green, so plans are identical" is a claim
// that snapshot cannot support. It was made, and it was false: the first cut
// of the #700 chain fix widened 21 stage lines across six queries while every
// existing gate stayed green.
//
// Bytes on the wire is a co-equal metric with wall time here (CLAUDE.md; the
// perf memos), and an exchange manifest IS bytes on the wire — two extra
// STRING columns on a Q07 shuffle are paid on every row of every task. So the
// column lists get a gate of their own: any plan change shows as a diff, and
// a widening has to be deliberate and reviewed rather than invisible.
//
// The golden is regenerated with `WADJET_WRITE_STAGE_GOLDEN=1 go test -run
// TestTPCHStageDumpGolden ./internal/planner/physical/`, and a regeneration
// is a reviewable diff in the same commit as whatever caused it.
//
// FOUR fields beyond `Columns`, each because a pass writes it and nothing
// gated it:
//
//   - `out=` is Stage.OutputColumns, the SHIPPED set pruneScanOutputColumns
//     narrows. It is a different list from Columns — a scan READS its pushed
//     filter's columns and does not ship them — so a widening that puts one
//     back was invisible here while the commit citing this gate claimed no
//     stage had gained a column. The claim was true and the citation did not
//     support it; now it does.
//   - `builds=` is the build side a chained or fused join and a union arm
//     NAME. It is the fourth place a stage names another stage, dispatch
//     resolves it through its own `inputs` map, and a rewiring pass that
//     misses it produces a plan that validates and then fails at dispatch
//     (#755).
//   - `chaincols=` is each chained link's OWN OutputFilter, a SECOND
//     narrowing inside one fragment: the primary probe applies Stage.Columns
//     and each link then applies its own to the joined stream its residual
//     filter reads. A column dropped there is a predicate that answers
//     UNKNOWN on every row.
//   - `ops=` already counted ProjectExprs, so a producer that gains an
//     absorbed projection shows even when every column list is unchanged.
func TestTPCHStageDumpGolden(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)

	build := func(t *testing.T, sql string) *logical.Node {
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

	var dump strings.Builder
	for qNum := 1; qNum <= 22; qNum++ {
		sql, ok := tpchPlanQueryMap[qNum]
		if !ok {
			continue
		}
		node := build(t, sql)
		planner := NewPlanner(cat)
		planner.WorkerCount = 4
		stages, err := planner.PlanDistributed(ctx, node)
		if err != nil {
			// A refusal is part of the shape and is recorded as one, so a
			// change that starts or stops routing a query local shows here.
			fmt.Fprintf(&dump, "Q%02d\tREFUSED\t%s\n", qNum, err)
			continue
		}
		for _, s := range stages {
			fmt.Fprintf(&dump, "Q%02d\t%s\t%s\tscanfiles=%d\tcols=%s\tout=%s\tdeps=%s\tbuilds=%s\tchaincols=%s\tops=%s\n",
				qNum, s.ID, s.Type, len(s.ScanFiles),
				sortedList(s.Columns), sortedList(s.OutputColumns), sortedList(s.Dependencies),
				stageBuildDeps(s), stageChainColumns(s), stageOpCounts(s))
		}
	}
	got := dump.String()

	const goldenPath = "testdata/tpch_stage_dump.golden"
	if os.Getenv("WADJET_WRITE_STAGE_GOLDEN") != "" {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatalf("mkdir testdata: %v", err)
		}
		if err := os.WriteFile(goldenPath, []byte(got), 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		t.Logf("golden rewritten: %d lines", strings.Count(got, "\n"))
		return
	}
	want, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("read golden (regenerate with WADJET_WRITE_STAGE_GOLDEN=1): %v", err)
	}
	if string(want) == got {
		return
	}
	wl := strings.Split(strings.TrimRight(string(want), "\n"), "\n")
	gl := strings.Split(strings.TrimRight(got, "\n"), "\n")
	var diff strings.Builder
	shown := 0
	for i := 0; i < len(wl) || i < len(gl); i++ {
		var a, b string
		if i < len(wl) {
			a = wl[i]
		}
		if i < len(gl) {
			b = gl[i]
		}
		if a == b {
			continue
		}
		if shown++; shown > 25 {
			diff.WriteString("  ... and more\n")
			break
		}
		fmt.Fprintf(&diff, "  -want %s\n  +got  %s\n", a, b)
	}
	t.Errorf("the TPC-H stage dump changed (%d golden lines, %d got).\n"+
		"A plan change is not automatically wrong, but it must be DELIBERATE: a wider join "+
		"OutputFilter or exchange manifest is bytes on the wire on every row of every task. "+
		"Review the diff, then regenerate with WADJET_WRITE_STAGE_GOLDEN=1 in the same commit.\n%s",
		len(wl), len(gl), diff.String())
}

// sortedList renders a string slice order-independently, so a pass that only
// reorders a column list does not read as a plan change.
func sortedList(in []string) string {
	if len(in) == 0 {
		return "[]"
	}
	cp := append([]string(nil), in...)
	sort.Strings(cp)
	return "[" + strings.Join(cp, " ") + "]"
}

// stageBuildDeps renders the build side a chained or fused join names, which
// is the FOURTH place a stage names another stage and the one a rewiring pass
// can leave dangling (#755). Dependencies alone cannot show it: dispatch
// resolves BuildDepStage through its own `inputs` map, so a pass that rewires
// the dependency list and not this field produces a plan that validates and
// then fails at dispatch.
func stageBuildDeps(s Stage) string {
	out := make([]string, 0, len(s.ChainedJoins)+len(s.FusedJoins))
	for _, cj := range s.ChainedJoins {
		out = append(out, "cj:"+cj.BuildDepStage)
	}
	for _, fj := range s.FusedJoins {
		out = append(out, "fj:"+fj.BuildDepStage)
	}
	for _, ua := range s.UnionArms {
		out = append(out, "ua:"+ua.DepStage)
	}
	return sortedList(out)
}

// stageChainColumns renders each chained link's own OutputFilter. It is a
// SECOND narrowing inside one fragment — the primary probe applies
// Stage.Columns and each link then applies its own — and it is what
// ensureJoinCarriesEvaluatedColumns widens for a link whose residual filter
// reads the build side. Bytes crossing a link are not bytes on the wire, but
// a column dropped here is a predicate that answers UNKNOWN, so the list is
// gated for the same reason the others are.
func stageChainColumns(s Stage) string {
	if len(s.ChainedJoins) == 0 {
		return "[]"
	}
	out := make([]string, 0, len(s.ChainedJoins))
	for i, cj := range s.ChainedJoins {
		out = append(out, fmt.Sprintf("%d:%s", i, sortedList(cj.Columns)))
	}
	return "[" + strings.Join(out, " ") + "]"
}

// stageOpCounts summarises the operator-bearing fields, so a stage that gains
// a filter, a projection, an aggregate or a fused/chained join shows up even
// when its column list is unchanged.
func stageOpCounts(s Stage) string {
	return fmt.Sprintf("f%d/p%d/a%d/w%d/fj%d/cj%d/sk%d",
		len(s.FilterExprs), len(s.ProjectExprs), len(s.AggSpecs), len(s.WindowCols),
		len(s.FusedJoins), len(s.ChainedJoins), len(s.SortKeys))
}
