// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"strings"

	"github.com/derekmwright/wadjet/internal/coordinator/dagplan"
	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// PostgreSQL's numeric CATEGORY of the columns a native-DAG query's stages
// materialize, for every expression a stage compiles from its text (#381).
//
// ROUND and the integer cast round by their operand's PostgreSQL type —
// half to even over a float8, half away from zero over a numeric — and a
// column a previous stage wrote is a bare FLOAT64 to the stage reading it
// whether it holds `5 / 2.0` (numeric to PostgreSQL) or a float8. The
// single-process planner hands each operator its input's categories from
// the plan; a stage's text has only names, so the coordinator folds the
// plan's categories by name (physical.PlanPGCategories), adds the names the
// stage planner gives aggregate and window outputs (`__agg_0`), and every
// task of the query carries the map (Scheduler.PublishTasks), as does the
// gather's own evaluation of the SELECT list (newBatchRenamer).
//
// A name the plan emits under two categories is left out, and a reader
// keeps the carrier's reading — the batch's own type.

type queryPGCategoriesKey struct{}

// withQueryPGCategories carries the query's category map on the dispatch
// context, the way the plan-time delete markers ride it (#491): a retry
// re-publishing through Scheduler.PublishTasks is stamped again.
func withQueryPGCategories(ctx context.Context, cats map[string]uint8) context.Context {
	if len(cats) == 0 {
		return ctx
	}
	return context.WithValue(ctx, queryPGCategoriesKey{}, cats)
}

func queryPGCategoriesFromContext(ctx context.Context) map[string]uint8 {
	if ctx == nil {
		return nil
	}
	m, _ := ctx.Value(queryPGCategoriesKey{}).(map[string]uint8)
	return m
}

// dagPGCategories is the map a native-DAG query's tasks carry: the plan's
// categories by name, plus each stage's aggregate and window outputs under
// the names the stage planner gave them.
func dagPGCategories(plan *logical.Node, stages []dagplan.Stage) map[string]uint8 {
	cats, planConflicts := physical.PlanPGCategories(plan)
	out := map[string]uint8{}
	// A name the plan's own fold found under two categories stays out: an
	// aggregate stage's output of that name (`avg(i) AS f` beside a float8
	// `f`) is one of the two, not the answer (review round 1, m_avg_union2).
	conflict := map[string]bool{}
	for name := range planConflicts {
		conflict[name] = true
	}
	note := func(name string, c expr.PGCategory) {
		name = strings.ToLower(strings.TrimSpace(name))
		if name == "" || c == expr.PGCatUnknown || conflict[name] {
			return
		}
		if prev, ok := out[name]; ok && prev != uint8(c) {
			conflict[name] = true
			delete(out, name)
			return
		}
		out[name] = uint8(c)
	}
	for name, c := range cats {
		note(name, c)
	}
	aggs := func(s *dagplan.Stage, specs []dagplan.AggSpec) {
		for _, a := range specs {
			if _, named := cats[strings.ToLower(a.OutputCol)]; named {
				continue
			}
			arg := a.InputExpr
			if arg == "" {
				arg = a.InputCol
			}
			note(a.OutputCol, physical.AggregatePGCategory(a.Func, arg, s.ScanSchema, cats))
		}
	}
	for i := range stages {
		s := &stages[i]
		aggs(s, s.AggSpecs)
		aggs(s, s.FusedAggSpecs)
		aggs(s, s.ChainedAggSpecs)
		for _, w := range s.WindowCols {
			if _, named := cats[strings.ToLower(w.OutputCol)]; named {
				continue
			}
			note(w.OutputCol, physical.AggregatePGCategory(w.Func, w.InputCol, s.ScanSchema, cats))
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// stampTaskPGCategories gives a task its query's category map, unless a
// dispatcher already set one.
func stampTaskPGCategories(t *distributed.Task, cats map[string]uint8) {
	if t == nil || len(cats) == 0 || t.PGCategories != nil {
		return
	}
	t.PGCategories = cats
}

// exprPGCategories is a task's or a query's map as the compiler takes it.
func exprPGCategories(cats map[string]uint8) map[string]expr.PGCategory {
	if len(cats) == 0 {
		return nil
	}
	out := make(map[string]expr.PGCategory, len(cats))
	for k, v := range cats {
		out[k] = expr.PGCategory(v)
	}
	return out
}

// gatherPGCategoriesOption is the query's map as the gather's own SELECT-list
// evaluation compiles it (newBatchRenamer).
func gatherPGCategoriesOption(ctx context.Context) expr.CompileOption {
	return expr.WithInputPGCategories(exprPGCategories(queryPGCategoriesFromContext(ctx)))
}
