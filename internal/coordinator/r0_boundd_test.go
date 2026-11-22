// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/coordinator/dagplan"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/worker"
)

// ssRenderResult renders a result as ssRunDAG does (the group-key table's
// renderer), for a result the replica produced.
func ssRenderResult(ctx context.Context, res *SQLResult, ordered bool) (string, error) {
	if res.Error != "" {
		return "", fmt.Errorf("%s", res.Error)
	}
	schema := res.OutputSchema()
	var cells [][]any
	st := res.Stream()
	if st == nil {
		rows, rerr := res.Rows()
		if rerr != nil {
			return "", rerr
		}
		for _, r := range rows {
			row := make([]any, len(res.Columns))
			for j, c := range res.Columns {
				row[j] = r[c]
			}
			cells = append(cells, row)
		}
	} else {
		defer st.Close()
		for {
			bb, berr := st.Next(ctx)
			if berr != nil {
				return "", berr
			}
			if bb == nil {
				break
			}
			cells = append(cells, bb.ToRowValues()...)
		}
	}
	return ssRender(schema, cells, ordered), nil
}

// R0's D2 evidence (epic #1678, ADR-0051 §2.6 / T1): what moves when the stage
// DAG is given a BOUND plan, reproduced with a TEST-ONLY switch and no
// production change. r0Statement(stamp=true, execDAG=true) plans the statement
// with auth.BindStatementColumns (as L is planned) where ExecuteSQL plans D
// unbound, hands that plan to PlanDistributed with ExecuteSQL's configuration,
// and executes the stage graph with r0ExecuteStages, a copy of ExecuteSQL's
// tail (coordinator.go:1385-1500). The replica is guarded by its own control:
// with stamp=false it must answer every corpus cell exactly as ExecuteSQL does.

// r0ExecuteStages is ExecuteSQL's tail after PlanDistributed, copied: the
// incarnation check, the categories, the dispatch, the DISTINCT dedup, the
// top-level LIMIT/OFFSET, and the result. It omits the ErrInputLost re-run and
// the streaming-disabled bookkeeping (neither changes an answer in-process).
func r0ExecuteStages(c *Coordinator, ctx context.Context, queryID, sql string, start time.Time, logicalPlan *logical.Node, physStages []dagplan.Stage) (*SQLResult, error) {
	queryCats, catLoss := dagPGCategories(logicalPlan, physStages)
	if catLoss != "" {
		return nil, fmt.Errorf("r0: category loss %s (the statement routes)", catLoss)
	}
	ctx = withQueryPGCategories(ctx, queryCats)
	if err := statementGuard.VerifyIncarnations(ctx); err != nil {
		return nil, err
	}
	gr, gerr := c.executeStageDAG(ctx, queryID, sql, physStages, c.workers.Count())
	if gerr != nil {
		if sqlerr.StateOf(gerr) != "" {
			return nil, gerr
		}
		return nil, fmt.Errorf("native DAG: %w", gerr)
	}
	if gr != nil {
		if mi := logical.ExtractMergeInfo(logicalPlan); mi != nil && mi.HasDistinct {
			if err := c.dedupGatherResult(gr, mi); err != nil {
				return nil, fmt.Errorf("distinct dedup: %w", err)
			}
		}
	}
	if gr != nil && gr.spillPath == "" {
		if mi := logical.ExtractMergeInfo(logicalPlan); mi != nil && (mi.HasLimit || mi.Offset > 0) {
			gr.batches = offsetBatches(gr.batches, mi.Offset)
			if mi.HasLimit {
				gr.batches = limitBatches(gr.batches, mi.Limit)
			}
			var total int64
			for _, b := range gr.batches {
				total += int64(b.ActiveLen())
			}
			gr.totalRows = total
		}
	}
	declared := schemaOrDeclared(gatherSchema(gr.batches), physStages)
	res := &SQLResult{
		QueryID:                  queryID,
		Columns:                  columnsOrDeclared(gr.columns, declared),
		TotalRows:                gr.totalRows,
		Elapsed:                  time.Since(start),
		Schema:                   declared,
		WireUnconstrainedDecimal: dagplan.GatherOutputWireUnconstrainedDecimal(physStages),
		StringLength:             dagplan.GatherOutputStringLength(physStages),
	}
	if gr.spillPath != "" {
		res.stream = newGatherReplayStream(gr.batches, gr.spillPath, gr.renamer)
	} else {
		res.Batches = gr.batches
	}
	return res, nil
}

// r0StageJSON is a stage graph as JSON, for telling WHICH stage fields a bound
// plan changed.
func r0StageJSON(stages []dagplan.Stage) []string {
	out := make([]string, len(stages))
	for i, s := range stages {
		s.ScanFiles, s.ScanFileSizes = nil, nil
		j, _ := json.Marshal(s)
		out[i] = string(j)
	}
	return out
}

// r0StageDiffFields names the top-level Stage fields that differ between two
// graphs of equal length, or "shape" when the graphs differ in length or
// stage types.
func r0StageDiffFields(a, b []dagplan.Stage) string {
	if len(a) != len(b) {
		return fmt.Sprintf("shape(%d vs %d stages)", len(a), len(b))
	}
	fields := map[string]bool{}
	for i := range a {
		if a[i].Type != b[i].Type {
			return "shape(stage types)"
		}
		var ma, mb map[string]json.RawMessage
		ja, _ := json.Marshal(a[i])
		jb, _ := json.Marshal(b[i])
		_ = json.Unmarshal(ja, &ma)
		_ = json.Unmarshal(jb, &mb)
		for k, v := range ma {
			if string(mb[k]) != string(v) {
				fields[k] = true
			}
		}
		for k := range mb {
			if _, ok := ma[k]; !ok {
				fields[k] = true
			}
		}
	}
	var f []string
	for k := range fields {
		f = append(f, k)
	}
	sort.Strings(f)
	return strings.Join(f, ",")
}

// r0BindingSwitchCell runs one statement on a DAG coordinator twice through
// the replica (D unbound, D bound) and classifies what moved.
type r0BindingSwitchCell struct {
	unbound, bound           r0Rendered
	unboundRoute, boundRoute string // the route the planning run took, "" when it did not route
	unboundRec, boundRec     *r0LocalDRecord
	unboundErr, boundErr     error
	cause, stageFieldsDiff   string
}

// r0RouteAnswer is what production answers for a statement whose stage plan
// ROUTES: every route runs the statement's bound plan L on the
// coordinator-local single-process pipeline (runRoute -> localPlans.forRoute
// -> planStatement(stamp=true) -> a runRefusedLocal wrapper,
// dag_routes.go:16-33, statement_plan.go:414-440), which is what
// r0Statement(stamp=true, execDAG=false) runs. L does not depend on whether D
// was bound, so both modes' routes answer from the same L.
func r0RouteAnswer(c *Coordinator, ctx context.Context, sql string) (*SQLResult, error) {
	res, _, err := r0Statement(c, ctx, sql, true, false)
	return res, err
}

// r0SwitchRun is one mode of the switch, ROUTE BEFORE ERROR: r0Statement
// returns the stage planner's refusal beside the route it names
// (r0_locald_test.go, execDAG), and a route is not a refusal: production runs
// it. The R0 review's item 8(b): the earlier renderer checked the error first
// and counted every planning-refusal route as a refusal.
func r0SwitchRun(c *Coordinator, ctx context.Context, sql string, stamp bool) (r0Rendered, string, *r0LocalDRecord, error) {
	res, rec, err := r0Statement(c, ctx, sql, stamp, true)
	if rec != nil && rec.DAGRoute != "" {
		return r0Render(r0RouteAnswer(c, ctx, sql)), rec.DAGRoute, rec, err
	}
	return r0Render(res, err), "", rec, err
}

func r0RunBindingSwitch(c *Coordinator, ctx context.Context, sql string) r0BindingSwitchCell {
	var out r0BindingSwitchCell
	out.unbound, out.unboundRoute, out.unboundRec, out.unboundErr = r0SwitchRun(c, ctx, sql, false)
	out.bound, out.boundRoute, out.boundRec, out.boundErr = r0SwitchRun(c, ctx, sql, true)
	urec, brec := out.unboundRec, out.boundRec
	// The cause, mechanically: validation (the bound planning run refused
	// where the unbound one planned, or the reverse); meaning (the optimized
	// logical plans print differently); route (one routes, the other not); a
	// stamp consumer (the plans print alike, so what differs is read off the
	// binder's stamps by a planner pass: the stage fields that changed name
	// it), or none (no stage field changed and the answers still differ:
	// execution nondeterminism, reported, never explained away).
	switch {
	case (urec.Frozen == nil) != (brec.Frozen == nil):
		out.cause = "validation"
	case urec.Frozen == nil:
		out.cause = "both-unplanned"
	case urec.DText != brec.DText:
		out.cause = "meaning(logical plan differs)"
	case (urec.DAGRoute != "") != (brec.DAGRoute != ""):
		out.cause = "route"
	default:
		out.stageFieldsDiff = r0StageDiffFields(urec.Stages, brec.Stages)
		if out.stageFieldsDiff == "" {
			out.cause = "none(identical stage graphs)"
		} else {
			out.cause = "stamp-consumer(" + out.stageFieldsDiff + ")"
		}
	}
	return out
}

// TestR0BindingSwitchReplicaIsExecuteSQL is the replica's control and the
// switch's corpus measurement: with D unbound the replica answers every
// corpus cell (on the dag arm's configuration) exactly as ExecuteSQL does; with
// D bound, every cell whose answer moves is reported with its cause, against
// PostgreSQL's answer (R0_BINDING_REPORT=1: r0_binding_report_corpus.tsv in a
// fresh directory whose path the test logs). Report mode: it asserts the
// control, not the movements.
func TestR0BindingSwitchReplicaIsExecuteSQL(t *testing.T) {
	r0RequireWriterUsage(t, "R0_BINDING_REPORT")
	if testing.Short() {
		t.Skip("-short: stands clusters up")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	cells := r0ReadCells(t)
	var report []string
	moved := map[string]int{}
	counts := map[string]int{}
	for _, fx := range []string{"own", "uniq", "dup"} {
		c := r0Cluster(t, ctx, fx, nil)
		for _, cell := range cells {
			if !containsString(cell.fixtures, fx) {
				continue
			}
			ordered := cell.mode == "ordered"
			before, _ := r0RouteSum(c)
			ref := r0Render(c.ExecuteSQL(ctx, cell.sql))
			after, _ := r0RouteSum(c)
			prodRouted := after != before
			sw := r0RunBindingSwitch(c, ctx, cell.sql)
			// The control, route-aware: an unbound replica that routes is
			// production routing (and answering from L); one that plans and
			// does not route is production executing the stage DAG.
			switch {
			case sw.unboundRoute != "":
				counts["unbound routed (route answer executed, compared with ExecuteSQL)"]++
				if !prodRouted {
					t.Errorf("%s/%s: the replica routes (%s) where ExecuteSQL does not", fx, cell.name, sw.unboundRoute)
				}
				if why := r0SameRendered(ref, sw.unbound, ordered); why != "" {
					t.Errorf("%s/%s: the replica's route answer differs from ExecuteSQL's (%s):\n  replica %+v\n  ExecuteSQL %+v", fx, cell.name, why, sw.unbound, ref)
				}
			case sw.unboundRec.Frozen != nil && sw.unboundRec.DAGPlanErr == "":
				counts["unbound executed on the stage DAG (compared with ExecuteSQL)"]++
				if prodRouted {
					t.Errorf("%s/%s: ExecuteSQL routes where the replica executes the stage DAG", fx, cell.name)
				}
				if why := r0SameRendered(ref, sw.unbound, ordered); why != "" {
					t.Errorf("%s/%s: the replica (D unbound) differs from ExecuteSQL (%s):\n  replica %+v\n  ExecuteSQL %+v", fx, cell.name, why, sw.unbound, ref)
				}
			default:
				counts["unbound not planned or stage-planning refused without a route (not compared)"]++
				if prodRouted {
					t.Errorf("%s/%s: ExecuteSQL routes where the replica reports no route", fx, cell.name)
				}
			}
			if sw.boundRoute != "" {
				counts["bound routed (route answer executed)"]++
			}
			same := r0SameRendered(sw.unbound, sw.bound, ordered)
			if same == "" && (sw.unboundRoute != "") == (sw.boundRoute != "") {
				continue
			}
			if same == "" {
				same = "route-only"
			}
			moved[sw.cause]++
			report = append(report, fmt.Sprintf("binding-switch\tcorpus/%s\t%s\tdiff=%s\tcause=%s\tunbound=%s%s\tbound=%s%s", fx, cell.name, same, sw.cause,
				r0RouteTag(sw.unboundRoute), r0RenderedText(sw.unbound), r0RouteTag(sw.boundRoute), r0RenderedText(sw.bound)))
		}
	}
	t.Logf("corpus (dag arm's configuration only): %v", counts)
	t.Logf("corpus cells whose dag answer or route moves when D is bound, by cause: %v", moved)
	r0WriteArtifact(t, "R0_BINDING_REPORT", "r0_binding_report_corpus.tsv", strings.Join(report, "\n")+"\n")
}

func r0RouteTag(route string) string {
	if route == "" {
		return ""
	}
	return "ROUTED(" + strings.NewReplacer("\t", " ", "\n", " ").Replace(route) + ") "
}

func r0RenderedText(r r0Rendered) string {
	if r.err != "" {
		return strings.NewReplacer("\t", " ", "\n", " ").Replace(r.err)
	}
	var rows []string
	for _, row := range r.rows {
		rows = append(rows, strings.Join(row, ","))
	}
	sort.Strings(rows)
	return fmt.Sprintf("%v %v [%s]", r.cols, r.types, strings.Join(rows, "; "))
}

// TestR0BindingSwitchGroupKeyTable reproduces the stage-5a spike's
// measurement ("binding D moves stage-DAG cells the wrong way", ADR-0050
// "Bind the one plan and give it to the stage planner too") on the group-key
// table's cells, on the three DAG arms, through the switch: for every cell,
// the DAG outcome with D unbound and with D bound, each matched against
// PostgreSQL's answer with the table's own matcher, and every transition
// classified by cause. ROUTE BEFORE ERROR: a mode whose stage plan routes is
// "routed", its answer is the route's (L run single-process, as production
// runs it) and is executed and matched too (routed-right / routed-wrong /
// routed-refused); a planning-refusal route is never counted as a refusal.
// Report mode (R0_BINDING_REPORT=1: r0_binding_report_gk.tsv in a fresh
// directory whose path the test logs); it asserts the production control: on
// every cell and arm the unbound mode's outcome (route or not, and the
// answer, or the SQLSTATE of a refusal) is ExecuteSQL's.
func TestR0BindingSwitchGroupKeyTable(t *testing.T) {
	r0RequireWriterUsage(t, "R0_BINDING_REPORT")
	if testing.Short() || !r0Writer(t, "R0_BINDING_REPORT") {
		t.Skip("R0_BINDING_REPORT not 1")
	}
	answers := gkPGAnswers(t)
	for name, a := range gkReadTSV(t, "testdata/arc_gk_group_key_spelling_more_pg17.tsv", 2) {
		answers[name] = a
	}
	cells := append(gkCells(), gkMoreCells(t)...)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	defer cancel()
	arms := map[string]*Coordinator{}
	for _, a := range []string{"dag", "dag-shuffled", "dag-morsel4"} {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, ssTables())
		switch a {
		case "dag":
			arms[a] = tmdCoordinator(t, ctx, infra)
		case "dag-shuffled":
			arms[a] = tmdCoordinator(t, ctx, infra, func(c *Config) { c.BroadcastBytesOverride = 1 })
		case "dag-morsel4":
			arms[a] = tmdCoordinatorWithWorkers(t, ctx, infra, func(w *worker.Config) { w.MorselWorkers = 4 })
		}
	}
	renderRes := func(res *SQLResult, err error, ordered bool) string {
		if err != nil {
			return "ERR " + sqlerr.StateOf(err) + " " + strings.ReplaceAll(err.Error(), "\n", " ")
		}
		out, rerr := ssRenderResult(ctx, res, ordered)
		if rerr != nil {
			return "ERR " + sqlerr.StateOf(rerr) + " " + strings.ReplaceAll(rerr.Error(), "\n", " ")
		}
		return out
	}
	// render is one mode's outcome: "ROUTED <route answer>" when the stage
	// plan routes (checked FIRST), else the DAG's answer or "ERR ...".
	render := func(c *Coordinator, sql string, stamp, ordered bool) (string, *r0LocalDRecord) {
		res, rec, err := r0Statement(c, ctx, sql, stamp, true)
		if rec != nil && rec.DAGRoute != "" {
			lres, lerr := r0RouteAnswer(c, ctx, sql)
			return "ROUTED " + renderRes(lres, lerr, ordered), rec
		}
		return renderRes(res, err, ordered), rec
	}
	// same compares two outcomes as the control does: route or not, then a
	// refusal by SQLSTATE (the replica's message omits ExecuteSQL's own
	// wrapping), an answer by its rendering.
	same := func(a, b string) bool {
		ar, br := strings.HasPrefix(a, "ROUTED "), strings.HasPrefix(b, "ROUTED ")
		if ar != br {
			return false
		}
		a, b = strings.TrimPrefix(a, "ROUTED "), strings.TrimPrefix(b, "ROUTED ")
		if strings.HasPrefix(a, "ERR ") || strings.HasPrefix(b, "ERR ") {
			sa := strings.SplitN(a, " ", 3)
			sb := strings.SplitN(b, " ", 3)
			return len(sa) > 1 && len(sb) > 1 && sa[0] == sb[0] && sa[1] == sb[1]
		}
		return a == b
	}
	// The table's own legal-nondeterminism cells (ADR-0013, the kept file's
	// "skip" lines: a LIMIT 1 with no ORDER BY picks any row): their answer
	// may differ run to run on every path, so the control compares only
	// whether they route and whether they refuse, and a difference between
	// the two modes is reported apart, never as a direction.
	skip := map[string]bool{}
	for name, k := range gkReadTSV(t, "testdata/arc_gk_group_key_spelling_kept.tsv", 2) {
		if strings.HasPrefix(k, "skip") {
			skip[name] = true
		}
	}
	shape := func(s string) string {
		r, routed := strings.CutPrefix(s, "ROUTED ")
		out := "answer"
		if strings.HasPrefix(r, "ERR ") {
			out = strings.Join(strings.SplitN(r, " ", 3)[:2], " ")
		}
		if routed {
			out = "ROUTED " + out
		}
		return out
	}
	trans := map[string]int{}
	control := map[string]int{}
	var report []string
	for _, a := range []string{"dag", "dag-shuffled", "dag-morsel4"} {
		c := arms[a]
		for _, tc := range cells {
			want := answers[tc.name]
			before, _ := r0RouteSum(c)
			pres, perr := c.ExecuteSQL(ctx, tc.sql)
			after, _ := r0RouteSum(c)
			prod := renderRes(pres, perr, tc.ordered)
			if after != before {
				prod = "ROUTED " + prod
			}
			u, urec := render(c, tc.sql, false, tc.ordered)
			b, brec := render(c, tc.sql, true, tc.ordered)
			if skip[tc.name] {
				if shape(u) == shape(prod) {
					control[a+" agrees (legal-nondeterminism cell: route and refusal only)"]++
				} else {
					control[a+" differs"]++
					t.Errorf("%s/%s: the replica's unbound outcome is not ExecuteSQL's in kind: %s vs %s", a, tc.name, shape(u), shape(prod))
				}
			} else if same(u, prod) {
				control[a+" agrees"]++
			} else {
				control[a+" differs"]++
				t.Errorf("%s/%s: the replica's unbound outcome is not ExecuteSQL's:\n  replica    %.300s\n  ExecuteSQL %.300s", a, tc.name, u, prod)
			}
			if u == b {
				continue
			}
			verdict := func(s string) string {
				prefix := ""
				if r, ok := strings.CutPrefix(s, "ROUTED "); ok {
					prefix, s = "routed-", r
				}
				switch {
				case ssMatches(s, want):
					return prefix + "right"
				case strings.HasPrefix(s, "ERR"):
					return prefix + "refused"
				}
				return prefix + "wrong"
			}
			tr := verdict(u) + "->" + verdict(b)
			if skip[tc.name] {
				tr = "legal-nondeterminism(" + shape(u) + "->" + shape(b) + ")"
			}
			cause := "?"
			switch {
			case (urec.Frozen == nil) != (brec.Frozen == nil):
				cause = "validation"
			case urec.Frozen != nil && urec.DText != brec.DText:
				cause = "meaning(logical plan differs)"
			case (urec.DAGRoute != "") != (brec.DAGRoute != ""):
				cause = "route"
			case urec.Frozen != nil:
				if f := r0StageDiffFields(urec.Stages, brec.Stages); f != "" {
					cause = "stamp-consumer(" + f + ")"
				} else {
					cause = "none(identical stage graphs)"
				}
			}
			trans[a+" "+tr]++
			report = append(report, fmt.Sprintf("binding-switch\tgk/%s\t%s\t%s\tcause=%s\tunbound=%s\tbound=%s\tpg=%s", a, tc.name, tr, cause,
				strings.ReplaceAll(u, "\t", " "), strings.ReplaceAll(b, "\t", " "), strings.ReplaceAll(want, "\t", " ")))
		}
	}
	t.Logf("group-key table, production control (unbound replica vs ExecuteSQL): %v", control)
	t.Logf("group-key table, transitions when D is bound: %v", trans)
	r0WriteArtifact(t, "R0_BINDING_REPORT", "r0_binding_report_gk.tsv", strings.Join(report, "\n")+"\n")
}
