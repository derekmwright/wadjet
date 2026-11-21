// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
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
	unbound, bound         r0Rendered
	unboundRec, boundRec   *r0LocalDRecord
	unboundErr, boundErr   error
	cause, stageFieldsDiff string
}

func r0RunBindingSwitch(c *Coordinator, ctx context.Context, sql string) r0BindingSwitchCell {
	var out r0BindingSwitchCell
	ures, urec, uerr := r0Statement(c, ctx, sql, false, true)
	bres, brec, berr := r0Statement(c, ctx, sql, true, true)
	out.unbound, out.bound = r0Render(ures, uerr), r0Render(bres, berr)
	out.unboundRec, out.boundRec, out.unboundErr, out.boundErr = urec, brec, uerr, berr
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
// PostgreSQL's answer (R0_BINDING_REPORT=<path>). Report mode: it asserts the
// control, not the movements.
func TestR0BindingSwitchReplicaIsExecuteSQL(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: stands clusters up")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	cells := r0ReadCells(t)
	var report []string
	moved := map[string]int{}
	for _, fx := range []string{"own", "uniq", "dup"} {
		c := r0Cluster(t, ctx, fx, nil)
		for _, cell := range cells {
			if !containsString(cell.fixtures, fx) {
				continue
			}
			ref := r0Render(c.ExecuteSQL(ctx, cell.sql))
			sw := r0RunBindingSwitch(c, ctx, cell.sql)
			routed := sw.unboundRec.DAGRoute != "" || sw.unboundRec.DAGPlanErr != ""
			if !routed && sw.unboundRec.Frozen != nil {
				if why := r0SameRendered(ref, sw.unbound, cell.mode == "ordered"); why != "" {
					t.Errorf("%s/%s: the replica (D unbound) differs from ExecuteSQL (%s):\n  replica %+v\n  ExecuteSQL %+v", fx, cell.name, why, sw.unbound, ref)
				}
			}
			same := r0SameRendered(sw.unbound, sw.bound, cell.mode == "ordered")
			if same == "" {
				continue
			}
			moved[sw.cause]++
			report = append(report, fmt.Sprintf("binding-switch\tcorpus/%s\t%s\tdiff=%s\tcause=%s\tunbound=%s\tbound=%s", fx, cell.name, same, sw.cause,
				r0RenderedText(sw.unbound), r0RenderedText(sw.bound)))
		}
	}
	t.Logf("corpus cells whose dag answer moves when D is bound, by cause: %v", moved)
	if p := os.Getenv("R0_BINDING_REPORT"); p != "" {
		r0AppendFile(t, p, strings.Join(report, "\n")+"\n")
	}
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
// the DAG answer with D unbound and with D bound, each matched against
// PostgreSQL's answer with the table's own matcher, and every transition
// classified by cause. Report mode (R0_BINDING_REPORT); asserts only the
// replica control.
func TestR0BindingSwitchGroupKeyTable(t *testing.T) {
	if testing.Short() || os.Getenv("R0_BINDING_REPORT") == "" {
		t.Skip("R0_BINDING_REPORT unset")
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
	render := func(c *Coordinator, sql string, stamp, ordered bool) (string, *r0LocalDRecord) {
		res, rec, err := r0Statement(c, ctx, sql, stamp, true)
		if err != nil {
			return "ERR " + sqlerr.StateOf(err) + " " + strings.ReplaceAll(err.Error(), "\n", " "), rec
		}
		if rec.DAGRoute != "" {
			return "ROUTED", rec
		}
		out, rerr := ssRenderResult(ctx, res, ordered)
		if rerr != nil {
			return "ERR " + sqlerr.StateOf(rerr) + " " + rerr.Error(), rec
		}
		return out, rec
	}
	trans := map[string]int{}
	var report []string
	for _, a := range []string{"dag", "dag-shuffled", "dag-morsel4"} {
		c := arms[a]
		for _, tc := range cells {
			want := answers[tc.name]
			u, urec := render(c, tc.sql, false, tc.ordered)
			b, brec := render(c, tc.sql, true, tc.ordered)
			if u == b {
				continue
			}
			verdict := func(s string) string {
				switch {
				case s == "ROUTED":
					return "routed"
				case ssMatches(s, want):
					return "right"
				case strings.HasPrefix(s, "ERR"):
					return "refused"
				}
				return "wrong"
			}
			tr := verdict(u) + "->" + verdict(b)
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
	t.Logf("group-key table, transitions when D is bound: %v", trans)
	r0AppendFile(t, os.Getenv("R0_BINDING_REPORT"), strings.Join(report, "\n")+"\n")
}
