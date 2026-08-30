package physical

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// The one-directionality gate for Stage.ConsumerScoped.
//
// The marker is set at exactly ONE site — filterCarrierIndex, when a
// consumer's predicate lands on a single-reference CTE body's terminal — and
// assertNoConsumerScopedFilterOnSharedStage trusts it, because ownership is
// knowable only where the attachment happens (ADR-0025). That trust is sound
// for the route the marker covers and says nothing about any OTHER route to a
// second consumer: shared-subplan dedup, a join fusion that folds two stages
// into one, an exchange dedup. A stage that gains its second consumer that way
// carries no marker, and the assert would wave it through.
//
// No SQL reaching such a stage with an unowned attachment is known today. This
// test is what makes that a CHECKED fact rather than a belief: it walks every
// plan the shape sweep emits and asserts, for each stage with two or more
// consumers that carries a Filter or a Project, that it is one of the two
// shapes the ownership rule permits — a scan's own pushed-down predicate, or a
// projection the PRODUCER owns (the aggregate-output projection on a CTE body,
// whose outputs are the stage's own columns) — or that the plan was refused.
//
// A future dedup route that creates a shared stage carrying a CONSUMER's
// attachment trips this without needing anyone to remember the rule.
func TestSharedProducerAttachmentsAreProducerOwned(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)

	plan := func(sql string) (stages []Stage, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("PANIC: %v", r)
			}
		}()
		parsed, perr := plansql.Parse(sql)
		if perr != nil {
			return nil, fmt.Errorf("parse: %w", perr)
		}
		info, ierr := plansql.ExtractSelect(parsed)
		if ierr != nil {
			return nil, fmt.Errorf("extract: %w", ierr)
		}
		node, lerr := logical.BuildFromSelect(info)
		if lerr != nil {
			return nil, fmt.Errorf("logical: %w", lerr)
		}
		annotate := func(n *logical.Node) { NewPlanner(cat).AnnotateScanColumns(ctx, n) }
		annotate(node)
		node = logical.Optimize(node, annotate)
		p := NewPlanner(cat)
		p.WorkerCount = 3
		return p.PlanDistributed(ctx, node)
	}

	var checked, shared int
	check := func(t *testing.T, name, sql string) {
		t.Helper()
		stages, err := plan(sql)
		if err != nil {
			// A refused plan is an answer: the coordinator routes it local.
			// Only an unexpected refusal is this sweep's business, and
			// TestStageShapePlacementSweep is what asserts that.
			return
		}
		checked++
		consumers := map[string]int{}
		for i := range stages {
			for _, dep := range stages[i].Dependencies {
				consumers[dep]++
			}
		}
		for i := range stages {
			s := &stages[i]
			if consumers[s.ID] < 2 {
				continue
			}
			if len(s.FilterExprs) == 0 && len(s.ProjectExprs) == 0 {
				continue
			}
			shared++
			if s.ConsumerScoped {
				t.Errorf("%s: stage %s (%s) has %d consumers and is marked ConsumerScoped, "+
					"which PlanDistributed should have refused\n  SQL: %s",
					name, s.ID, s.Type, consumers[s.ID], sql)
				continue
			}
			if why, ok := producerOwnsItsAttachments(s); !ok {
				t.Errorf("%s: stage %s (%s) has %d consumers and carries %s that the PRODUCER "+
					"does not own, with no ConsumerScoped marker — a route to a second consumer "+
					"other than the CTE one has appeared, and the ownership rule (ADR-0025) no "+
					"longer holds. Either mark it, or refuse it.\n  SQL: %s",
					name, s.ID, s.Type, consumers[s.ID], why, sql)
			}
		}
	}

	for pname, body := range sweepProducers() {
		for cname, tmpl := range sweepConsumers() {
			name := pname + "/" + cname
			check(t, name, fmt.Sprintf(tmpl, body))
		}
	}
	for name, sql := range sweepCTEShapes() {
		check(t, name, sql)
	}
	// The sweep is only useful if it REACHES the shape: a corpus that
	// produced no shared stage at all would pass vacuously.
	if checked == 0 {
		t.Fatal("no plan was produced; the sweep asserts nothing")
	}
	if shared == 0 {
		t.Fatal("no plan carried a Filter or a Project on a stage with two consumers, so this " +
			"gate asserted nothing — the CTE shapes that produce one have been lost")
	}
	t.Logf("%d plans, %d shared stages carrying an attachment", checked, shared)
}

// producerOwnsItsAttachments reports whether every Filter and Project on a
// shared stage belongs to the RELATION its consumers read, rather than to one
// of them, and names what it found when it does not.
//
// Two shapes qualify, and they are the two ADR-0025 names:
//
//   - a SCAN's own pushed-down predicate, which is part of the relation
//     (`WITH c AS (SELECT … WHERE id < 100)` referenced twice);
//   - a projection whose outputs are the stage's OWN columns, which is what
//     absorbAggregateOutputProjection puts on a CTE body that aggregates. A
//     consumer's SELECT list computes something the producer does not emit;
//     the body's own does not.
func producerOwnsItsAttachments(s *Stage) (string, bool) {
	if len(s.FilterExprs) > 0 && s.Type != StageScan {
		return fmt.Sprintf("a filter (%v)", s.FilterExprs), false
	}
	if len(s.ProjectExprs) == 0 {
		return "", true
	}
	own := map[string]bool{}
	for _, k := range s.GroupByCols {
		own[strings.ToLower(k)] = true
	}
	for _, a := range s.AggSpecs {
		if a.OutputCol != "" {
			own[strings.ToLower(a.OutputCol)] = true
		}
	}
	for _, k := range s.FusedAggGroupBy {
		own[strings.ToLower(k)] = true
	}
	for _, a := range s.FusedAggSpecs {
		if a.OutputCol != "" {
			own[strings.ToLower(a.OutputCol)] = true
		}
	}
	for _, c := range s.Columns {
		own[strings.ToLower(c)] = true
	}
	if len(own) == 0 {
		return "a projection on a stage with no output set to compare it to", false
	}
	var foreign []string
	for _, sp := range s.ProjectExprs {
		// A pass-through or a rename of one of the stage's OWN outputs is
		// the producer's; anything computed over something else is not.
		if src, ok := bareColumnRefName(sp.Expr); ok && own[strings.ToLower(src)] {
			continue
		}
		if own[strings.ToLower(sp.Expr)] {
			continue
		}
		foreign = append(foreign, fmt.Sprintf("%s AS %s", sp.Expr, sp.Name))
	}
	if len(foreign) == 0 {
		return "", true
	}
	sort.Strings(foreign)
	return fmt.Sprintf("a projection over columns it does not emit (%s)", strings.Join(foreign, ", ")), false
}

// TestSharedProducerRefusalStillFires is the other direction: the assert must
// still REFUSE a marked attachment on a shared stage, or the gate above is
// asserting a property nothing enforces.
func TestSharedProducerRefusalStillFires(t *testing.T) {
	stages := []Stage{
		{ID: "scan-0", Type: StageScan, Columns: []string{"k", "v"}},
		{ID: "sort-1", Type: StageSort, Dependencies: []string{"scan-0"},
			FilterExprs: []string{"v > 0"}, ConsumerScoped: true},
		{ID: "union-2", Type: StageUnion, Dependencies: []string{"sort-1", "sort-1"}},
	}
	err := assertNoConsumerScopedFilterOnSharedStage(stages)
	if err == nil {
		t.Fatal("a consumer-scoped filter on a stage with two consumers was accepted")
	}
	var spe *sharedProducerError
	if !errors.As(err, &spe) {
		t.Fatalf("refused with the wrong error type: %v", err)
	}
}
