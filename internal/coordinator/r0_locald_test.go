// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/google/uuid"

	"github.com/derekmwright/wadjet/internal/coordinator/dagplan"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// The localD seam (epic #1678 stage R0, ADR-0051): the statement is planned
// exactly as ExecuteSQL plans it for the stage DAG — the same preamble, the
// same veto, the same planStatement(stamp=false) call, so the same D the stage
// planner receives — and D is then executed single-process with
// runRefusedLocal's mechanics instead of being emitted.
//
// It is a TEST-ONLY seam: everything here is in a _test.go file, so no
// production binary can reach it, and it adds no line to ExecuteSQL. The price
// is that the preamble below is a copy of ExecuteSQL's; r0LocalD's caller
// guards the copy against drift by comparing the D text it planned with the
// plan text the DAG arm's own ExecuteSQL reports for the same statement
// (SQLResult.Plan), statement by statement.
//
// D is deep-copied before any physical planner touches it (the physical
// planners mutate a logical plan, statement_plan.go:63-66): the FROZEN copy is
// the specification later stages check against, a second copy is what
// PlanDistributed is given so the stage graph G is captured beside the run,
// and the original object is what runRefusedLocal consumes.

// r0LocalDRecord is what one localD run recorded.
type r0LocalDRecord struct {
	// Status: established, late-subplan (D carries a subquery the stage
	// planner plans AFTER D, as a separate plan D' — dagplan/
	// subquery_resolution.go:607-687 — or one the single-process planner
	// plans on its own; the run is not evidence about D alone), vetoed (the
	// bound validation refused), not-select, planning-failed,
	// refused-before-planning (the parse or a statement-level refusal).
	Status string
	// DText is D's PrettyPrint text before anything ran.
	DText string
	// Frozen is the deep copy of D taken before any physical planner ran.
	Frozen *logical.Node
	// FrozenText is Frozen's PrettyPrint AFTER the run: equal to DText when
	// the copy shares no mutable node with what was executed.
	FrozenText string
	// MutatedByLocal reports whether the single-process physical planner
	// changed the executed D object's text (evidence for the freeze).
	MutatedByLocal bool
	// SharedPointers lists the pointer types reachable from both the frozen
	// copy and the executed original (must be empty).
	SharedPointers []string
	// DAGRoute names why the stage DAG would route this statement to L, or
	// "" when it would execute the DAG (a routed statement's DAG answer is
	// L's, not evidence about the DAG).
	DAGRoute string
	// DAGPlanErr is PlanDistributed's own error on the copy, if any.
	DAGPlanErr string
	// Stages is the stage graph PlanDistributed produced from the copy: G as
	// planned (the dispatch-time rewrites and the merge are not in it).
	Stages []dagplan.Stage
	// BoundRefs counts the column references in D that carry a binder stamp
	// (plansql.ColRef.Bound). D is planned unbound, so it is 0; the bound
	// plan L carries them, which is how the seam's negative control tells L
	// from D when their texts print alike.
	BoundRefs int
	// LateSubplan names why the status is late-subplan.
	LateSubplan string
	// PlanTime is planStatement's wall time for D (parse to optimized,
	// enforced plan); StagePlanTime is PlanDistributed's on the copy.
	PlanTime, StagePlanTime time.Duration
	// Options, Snapshot and Policy record the planning state used.
	Options  string
	Snapshot string
	Policy   string
}

// r0LocalDFault is a test-only fault for the seam's own negative control:
// "bound" plans L (stamp=true) where the seam must plan D.
var r0LocalDFault atomic.Value // string

// r0LocalD plans sql as ExecuteSQL plans it for the stage DAG and runs that D
// on the coordinator-local single-process pipeline.
func r0LocalD(c *Coordinator, ctx context.Context, sql string) (res *SQLResult, rec *r0LocalDRecord, err error) {
	stamp := false
	if f, _ := r0LocalDFault.Load().(string); f == "bound" {
		stamp = true
	}
	return r0Statement(c, ctx, sql, stamp, false)
}

// r0Statement is the seam's body. stamp plans the statement BOUND where
// ExecuteSQL plans D unbound (the test-only binding switch of
// r0_boundd_test.go, and the seam's own fault); execDAG executes the captured
// stage graph on the cluster instead of running the plan single-process (the
// replica of ExecuteSQL's tail, r0ExecuteStages), so a bound D can be put on
// the stage DAG without a production change.
func r0Statement(c *Coordinator, ctx context.Context, sql string, stamp, execDAG bool) (res *SQLResult, rec *r0LocalDRecord, err error) {
	rec = &r0LocalDRecord{Status: "refused-before-planning"}
	// --- ExecuteSQL's preamble (coordinator.go:1035-1260), copied. ---
	ctx = expr.StartStatement(ctx)
	defer func() {
		if r := recover(); r != nil {
			err = exec.RecoverQueryPanic(ctx, "coordinator query", r)
		}
		err = sqlerr.Sentence(err)
	}()
	select {
	case c.querySem <- struct{}{}:
		defer func() { <-c.querySem }()
	case <-ctx.Done():
		return nil, rec, fmt.Errorf("query queue full: %w", ctx.Err())
	}
	start := time.Now()
	queryID := uuid.New().String()
	ctx = exec.WithQueryID(ctx, queryID)
	ctx = dagplan.WithManifestSnapshot(ctx, physical.NewPlannerForContext(ctx, c.catalog).ManifestSnapshot)
	if err := plansql.RefuseColumnValueCall(sql); err != nil {
		return nil, rec, err
	}
	parsed, err := plansql.Parse(sql)
	if err != nil {
		return nil, rec, err
	}
	switch parsed.Type {
	case plansql.QueryCreateSnapshot, plansql.QueryCreateAlert, plansql.QueryDropAlert, plansql.QueryAlterAlert, plansql.QueryExplain:
		rec.Status = "not-select"
		return nil, rec, errors.New("r0 localD: not a SELECT")
	}
	if err := plansql.RefuseUnsupportedStatement(parsed); err != nil {
		return nil, rec, err
	}
	ctx, _ = c.statementAuth(ctx)
	ctx = statementGuard.WithIncarnationGuard(ctx, c.catalog)
	defer func() {
		verr := statementGuard.VerifyIncarnations(ctx)
		if verr == nil || (err != nil && !statementGuard.IsIncarnationChanged(verr)) {
			return
		}
		res, err = nil, verr
	}()
	opts := c.statementPlanOptions(ctx)
	veto := c.validateBound(ctx, parsed)
	if err := checkTransition(ctx, transitionAfterVeto); err != nil {
		return nil, rec, err
	}
	switch veto.outcome {
	case vetoRefused:
		rec.Status = "vetoed"
		return nil, rec, veto.err
	case vetoAccepted:
		ctx = veto.ctx
	}
	if err := checkTransition(ctx, transitionDDirect); err != nil {
		return nil, rec, err
	}
	planStart := time.Now()
	dplan, err := c.planStatement(ctx, sql, stamp, opts)
	rec.PlanTime = time.Since(planStart)
	if err != nil {
		rec.Status = "planning-failed"
		return nil, rec, err
	}
	// --- The seam proper. ---
	rec.DText = r0PlanText(dplan.text)
	rec.Options = fmt.Sprintf("logical=%+v sortMergeJoinBytes=%d lateMaterialization=%v bushyJoinReorder=%v queryLimits=%v",
		opts.logical, opts.sortMergeJoinBytes, opts.lateMaterialization, opts.bushyJoinReorder, opts.queryLimits != nil)
	rec.Snapshot = r0SnapshotSummary(physical.NewPlannerForContext(dplan.ctx, c.catalog).ManifestSnapshot)
	rec.Policy = fmt.Sprintf("provider=%v columnPolicies=%d policyLookup=%v",
		statementAuthOf(dplan.ctx) != nil && statementAuthOf(dplan.ctx).Enabled(),
		len(logical.ColumnPoliciesFromContext(dplan.ctx)), logical.PolicyLookupFromContext(dplan.ctx) != nil)
	rec.Frozen = r0DeepCopy(dplan.node)
	gCopy := r0DeepCopy(dplan.node)
	rec.BoundRefs = r0CountBound(rec.Frozen)
	if r0Reaches(dplan.node, func(t reflect.Type) bool {
		return t == reflect.TypeOf(&plansql.SubqueryNode{}) || t == reflect.TypeOf(&plansql.ExistsNode{})
	}) {
		rec.LateSubplan = "D carries a subquery expression"
	}
	// G, as PlanDistributed plans it from the same D (ExecuteSQL's planner
	// configuration, coordinator.go:1322-1335), on its own copy.
	planner := dagplan.NewStagePlanner(physical.NewPlannerForContext(dplan.ctx, c.catalog))
	planner.WorkerCount = c.workers.Count()
	planner.BroadcastBytesThreshold = broadcastThresholdFromCluster(c.workers.MinWorkerPoolBudget())
	if c.config.BroadcastBytesOverride != 0 {
		planner.BroadcastBytesThreshold = c.config.BroadcastBytesOverride
	}
	planner.SortMergeJoinBytes = opts.sortMergeJoinBytes
	planner.LateMaterialization = opts.lateMaterialization
	planner.BushyJoinReorder = opts.bushyJoinReorder
	planner.DynamicFiltersEnabled = c.config.DynamicFilters
	planner.QueryLimits = opts.queryLimits
	stageStart := time.Now()
	stages, perr := planner.PlanDistributed(dplan.ctx, gCopy)
	rec.StagePlanTime = time.Since(stageStart)
	rec.Stages = stages
	if perr != nil {
		rec.DAGPlanErr = perr.Error()
		if c.dagRefusalRoute(perr) != nil {
			rec.DAGRoute = "refusal route: " + perr.Error()
		}
	} else {
		if verr := dagplan.ValidateNativeDAGShape(stages); verr != nil && errors.Is(verr, dagplan.ErrUnreachableGatherOutput) {
			rec.DAGRoute = "unreachable gather output"
		} else if _, catLoss := dagPGCategories(gCopy, stages); catLoss != "" {
			rec.DAGRoute = "category by name: " + catLoss
		}
		for _, s := range stages {
			if len(s.ScalarDependencies) > 0 {
				rec.LateSubplan = "stage " + s.ID + " has a deferred scalar producer"
				break
			}
		}
	}
	rec.Status = "established"
	if rec.LateSubplan != "" {
		rec.Status = "late-subplan"
	}
	if execDAG {
		rec.FrozenText = r0PlanText(rec.Frozen.PrettyPrint(0))
		if rec.DAGRoute != "" || perr != nil {
			// A routed statement is answered from L, which is not the
			// question this mode asks: report the route, run nothing.
			return nil, rec, perr
		}
		res, err = r0ExecuteStages(c, dplan.ctx, queryID, sql, start, gCopy, stages)
		return res, rec, err
	}
	var counter atomic.Int64
	res, err = c.runRefusedLocal(queryID, dplan, start, errors.New("r0 localD: D on the local pipeline"), "r0-localD", &counter)
	rec.FrozenText = r0PlanText(rec.Frozen.PrettyPrint(0))
	rec.MutatedByLocal = r0PlanText(dplan.node.PrettyPrint(0)) != rec.DText
	rec.SharedPointers = r0SharedPointers(rec.Frozen, dplan.node)
	return res, rec, err
}

// r0PlanText is a plan's PrettyPrint with the pointer addresses it prints
// (an ORDER BY term's NULLS flag is a *bool) blanked, so two plans compare by
// content.
func r0PlanText(s string) string { return r0HexAddr.ReplaceAllString(s, "0x?") }

var r0HexAddr = regexp.MustCompile(`0x[0-9a-f]{6,}`)

// r0SnapshotSummary lists the tables a statement's manifest snapshot pinned
// and the revision of each (the snapshot's fields are unexported: read by
// reflection, test-only).
func r0SnapshotSummary(s *physical.ManifestSnapshot) string {
	if s == nil {
		return "none"
	}
	v := reflect.ValueOf(s).Elem().FieldByName("byTbl")
	v = reflect.NewAt(v.Type(), unsafe.Pointer(v.UnsafeAddr())).Elem()
	var parts []string
	iter := v.MapRange()
	for iter.Next() {
		e := iter.Value().Elem()
		rev := e.FieldByName("rev")
		rev = reflect.NewAt(rev.Type(), unsafe.Pointer(rev.UnsafeAddr())).Elem()
		parts = append(parts, fmt.Sprintf("%s@%d", iter.Key().String(), rev.Uint()))
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// r0DeepCopy copies v and everything reachable from it, keeping the sharing
// structure inside the copy (a node reachable twice is copied once) and
// sharing nothing with the original except functions, channels and unsafe
// pointers (which r0SharedPointers does not count: they are code or
// synchronization, not plan state).
func r0DeepCopy[T any](v T) T {
	src := reflect.New(reflect.TypeOf(&v).Elem()).Elem()
	src.Set(reflect.ValueOf(&v).Elem())
	dst := reflect.New(src.Type()).Elem()
	c := &r0Copier{seen: map[r0PtrKey]reflect.Value{}}
	c.copy(dst, src)
	return dst.Interface().(T)
}

type r0PtrKey struct {
	p uintptr
	t reflect.Type
}

type r0Copier struct{ seen map[r0PtrKey]reflect.Value }

// r0Open makes an addressable value settable even when it was reached through
// an unexported field.
func r0Open(v reflect.Value) reflect.Value {
	if v.CanSet() {
		return v
	}
	return reflect.NewAt(v.Type(), unsafe.Pointer(v.UnsafeAddr())).Elem()
}

func (c *r0Copier) copy(dst, src reflect.Value) {
	switch src.Kind() {
	case reflect.Pointer:
		if src.IsNil() {
			return
		}
		key := r0PtrKey{src.Pointer(), src.Type()}
		if p, ok := c.seen[key]; ok {
			dst.Set(p)
			return
		}
		np := reflect.New(src.Type().Elem())
		c.seen[key] = np
		c.copy(np.Elem(), r0Open(src.Elem()))
		dst.Set(np)
	case reflect.Struct:
		for i := 0; i < src.NumField(); i++ {
			c.copy(r0Open(dst.Field(i)), r0Open(src.Field(i)))
		}
	case reflect.Slice:
		if src.IsNil() {
			return
		}
		ns := reflect.MakeSlice(src.Type(), src.Len(), src.Cap())
		for i := 0; i < src.Len(); i++ {
			c.copy(ns.Index(i), r0Open(src.Index(i)))
		}
		dst.Set(ns)
	case reflect.Array:
		for i := 0; i < src.Len(); i++ {
			c.copy(r0Open(dst.Index(i)), r0Open(src.Index(i)))
		}
	case reflect.Map:
		if src.IsNil() {
			return
		}
		nm := reflect.MakeMapWithSize(src.Type(), src.Len())
		iter := src.MapRange()
		for iter.Next() {
			k := reflect.New(src.Type().Key()).Elem()
			kv := reflect.New(src.Type().Key()).Elem()
			kv.Set(iter.Key())
			c.copy(k, kv)
			e := reflect.New(src.Type().Elem()).Elem()
			ev := reflect.New(src.Type().Elem()).Elem()
			ev.Set(iter.Value())
			c.copy(e, ev)
			nm.SetMapIndex(k, e)
		}
		dst.Set(nm)
	case reflect.Interface:
		if src.IsNil() {
			return
		}
		inner := reflect.New(src.Elem().Type()).Elem()
		inner.Set(src.Elem())
		out := reflect.New(inner.Type()).Elem()
		c.copy(out, inner)
		dst.Set(out)
	default:
		dst.Set(src)
	}
}

// r0Walk visits every pointer, map and slice backing store reachable from v,
// once, calling f with its address and type.
func r0Walk(v any, f func(addr uintptr, t reflect.Type)) {
	r0WalkValues(v, func(x reflect.Value) { f(x.Pointer(), x.Type()) })
}

// r0WalkValues is r0Walk handing over the pointer, map or slice value itself.
func r0WalkValues(v any, f func(x reflect.Value)) {
	seen := map[r0PtrKey]bool{}
	var walk func(reflect.Value)
	walk = func(x reflect.Value) {
		switch x.Kind() {
		case reflect.Pointer:
			if x.IsNil() {
				return
			}
			key := r0PtrKey{x.Pointer(), x.Type()}
			if seen[key] {
				return
			}
			seen[key] = true
			if x.Type().Elem().Size() > 0 {
				f(x)
			}
			walk(r0Open(x.Elem()))
		case reflect.Struct:
			for i := 0; i < x.NumField(); i++ {
				walk(r0Open(x.Field(i)))
			}
		case reflect.Slice:
			if x.IsNil() || x.Cap() == 0 {
				return
			}
			key := r0PtrKey{x.Pointer(), x.Type()}
			if !seen[key] && x.Type().Elem().Size() > 0 {
				seen[key] = true
				f(x)
			}
			for i := 0; i < x.Len(); i++ {
				walk(r0Open(x.Index(i)))
			}
		case reflect.Array:
			for i := 0; i < x.Len(); i++ {
				walk(r0Open(x.Index(i)))
			}
		case reflect.Map:
			if x.IsNil() {
				return
			}
			key := r0PtrKey{x.Pointer(), x.Type()}
			if seen[key] {
				return
			}
			seen[key] = true
			f(x)
			iter := x.MapRange()
			for iter.Next() {
				k := reflect.New(x.Type().Key()).Elem()
				k.Set(iter.Key())
				walk(k)
				e := reflect.New(x.Type().Elem()).Elem()
				e.Set(iter.Value())
				walk(e)
			}
		case reflect.Interface:
			if x.IsNil() {
				return
			}
			inner := reflect.New(x.Elem().Type()).Elem()
			inner.Set(x.Elem())
			walk(inner)
		}
	}
	root := reflect.New(reflect.TypeOf(v)).Elem()
	root.Set(reflect.ValueOf(v))
	walk(root)
}

// r0SharedPointers lists the types of the pointers, maps and slice backing
// stores reachable from both a and b.
func r0SharedPointers(a, b any) []string {
	inA := map[uintptr]reflect.Type{}
	r0Walk(a, func(p uintptr, t reflect.Type) { inA[p] = t })
	shared := map[string]bool{}
	r0Walk(b, func(p uintptr, t reflect.Type) {
		if _, ok := inA[p]; ok {
			shared[t.String()] = true
		}
	})
	var out []string
	for t := range shared {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// r0Reaches reports whether a pointer of a type match accepts is reachable
// from v.
func r0Reaches(v any, match func(reflect.Type) bool) bool {
	found := false
	r0Walk(v, func(_ uintptr, t reflect.Type) {
		if match(t) {
			found = true
		}
	})
	return found
}

// r0CountBound counts the plansql.ColRef values reachable from v whose Bound
// stamp is set.
func r0CountBound(v any) int {
	n := 0
	colRef := reflect.TypeOf(&plansql.ColRef{})
	r0WalkValues(v, func(x reflect.Value) {
		if x.Type() == colRef && x.Interface().(*plansql.ColRef).Bound != nil {
			n++
		}
	})
	return n
}
