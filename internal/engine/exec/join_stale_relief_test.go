// SPDX-License-Identifier: MIT

package exec

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #1706, line 1706k: a relief call the registry selected while a hash join
// was still building can land AFTER that join's Build returned — after its
// probe matched rows, or after its Close. The registry obtains the operator
// and calls its SpillSome outside the registry lock (RequestRelief,
// forceSpillLargestOwned), and Build's unregister does not wait for a call
// already selected. Nothing in these tests calls SpillSome: a second join's
// refused arrival reservation drives the registry, and the only test seam is
// memory.SetReliefCallHook, which holds the already-selected call until the
// victim reaches the chosen point of its lifecycle.
//
// The ten join kinds are those of the joinproof battery. Every expected digest
// was measured on PostgreSQL 17.11 (postgres:17-alpine, initdb --locale=C
// --encoding=UTF8, datcollate C) over the same generated tables; the SQL is in
// staleReliefPGSQL.

// staleKind is one join kind of the battery.
type staleKind struct {
	name     string
	jt       JoinType
	residual bool // ON residual (RIGHT/FULL)
	filter   bool // semi/anti filter (stored, not key-only)
}

var staleKinds = []staleKind{
	{name: "inner", jt: InnerJoin},
	{name: "left", jt: LeftJoin},
	{name: "semi_filter", jt: SemiJoin, filter: true},
	{name: "anti_filter", jt: AntiJoin, filter: true},
	{name: "right", jt: RightJoin},
	{name: "full", jt: FullOuterJoin},
	{name: "right_semi", jt: RightSemiJoin},
	{name: "right_anti", jt: RightAntiJoin},
	{name: "right_residual", jt: RightJoin, residual: true},
	{name: "full_residual", jt: FullOuterJoin, residual: true},
}

// staleDigest is the order-insensitive digest of an answer: rows, Σpid, Σbid,
// NULL pid count, NULL bid count, Σ((pid%9973)·(bid%9967)) over rows where
// both are non-NULL.
type staleDigest struct {
	rows, sumPid, sumBid, nullPid, nullBid, mix int64
}

func (d staleDigest) String() string {
	return fmt.Sprintf("rows=%d Σpid=%d Σbid=%d nullpid=%d nullbid=%d mix=%d",
		d.rows, d.sumPid, d.sumBid, d.nullPid, d.nullBid, d.mix)
}

// staleReliefPG holds PostgreSQL 17.11's answer for each kind (see
// staleReliefPGSQL; measured 2026-11-23, recorded in the 1706k result note).
var staleReliefPG = map[string]staleDigest{
	"inner":          {32000, 255984000, 351984000, 0, 0, 846612933254},
	"left":           {46000, 577977000, 351984000, 0, 14000, 846612933254},
	"semi_filter":    {16000, 127992000, 0, 0, 16000, 0},
	"anti_filter":    {14000, 321993000, 0, 0, 14000, 0},
	"right":          {36000, 255984000, 375982000, 4000, 0, 846612933254},
	"full":           {50000, 577977000, 375982000, 4000, 14000, 846612933254},
	"right_semi":     {16000, 0, 175992000, 16000, 0, 0},
	"right_anti":     {4000, 0, 23998000, 4000, 0, 0},
	"right_residual": {20000, 127992000, 199990000, 4000, 0, 423306269617},
	"full_residual":  {34000, 449985000, 199990000, 4000, 14000, 423306269617},
}

// staleReliefPGSQL is the PostgreSQL side of the oracle: the same tables the
// fixtures below generate, and one statement per kind.
const staleReliefPGSQL = `
CREATE TABLE p AS SELECT i::bigint AS pid, (i/2 + 2000)::bigint AS pk FROM generate_series(0, 29999) i;
CREATE TABLE b AS SELECT i::bigint AS bid, (i % 10000)::bigint AS bk, repeat('x', 32) AS pad FROM generate_series(0, 19999) i;
-- digest(pid, bid) = count(*), coalesce(sum(pid),0), coalesce(sum(bid),0),
--   count(*) FILTER (WHERE pid IS NULL), count(*) FILTER (WHERE bid IS NULL),
--   coalesce(sum((pid % 9973) * (bid % 9967)),0)
inner:          SELECT pid, bid FROM p JOIN b ON pk = bk
left:           SELECT pid, bid FROM p LEFT JOIN b ON pk = bk
semi_filter:    SELECT pid, NULL::bigint AS bid FROM p WHERE EXISTS (SELECT 1 FROM b WHERE bk = pk AND (pid + bid) % 3 <> 0)
anti_filter:    SELECT pid, NULL::bigint AS bid FROM p WHERE NOT EXISTS (SELECT 1 FROM b WHERE bk = pk AND (pid + bid) % 3 <> 0)
right:          SELECT pid, bid FROM p RIGHT JOIN b ON pk = bk
full:           SELECT pid, bid FROM p FULL JOIN b ON pk = bk
right_semi:     SELECT NULL::bigint AS pid, bid FROM b WHERE EXISTS (SELECT 1 FROM p WHERE pk = bk)
right_anti:     SELECT NULL::bigint AS pid, bid FROM b WHERE NOT EXISTS (SELECT 1 FROM p WHERE pk = bk)
right_residual: SELECT pid, bid FROM p RIGHT JOIN b ON pk = bk AND (pid + bid) % 3 <> 0
full_residual:  SELECT pid, bid FROM p FULL JOIN b ON pk = bk AND (pid + bid) % 3 <> 0
`

var (
	staleProbeSchema = []parquet.Column{
		{Name: "pid", Type: parquet.TypeInt64},
		{Name: "pk", Type: parquet.TypeInt64},
	}
	staleBuildSchema = []parquet.Column{
		{Name: "bid", Type: parquet.TypeInt64},
		{Name: "bk", Type: parquet.TypeInt64},
		{Name: "pad", Type: parquet.TypeString},
	}
	// The requester: a second grace build whose first arrival does not fit.
	staleReqSchema = []parquet.Column{
		{Name: "rid", Type: parquet.TypeInt64},
		{Name: "rk", Type: parquet.TypeInt64},
		{Name: "rpad", Type: parquet.TypeString},
	}
)

const (
	staleBuildN = 20000
	staleProbeN = 30000
	staleReqN   = 2048
)

func staleProbeRows() []map[string]any {
	rows := make([]map[string]any, staleProbeN)
	for i := range rows {
		rows[i] = map[string]any{"pid": int64(i), "pk": int64(i/2 + 2000)}
	}
	return rows
}

func staleBuildRows() []map[string]any {
	rows := make([]map[string]any, staleBuildN)
	for i := range rows {
		rows[i] = map[string]any{"bid": int64(i), "bk": int64(i % 10000), "pad": strings.Repeat("x", 32)}
	}
	return rows
}

func staleReqRows(padLen int) []map[string]any {
	rows := make([]map[string]any, staleReqN)
	pad := strings.Repeat("r", padLen)
	for i := range rows {
		rows[i] = map[string]any{"rid": int64(i), "rk": int64(i), "rpad": pad}
	}
	return rows
}

// staleResidual is (pid + bid) % 3 <> 0.
func staleResidual(probe *batch.RecordBatch, probeRow int, build *batch.RecordBatch, buildRow int) bool {
	pid := probe.Columns[probe.ColumnIndex("pid")].Int64Data[probeRow]
	bid := build.Columns[build.ColumnIndex("bid")].Int64Data[buildRow]
	return (pid+bid)%3 != 0
}

func newStaleJoin(k staleKind) *HashJoin {
	hj := NewHashJoin(k.jt, []string{"pk"}, []string{"bk"})
	if k.residual {
		hj.NewResidual = func() JoinResidual { return staleResidual }
	}
	if k.filter {
		hj.SemiAntiFilter = staleResidual
	}
	hj.BuildSchemaHint = staleBuildSchema
	hj.ProbeSchemaHint = staleProbeSchema
	return hj
}

// staleFixture is one data shape the schedules run over.
type staleFixture struct {
	probeSchema []parquet.Column
	buildRows   func() []map[string]any
	probeRows   func() []map[string]any
	newJoin     func(staleKind) *HashJoin
	pg          map[string]staleDigest
	// reqPad is the requester's row pad (its first-arrival size).
	reqPad int
	// tailRoom sizes the budget so the victim finishes its build without
	// evicting (the key-side repair rebuilds only an un-evicted build).
	tailRoom bool
	// afterBuild runs between the victim's Build and its probe, where the
	// planner's build goroutine runs it (join_plan.go, deferred build).
	afterBuild func(*testing.T, *HashJoin)
	// fault, when set, places a file-system obstacle in the spill
	// directory just before the held call lands and lifts it once the call
	// returned (join_relief_fault_test.go).
	fault *staleFault
}

var staleMain = staleFixture{
	probeSchema: staleProbeSchema,
	buildRows:   staleBuildRows,
	probeRows:   staleProbeRows,
	newJoin:     newStaleJoin,
	pg:          staleReliefPG,
	reqPad:      400,
}

// The key-side repair fixture: the plan put the build column on the probe side
// of the key pair, so FixKeyAssignment swaps the pair after the build and —
// because nothing was evicted — rebuilds the grace build's 64-part index FLAT
// into parts[0] (join.go). Partition 0 is made the largest (3,000 build rows
// share key 0, whose spill partition is 0), so a relief call landing after the
// rebuild evicts partition 0 first, and freeIndexPart(0) now frees the whole
// index.
var (
	staleFixKeyProbeSchema = []parquet.Column{
		{Name: "pid", Type: parquet.TypeInt64},
		{Name: "bk", Type: parquet.TypeInt64},
	}
	staleFixKey = staleFixture{
		probeSchema: staleFixKeyProbeSchema,
		buildRows: func() []map[string]any {
			rows := make([]map[string]any, staleBuildN)
			for i := range rows {
				k := int64(i)
				if i < 3000 {
					k = 0
				}
				rows[i] = map[string]any{"bid": int64(i), "bk": k, "pad": strings.Repeat("x", 32)}
			}
			return rows
		},
		probeRows: func() []map[string]any {
			rows := make([]map[string]any, staleProbeN)
			for i := range rows {
				rows[i] = map[string]any{"pid": int64(i), "bk": int64(i / 2)}
			}
			return rows
		},
		newJoin: func(k staleKind) *HashJoin {
			hj := newStaleJoin(k)
			hj.LeftKeys, hj.RightKeys = []string{"bk"}, []string{"src.bk"}
			hj.ProbeSchemaHint = staleFixKeyProbeSchema
			return hj
		},
		pg:       staleFixKeyPG,
		reqPad:   3000,
		tailRoom: true,
		afterBuild: func(t *testing.T, hj *HashJoin) {
			if !hj.FixKeyAssignment() {
				t.Fatal("fixture: the key-side repair did not fire")
			}
			if len(hj.parts) != 1 {
				t.Fatalf("fixture: the key-side repair did not collapse the index (%d parts; a partition was evicted during the build)", len(hj.parts))
			}
		},
	}
)

// staleFixKeyPG is PostgreSQL 17.11's answer over the key-side repair fixture:
//
//	CREATE TABLE p AS SELECT i::bigint AS pid, (i/2)::bigint AS pk FROM generate_series(0, 29999) i;
//	CREATE TABLE b AS SELECT i::bigint AS bid, (CASE WHEN i < 3000 THEN 0 ELSE i END)::bigint AS bk,
//	  repeat('x', 32) AS pad FROM generate_series(0, 19999) i;
//
// with the statements of staleReliefPGSQL.
var staleFixKeyPG = map[string]staleDigest{
	"inner":          {30000, 431991000, 224985000, 0, 0, 706945215537},
	"left":           {35998, 449987999, 224985000, 0, 5998, 706945215537},
	"semi_filter":    {12002, 216000001, 0, 0, 12002, 0},
	"anti_filter":    {17998, 233984999, 0, 0, 17998, 0},
	"right":          {35000, 431991000, 312482500, 5000, 0, 706945215537},
	"full":           {40998, 449987999, 312482500, 5000, 5998, 706945215537},
	"right_semi":     {15000, 0, 112492500, 15000, 0, 0},
	"right_anti":     {5000, 0, 87497500, 5000, 0, 0},
	"right_residual": {21000, 216002000, 201489500, 5000, 0, 353452516266},
	"full_residual":  {38998, 449986999, 201489500, 5000, 17998, 353452516266},
}

func digestRows(rows []map[string]any) staleDigest {
	var d staleDigest
	for _, r := range rows {
		d.rows++
		pid, pok := r["pid"].(int64)
		bid, bok := r["bid"].(int64)
		if pok {
			d.sumPid += pid
		} else {
			d.nullPid++
		}
		if bok {
			d.sumBid += bid
		} else {
			d.nullBid++
		}
		if pok && bok {
			d.mix += (pid % 9973) * (bid % 9967)
		}
	}
	return d
}

// runStaleProbe drives the probe the way the planner's sources do: the
// pipeline (which drains spilled partitions through NextFlush), then, for
// RIGHT SEMI / RIGHT ANTI, the resident partitions' matched / unmatched build
// rows (physical.rightSemiFlushSource).
func runStaleProbe(hj *HashJoin, src Source) ([]map[string]any, error) {
	probe := hj.Probe()
	sink := &CollectSink{}
	pipe := &Pipeline{Source: src, Ops: []UnaryOperator{probe}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		return nil, err
	}
	rows := sink.Rows
	var tail *batch.RecordBatch
	switch hj.JoinType {
	case RightSemiJoin:
		tail = probe.FlushMatched()
	case RightAntiJoin:
		tail = probe.FlushAntiMatched()
	}
	if tail != nil && tail.ActiveLen() > 0 {
		ts := &CollectSink{}
		ctx := context.Background()
		if err := ts.Init(ctx); err != nil {
			return nil, err
		}
		if err := ts.Consume(ctx, tail); err != nil {
			return nil, err
		}
		if err := ts.Finalize(ctx); err != nil {
			return nil, err
		}
		rows = append(rows, ts.Rows...)
	}
	return rows, nil
}

// gatedSource calls before(i) ahead of the i-th Next (0-based).
type gatedSource struct {
	inner  Source
	before func(i int)
	n      int
}

func (g *gatedSource) Init(ctx context.Context) error { return g.inner.Init(ctx) }
func (g *gatedSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	if g.before != nil {
		g.before(g.n)
	}
	g.n++
	return g.inner.Next(ctx)
}
func (g *gatedSource) Close() error { return g.inner.Close() }

// staleTiming is where the held relief call lands in the victim's lifecycle.
type staleTiming string

const (
	// landDuringBuild: the call lands while the victim is still building —
	// the eviction the registry exists for; it must still evict.
	landDuringBuild staleTiming = "during_build"
	// landAfterBuild: Build returned (unregistered), probe not started.
	landAfterBuild staleTiming = "after_build"
	// landAfterProbeStart: the probe has matched half its input, quiescent.
	landAfterProbeStart staleTiming = "after_probe_start"
	// landDuringProbe: released while the probe keeps running (concurrent).
	landDuringProbe staleTiming = "during_probe"
	// landAfterClose: the victim answered and was closed.
	landAfterClose staleTiming = "after_close"
	// landAtBuildEnd: released inside the victim's build tail (which holds
	// mu, so the call waits for it) and landing in the window after the
	// tail's unlock and before Build's deferred close takes mu again and
	// unregisters — the window in which the tail has already closed the
	// build's spill writers (#1706, line 1706o).
	landAtBuildEnd staleTiming = "at_build_end"
)

// staleOutcome is what one schedule produced.
type staleOutcome struct {
	victim      staleDigest
	victimErr   error
	reqErr      error
	reqPanic    any
	evicted     int // victim partitions the held call evicted; -1 after Close (bookkeeping cleared)
	usedAtClose int64
	// filesAtClose counts what is left in the spill directory once both
	// joins closed; landed is the fault schedules' consistency check of the
	// victim and the tracker right after the held call returned ("" = held).
	filesAtClose int
	landed       string
}

// spillDirEntries counts the entries left in a spill directory.
func spillDirEntries(t *testing.T, dir string) int {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	return len(ents)
}

// landedConsistency checks, right after a held relief call returned and the
// requester's Build ended, that the victim's bookkeeping, what it published to
// the registry and the tracker agree. The victim is parked at an arrival
// boundary (it holds no lock) and the requester has returned.
func landedConsistency(victim, req *HashJoin, tr *memory.Tracker) string {
	victim.mu.Lock()
	defer victim.mu.Unlock()
	var bad []string
	if victim.accInstanceID != 0 {
		if pub, own := tr.OwnedFor(victim.accInstanceID), victim.trackedMem+int64(cap(victim.keyBuf)); pub != own {
			bad = append(bad, fmt.Sprintf("victim published %d, owns %d", pub, own))
		}
	}
	if st := memory.OpState(victim.accState.Load()); st != memory.OpActive {
		bad = append(bad, fmt.Sprintf("victim registry state %v, want active", st))
	}
	if used, sum := tr.Used(), victim.trackedMem+req.trackedMem; used != sum {
		bad = append(bad, fmt.Sprintf("tracker used %d, the two joins track %d", used, sum))
	}
	if ss := victim.spillState; ss != nil {
		for p := range ss.spilledParts {
			if _, ok := ss.partBuildWriters[p]; !ok && len(ss.partBuildFiles[p]) == 0 {
				bad = append(bad, fmt.Sprintf("partition %d marked spilled with no run", p))
			}
		}
		for p, bs := range ss.partBuildBatches {
			if ss.spilledParts[p] && len(bs) > 0 {
				dropped := 0
				for i, pid := range ss.batchPartID {
					if pid == p && victim.buildBatches[i] == nil {
						dropped++
					}
				}
				bad = append(bad, fmt.Sprintf("partition %d marked spilled with %d batches resident, %d written and dropped", p, len(bs), dropped))
			}
		}
	}
	return strings.Join(bad, "; ")
}

// staleForceAt is the victim arrival the force schedule's first relief round
// lands on (the victim is mid-build there).
const staleForceAt = 4

// calibrateStale measures what the victim has charged when it reaches arrival
// at (0-based), and the requester's first-arrival cost.
func calibrateStale(t *testing.T, fx staleFixture, k staleKind, at int) (usedAt, usedFinal, reqCost int64) {
	t.Helper()
	tr := memory.NewTracker("calib", 1<<30)
	sm, err := memory.NewSpillManager(t.TempDir(), tr)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()
	hj := fx.newJoin(k)
	hj.Spill, hj.MemTracker = sm, tr
	src := &gatedSource{inner: NewSliceSource(staleBuildSchema, fx.buildRows()), before: func(i int) {
		if i == at {
			usedAt = tr.Used()
		}
	}}
	if err := hj.Build(context.Background(), src); err != nil {
		t.Fatalf("calibration build: %v", err)
	}
	usedFinal = tr.Used()
	hj.Close()
	reqCost = hashBuildBytes(batch.FromRows(staleReqSchema, staleReqRows(fx.reqPad)))
	return usedAt, usedFinal, reqCost
}

// runStaleRelief runs one schedule. A second grace build (the requester) is
// refused on its first arrival and asks the registry for relief while the
// victim is still building; the registry selects the victim through site; the
// hook holds that call; the victim moves to timing; the call lands.
//
// ReliefSiteRequest: the victim waits at its last arrival; the budget leaves
// the requester's arrival half its size short, so RequestRelief's first pass
// selects the victim.
//
// ReliefSiteForceLargest: RequestRelief's first pass must find nothing and the
// ledger must drift. The first relief round selects the victim through the
// first pass while the victim waits at arrival staleForceAt; the hook lets the
// victim absorb that arrival before the call lands, so the victim delivers less
// than half of what it claimed (its owned bytes grew under the eviction) and the
// registry rests it — the production cooldown. The requester still does not
// fit, asks again, the first pass is empty (victim resting, requester holding
// nothing), and the drift backstop picks the largest owner: the victim. The
// drift is a published owned count no operator reserved — an under-charged
// owner, which is what the backstop exists for.
func runStaleRelief(t *testing.T, fx staleFixture, k staleKind, site memory.ReliefSite, timing staleTiming) staleOutcome {
	t.Helper()
	holdAt := staleBuildN / batch.DefaultBatchSize // the last arrival
	if site == memory.ReliefSiteForceLargest {
		holdAt = staleForceAt
	}
	usedAt, usedFinal, reqCost := calibrateStale(t, fx, k, holdAt)
	budget := usedAt + reqCost/2
	if fx.tailRoom {
		// The victim's whole build stays under the 40% proactive-eviction
		// threshold (ShouldSpillFor(SpillCheap)), and the requester's first
		// arrival still does not fit beside the victim at its last arrival.
		budget = usedFinal * 100 / 38
		if reqCost <= budget-usedAt {
			t.Fatalf("fixture: the requester's arrival (%d) fits beside the victim (room %d)", reqCost, budget-usedAt)
		}
	}
	if site == memory.ReliefSiteForceLargest {
		budget = usedAt + reqCost - 32<<10 // first-pass gap of 32 KiB
	}

	dir, err := os.MkdirTemp("", "stale-relief")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	tr := memory.NewTracker("stale", budget)
	sm, err := memory.NewSpillManager(dir, tr)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()
	if site == memory.ReliefSiteForceLargest {
		phantom := memory.NextInstanceID()
		tr.PublishOwned(phantom, 8*budget)
		defer tr.UnpublishOwned(phantom)
	}

	victim := fx.newJoin(k)
	victim.Spill, victim.MemTracker = sm, tr
	victimSpilled := func() int {
		victim.mu.Lock()
		defer victim.mu.Unlock()
		if victim.spillState == nil {
			return 0
		}
		return len(victim.spillState.spilledParts)
	}

	gateHold := make(chan struct{}) // opens the held arrival
	atHold := make(chan struct{})   // victim reached the held arrival
	gateRest := make(chan struct{}) // force: opens the arrival after it
	atNext := make(chan struct{})   // force: victim reached the arrival after it
	buildDone := make(chan error, 1)
	go func() {
		src := &gatedSource{inner: NewSliceSource(staleBuildSchema, fx.buildRows()), before: func(i int) {
			switch {
			case i == holdAt:
				close(atHold)
				<-gateHold
			case i == holdAt+1 && site == memory.ReliefSiteForceLargest:
				close(atNext)
				<-gateRest
			}
		}}
		buildDone <- victim.Build(context.Background(), src)
	}()
	select {
	case <-atHold:
	case err := <-buildDone:
		t.Fatalf("victim build ended before arrival %d: %v", holdAt, err)
	case <-time.After(60 * time.Second):
		t.Fatalf("victim build never reached arrival %d", holdAt)
	}
	selected := make(chan struct{})
	release := make(chan struct{})
	var taken, firstRound atomic.Bool
	victimOp := memory.AccountedOperator(victim)
	restore := memory.SetReliefCallHook(func(s memory.ReliefSite, op memory.AccountedOperator) {
		if op != victimOp {
			return
		}
		if site == memory.ReliefSiteForceLargest && s == memory.ReliefSiteRequest && firstRound.CompareAndSwap(false, true) {
			// Round 1: the victim absorbs the held arrival before the
			// selected call lands, then waits at the next one.
			close(gateHold)
			<-atNext
			return
		}
		if s != site || !taken.CompareAndSwap(false, true) {
			return
		}
		close(selected)
		<-release
	})
	defer restore()

	var out staleOutcome
	req := NewHashJoin(InnerJoin, []string{"x"}, []string{"rk"})
	req.Spill, req.MemTracker = sm, tr
	reqDone := make(chan struct{})
	go func() {
		defer close(reqDone)
		defer func() { out.reqPanic = recover() }()
		out.reqErr = req.Build(context.Background(), NewSliceSource(staleReqSchema, staleReqRows(fx.reqPad)))
	}()
	select {
	case <-selected:
	case <-reqDone:
		t.Fatalf("schedule not reached: the requester finished without the registry selecting the victim "+
			"through %s (err=%v panic=%v, used=%d budget=%d)", site, out.reqErr, out.reqPanic, tr.Used(), budget)
	case <-time.After(60 * time.Second):
		t.Fatalf("schedule not reached: no relief call selected the victim through %s", site)
	}
	gateVictim := gateHold
	if site == memory.ReliefSiteForceLargest {
		gateVictim = gateRest
	}

	var spilledBefore int
	landAndWait := func() {
		closed := timing == landAfterClose
		if !closed {
			spilledBefore = victimSpilled()
		}
		if fx.fault != nil {
			fx.fault.place(t, sm.SpillDir())
		}
		close(release)
		<-reqDone
		if fx.fault != nil {
			fx.fault.lift()
			if !closed {
				out.landed = landedConsistency(victim, req, tr)
			}
		}
		out.evicted = -1
		if !closed {
			out.evicted = victimSpilled() - spilledBefore
		}
	}

	if timing == landDuringBuild {
		landAndWait()
	}
	if timing == landAtBuildEnd {
		restorePhase := SetPhaseHook(func(s PhaseSite, op any) {
			if op != victim {
				return
			}
			switch s {
			case PhaseJoinBuildTail:
				// The victim holds mu here: read its state directly.
				if victim.spillState != nil {
					spilledBefore = len(victim.spillState.spilledParts)
				}
				close(release)
			case PhaseJoinBuildEnd:
				// No lock is held: the released call takes mu now, in the
				// window, and the victim waits until it has landed.
				<-reqDone
				out.evicted = victimSpilled() - spilledBefore
			}
		})
		defer restorePhase()
	}
	close(gateVictim)
	if err := <-buildDone; err != nil {
		if fx.fault == nil {
			t.Fatalf("victim build: %v", err)
		}
		// Under an injected fault the victim's own Build failing is an
		// outcome, not a harness failure: it is the loud end the fault
		// test is looking for.
		out.victimErr = err
		victim.Close()
		req.Close()
		out.usedAtClose = tr.Used()
		out.filesAtClose = spillDirEntries(t, sm.SpillDir())
		return out
	}
	if fx.afterBuild != nil {
		fx.afterBuild(t, victim)
	}
	if timing == landAfterBuild {
		landAndWait()
	}

	half := (staleProbeN / batch.DefaultBatchSize) / 2
	probeSrc := &gatedSource{inner: NewSliceSource(fx.probeSchema, fx.probeRows()), before: func(i int) {
		if i != half {
			return
		}
		switch timing {
		case landAfterProbeStart:
			landAndWait()
		case landDuringProbe:
			spilledBefore = victimSpilled()
			close(release)
		}
	}}
	rows, err := runStaleProbe(victim, probeSrc)
	out.victimErr = err
	if timing == landDuringProbe {
		<-reqDone
		out.evicted = victimSpilled() - spilledBefore
	}
	if out.victimErr == nil {
		out.victim = digestRows(rows)
	}
	victim.Close()
	if timing == landAfterClose {
		landAndWait()
	}
	req.Close()
	out.usedAtClose = tr.Used()
	out.filesAtClose = spillDirEntries(t, sm.SpillDir())
	return out
}

var staleTimings = []staleTiming{landDuringBuild, landAfterBuild, landAfterProbeStart, landDuringProbe, landAfterClose}

// TestStaleReliefRequestRelief is the production schedule through
// RequestRelief's first pass, for every kind and every landing point.
func TestStaleReliefRequestRelief(t *testing.T) {
	for _, timing := range staleTimings {
		for _, k := range staleKinds {
			t.Run(string(timing)+"/"+k.name, func(t *testing.T) {
				out := runStaleRelief(t, staleMain, k, memory.ReliefSiteRequest, timing)
				checkStaleOutcome(t, staleMain, k, timing, out)
			})
		}
	}
}

// TestStaleReliefForceLargest is the same through the drift backstop
// (forceSpillLargestOwned).
func TestStaleReliefForceLargest(t *testing.T) {
	for _, timing := range staleTimings {
		for _, k := range staleKinds {
			t.Run(string(timing)+"/"+k.name, func(t *testing.T) {
				out := runStaleRelief(t, staleMain, k, memory.ReliefSiteForceLargest, timing)
				checkStaleOutcome(t, staleMain, k, timing, out)
			})
		}
	}
}

// TestStaleReliefAfterKeyRepair lands the held call after FixKeyAssignment
// collapsed the grace build's index into one part (join.go).
func TestStaleReliefAfterKeyRepair(t *testing.T) {
	// RequestRelief only: the force path's schedule rests the victim by
	// evicting from it during the build, and the repair rebuilds only a build
	// that evicted nothing. Both paths call the same SpillSome.
	for _, site := range []memory.ReliefSite{memory.ReliefSiteRequest} {
		for _, timing := range []staleTiming{landAfterBuild, landAfterProbeStart} {
			for _, k := range staleKinds {
				t.Run(string(site)+"/"+string(timing)+"/"+k.name, func(t *testing.T) {
					out := runStaleRelief(t, staleFixKey, k, site, timing)
					checkStaleOutcome(t, staleFixKey, k, timing, out)
				})
			}
		}
	}
}

// TestStaleReliefAtBuildEnd lands the held call in the window between the
// build tail's unlock and Build's deferred close (#1706, line 1706o): the tail
// has closed the build's spill writers by then, so it must also have ended
// relief eligibility under the same lock. A call landing there must evict
// nothing, must not fail on the closed writers, and the victim must still
// answer PostgreSQL's answer.
func TestStaleReliefAtBuildEnd(t *testing.T) {
	for _, k := range staleKinds {
		t.Run(k.name, func(t *testing.T) {
			out := runStaleRelief(t, staleMain, k, memory.ReliefSiteRequest, landAtBuildEnd)
			checkStaleOutcome(t, staleMain, k, landAtBuildEnd, out)
			if out.evicted != 0 {
				t.Errorf("the relief call landing after the build tail evicted %d of the victim's "+
					"partitions: its build writers were already closed", out.evicted)
			}
			if out.reqErr != nil && strings.Contains(out.reqErr.Error(), "already closed") {
				t.Errorf("the relief call reached the victim's closed build writers: %v", out.reqErr)
			}
		})
	}
}

var staleLogMu sync.Mutex

func checkStaleOutcome(t *testing.T, fx staleFixture, k staleKind, timing staleTiming, out staleOutcome) {
	t.Helper()
	staleLogMu.Lock()
	t.Logf("STALE %s/%s victim{%v err=%v} requester{err=%v panic=%v} victim_partitions_evicted_by_held_call=%d used_after_close=%d",
		timing, k.name, out.victim, out.victimErr, out.reqErr, out.reqPanic, out.evicted, out.usedAtClose)
	staleLogMu.Unlock()
	if out.reqPanic != nil {
		t.Errorf("the held relief call panicked in the requester: %v", out.reqPanic)
	}
	if out.victimErr != nil {
		t.Fatalf("victim: %v", out.victimErr)
	}
	if want := fx.pg[k.name]; out.victim != want {
		t.Errorf("victim answer %v, PostgreSQL 17.11: %v", out.victim, want)
	}
	if out.usedAtClose != 0 {
		t.Errorf("tracker holds %d bytes after both joins closed", out.usedAtClose)
	}
}
