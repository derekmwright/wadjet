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

// #1706, line 1706n: a relief call must not interleave with an operator phase
// that mutates relief-visible state while the operator is still registered —
// a Sort's or a HashAggregate's clone merge (Pipeline.runParallel and the
// worker's breaker consume call MergeSink after the workers end and before
// Finalize), and a partitioned HashJoin's build tail (after the last
// accumulator froze, before Build returns and closes relief).
//
// Nothing here calls SpillSome. A second operator's arrival that does not fit
// beside the victim drives SpillManager.RequestRelief; the test seams are
// memory.SetReliefCallHook (holds the selected call) and SetPhaseHook (holds
// the victim inside its phase and launches the requester from there).
//
// Two landings:
//   - quiescent: the victim waits, bounded, for the selected call to finish
//     while it is inside the phase. Finishing there is the defect: relief
//     reclaimed state the phase was consuming. With the phase under the
//     operator's lock the call cannot even be selected (rankCandidates reads
//     the victim's Inspect under that lock) and the wait times out.
//   - concurrent: the selected call is released and the phase continues at
//     once, so the two run side by side. Under -race that is a reported race
//     wherever the phase writes without the lock (not on every run: the
//     tracker's atomics order some pairs).
//
// Either way, the registry SELECTING the victim inside the phase is already
// the defect: it read the victim's footprint while the phase was rewriting it.
type phaseLanding string

const (
	landQuiescent  phaseLanding = "quiescent"
	landConcurrent phaseLanding = "concurrent"
)

var phaseLandings = []phaseLanding{landQuiescent, landConcurrent}

// phaseWait bounds each wait inside the phase. Only the fixed code waits it
// out (the relief call is held behind the lock); the defect shows in well
// under it.
const phaseWait = 500 * time.Millisecond

// phaseConcurrentHead is the head start a released call gets in the
// concurrent landing. It is not a synchronization: the race detector sees no
// ordering between the call and the rest of the phase either way.
const phaseConcurrentHead = 20 * time.Millisecond

// phaseOutcome is what the schedule observed.
type phaseOutcome struct {
	reached         atomic.Bool // the victim entered the phase and launched the requester
	selectedInPhase atomic.Bool // the registry selected the victim while it was in the phase
	reliefInPhase   atomic.Bool // the selected call FINISHED while the victim was still in the phase
}

// armPhaseSchedule installs both hooks for one victim. startReq launches the
// requester and returns a channel closed when it is done. The returned restore
// removes the hooks and frees any call still held.
func armPhaseSchedule(site PhaseSite, victim any, victimOp memory.AccountedOperator,
	landing phaseLanding, startReq func() <-chan struct{}) (*phaseOutcome, func()) {
	out := &phaseOutcome{}
	selected := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	free := func() { releaseOnce.Do(func() { close(release) }) }
	var armed, taken atomic.Bool

	restoreRelief := memory.SetReliefCallHook(func(_ memory.ReliefSite, op memory.AccountedOperator) {
		if op != victimOp || !taken.CompareAndSwap(false, true) {
			return
		}
		close(selected)
		<-release
	})
	restorePhase := SetPhaseHook(func(s PhaseSite, op any) {
		if s != site || op != victim || !armed.CompareAndSwap(false, true) {
			return
		}
		out.reached.Store(true)
		done := startReq()
		defer free()
		select {
		case <-selected:
		case <-time.After(phaseWait):
			return // the call is held behind the phase's lock
		}
		out.selectedInPhase.Store(true)
		free()
		if landing == landConcurrent {
			// Give the released call time to start before the phase goes
			// on, with no synchronization between them: the rest of the
			// phase then runs beside it, not after it.
			time.Sleep(phaseConcurrentHead)
			return
		}
		select {
		case <-done:
			out.reliefInPhase.Store(true)
		case <-time.After(phaseWait):
		}
	})
	return out, func() {
		restorePhase()
		restoreRelief()
		free()
	}
}

// startRequester launches a grace build whose single arrival (reqPad-padded
// rows, see staleReqRows) cannot fit beside the victim, so its first act is to
// ask the registry for relief.
func startRequester(sm *memory.SpillManager, tr *memory.Tracker, reqPad int, r *phaseRequester) func() <-chan struct{} {
	return func() <-chan struct{} {
		hj := NewHashJoin(InnerJoin, []string{"x"}, []string{"rk"})
		hj.Spill, hj.MemTracker = sm, tr
		r.hj = hj
		done := make(chan struct{})
		r.done = done
		go func() {
			defer close(done)
			r.err = hj.Build(context.Background(), NewSliceSource(staleReqSchema, staleReqRows(reqPad)))
		}()
		return done
	}
}

// phaseRequester is the requester a schedule launched, if it launched one.
type phaseRequester struct {
	hj   *HashJoin
	done chan struct{}
	err  error
}

// finish waits for the requester and closes it. Its build may outlive the
// victim's phase (it waits behind the phase's lock), so nothing of it is read
// before it is done.
func (r *phaseRequester) finish(t *testing.T) {
	t.Helper()
	if r.done == nil {
		return
	}
	select {
	case <-r.done:
	case <-time.After(60 * time.Second):
		t.Fatal("the requester never finished")
	}
	r.hj.Close()
}

// reqPadFor returns the requester row pad whose arrival costs at least target.
func reqPadFor(target int64) int {
	cost := func(pad int) int64 {
		return hashBuildBytes(batch.FromRows(staleReqSchema, staleReqRows(pad)))
	}
	base := cost(1)
	if target <= base {
		return 1
	}
	pad := int((target-base)/staleReqN) + 1
	for cost(pad) < target {
		pad++
	}
	return pad
}

// phaseBarrierSource hands pre-built batches to parallel pipeline workers.
// The first pull (runParallel's serial warm-up, consumed by the primary) passes
// at once; the next `workers` pulls wait until all of them have arrived, so
// every worker — every clone sink — holds at least one batch when the merge
// runs.
type phaseBarrierSource struct {
	mu      sync.Mutex
	batches []*batch.RecordBatch
	idx     int
	workers int
	arrived int
	open    chan struct{}
}

func newPhaseBarrierSource(batches []*batch.RecordBatch, workers int) *phaseBarrierSource {
	return &phaseBarrierSource{batches: batches, workers: workers, open: make(chan struct{})}
}

func (s *phaseBarrierSource) Init(context.Context) error { return nil }
func (s *phaseBarrierSource) Close() error               { return nil }
func (s *phaseBarrierSource) Next(context.Context) (*batch.RecordBatch, error) {
	s.mu.Lock()
	i := s.idx
	if i >= len(s.batches) {
		s.mu.Unlock()
		return nil, nil
	}
	s.idx++
	b := s.batches[i]
	if i >= 1 && i <= s.workers {
		s.arrived++
		if s.arrived == s.workers {
			close(s.open)
		}
		s.mu.Unlock()
		<-s.open
		return b, nil
	}
	s.mu.Unlock()
	return b, nil
}

func newPhaseSpill(t *testing.T, budget int64) (*memory.Tracker, *memory.SpillManager, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "relief-phase")
	if err != nil {
		t.Fatal(err)
	}
	tr := memory.NewTracker("phase", budget)
	sm, err := memory.NewSpillManager(dir, tr)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatal(err)
	}
	return tr, sm, func() { sm.Cleanup(); os.RemoveAll(dir) }
}

func checkPhaseOutcome(t *testing.T, site PhaseSite, out *phaseOutcome) {
	t.Helper()
	if !out.reached.Load() {
		t.Fatalf("schedule not reached: the victim never entered %s", site)
	}
	if out.selectedInPhase.Load() {
		t.Errorf("the relief registry read the victim's footprint and selected it while the victim was inside %s: "+
			"the phase's writes are not excluded from the relief entry points", site)
	}
	if out.reliefInPhase.Load() {
		t.Errorf("a relief call the registry selected while the victim was inside %s finished before the phase "+
			"did: relief reclaimed state the phase was still consuming", site)
	}
}

// ---- Sort clone merge ----

const (
	sortPhaseBatches = 4
	sortPhasePad     = 120
)

var sortPhaseSchema = []parquet.Column{
	{Name: "k", Type: parquet.TypeInt64},
	{Name: "pad", Type: parquet.TypeString},
}

func sortPhaseInput() ([]*batch.RecordBatch, int64) {
	var out []*batch.RecordBatch
	var bytes int64
	n := sortPhaseBatches * batch.DefaultBatchSize
	for off := 0; off < n; off += batch.DefaultBatchSize {
		rows := make([]map[string]any, batch.DefaultBatchSize)
		for i := range rows {
			k := off + i
			rows[i] = map[string]any{"k": int64(k), "pad": fmt.Sprintf("%08d", k) + strings.Repeat("x", sortPhasePad-8)}
		}
		b := batch.FromRows(sortPhaseSchema, rows)
		bytes += b.MemBytes()
		out = append(out, b)
	}
	return out, bytes
}

// runSortPhase runs ORDER BY pad DESC over a two-worker morsel pipeline whose
// primary is registered for relief, landing a relief call inside the clone
// merge. It returns the sorted k values.
func runSortPhase(t *testing.T, landing phaseLanding) []int64 {
	t.Helper()
	ctx := context.Background()
	input, inputBytes := sortPhaseInput()
	// The requester's arrival costs about the sort's whole input; the budget
	// leaves it half that short beside the sort, so its relief gap is half the
	// sort's input and the sort is the only candidate. The sort's run floor
	// (minSortRunBytes, 64 MiB) keeps it from spilling by itself.
	reqPad := reqPadFor(inputBytes)
	reqCost := hashBuildBytes(batch.FromRows(staleReqSchema, staleReqRows(reqPad)))
	budget := inputBytes + reqCost/2
	tr, sm, cleanup := newPhaseSpill(t, budget)
	defer cleanup()

	s := NewSort([]SortKey{{Column: "pad", Order: Descending}})
	s.Spill = sm
	var req phaseRequester
	out, restore := armPhaseSchedule(PhaseSortMerge, s, memory.AccountedOperator(s), landing,
		startRequester(sm, tr, reqPad, &req))
	pipe := &Pipeline{Source: newPhaseBarrierSource(input, 2), Sink: s, Workers: 2}
	runErr := pipe.Run(ctx)
	restore()
	var ks []int64
	if runErr == nil {
		for {
			b, err := s.Next(ctx)
			if err != nil {
				runErr = err
				break
			}
			if b == nil {
				break
			}
			for _, r := range b.ToRows() {
				ks = append(ks, r["k"].(int64))
			}
		}
	}
	pipe.Close()
	req.finish(t)
	t.Logf("SORT %s: rows=%d selected_in_merge=%v relief_finished_in_merge=%v requester_err=%v used_after_close=%d",
		landing, len(ks), out.selectedInPhase.Load(), out.reliefInPhase.Load(), req.err, tr.Used())
	if runErr != nil {
		t.Fatalf("sort pipeline: %v", runErr)
	}
	checkPhaseOutcome(t, PhaseSortMerge, out)
	if u := tr.Used(); u != 0 {
		t.Errorf("tracker holds %d bytes after the sort and the requester closed", u)
	}
	return ks
}

// TestReliefDuringSortCloneMerge: ORDER BY pad DESC returns every row, k
// strictly descending (pad is k zero-padded), whatever the relief call does.
func TestReliefDuringSortCloneMerge(t *testing.T) {
	for _, landing := range phaseLandings {
		t.Run(string(landing), func(t *testing.T) {
			ks := runSortPhase(t, landing)
			n := sortPhaseBatches * batch.DefaultBatchSize
			if len(ks) != n {
				t.Fatalf("sort returned %d rows, want %d", len(ks), n)
			}
			for i, k := range ks {
				if want := int64(n - 1 - i); k != want {
					t.Fatalf("row %d: k=%d, want %d (ORDER BY pad DESC)", i, k, want)
				}
			}
		})
	}
}

// ---- HashAggregate clone merge ----

const aggPhaseRows = 4 * batch.DefaultBatchSize

var aggPhaseSchema = []parquet.Column{
	{Name: "k", Type: parquet.TypeInt64},
	{Name: "v", Type: parquet.TypeInt64},
}

func aggPhaseInput() []*batch.RecordBatch {
	var out []*batch.RecordBatch
	for off := 0; off < aggPhaseRows; off += batch.DefaultBatchSize {
		rows := make([]map[string]any, batch.DefaultBatchSize)
		for i := range rows {
			k := off + i
			rows[i] = map[string]any{"k": int64(k), "v": int64(3 * k)}
		}
		out = append(out, batch.FromRows(aggPhaseSchema, rows))
	}
	return out
}

func newAggPhase(sm *memory.SpillManager) *HashAggregate {
	h := NewHashAggregate([]string{"k"}, []AggColumn{
		{Func: AggCount, OutputCol: "c", OutputType: parquet.TypeInt64},
		{Func: AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeInt64},
	})
	h.Spill = sm
	return h
}

// aggPhaseDigest is (groups, Σc, Σs, Σk).
type aggPhaseDigest struct{ groups, sumC, sumS, sumK int64 }

func drainAggPhase(ctx context.Context, h *HashAggregate) (aggPhaseDigest, error) {
	var d aggPhaseDigest
	for {
		b, err := h.Next(ctx)
		if err != nil {
			return d, err
		}
		if b == nil {
			return d, nil
		}
		for _, r := range b.ToRows() {
			d.groups++
			d.sumC += r["c"].(int64)
			d.sumS += r["s"].(int64)
			d.sumK += r["k"].(int64)
		}
	}
}

// runAggPhase runs GROUP BY k over a two-worker morsel pipeline (the
// non-partitioned merge: WADJET_PARTITIONED_AGG=0, the invariance oracle's
// arm, and every shape the partitioned route does not take), landing a relief
// call inside the clone merge.
func runAggPhase(t *testing.T, landing phaseLanding) aggPhaseDigest {
	t.Helper()
	ctx := context.Background()
	prev := partitionedAggToggle.Set(false)
	defer partitionedAggToggle.Set(prev)

	// Calibration: the tracked group state when the merge starts.
	var usedAtMerge atomic.Int64
	{
		tr, sm, cleanup := newPhaseSpill(t, 1<<30)
		h := newAggPhase(sm)
		restore := SetPhaseHook(func(s PhaseSite, op any) {
			if s == PhaseAggMerge && op == any(h) {
				usedAtMerge.CompareAndSwap(0, tr.Used())
			}
		})
		pipe := &Pipeline{Source: newPhaseBarrierSource(aggPhaseInput(), 2), Sink: h, Workers: 2}
		err := pipe.Run(ctx)
		restore()
		pipe.Close()
		cleanup()
		if err != nil {
			t.Fatalf("calibration: %v", err)
		}
	}
	used := usedAtMerge.Load()
	if used == 0 {
		t.Fatal("calibration: the merge never ran")
	}
	// The aggregate stays under the 40% self-spill threshold; the requester's
	// arrival (80% of the budget) does not fit beside it but fits alone.
	budget := used * 100 / 35
	reqPad := reqPadFor(budget * 8 / 10)
	tr, sm, cleanup := newPhaseSpill(t, budget)
	defer cleanup()

	h := newAggPhase(sm)
	var req phaseRequester
	out, restore := armPhaseSchedule(PhaseAggMerge, h, memory.AccountedOperator(h), landing,
		startRequester(sm, tr, reqPad, &req))
	pipe := &Pipeline{Source: newPhaseBarrierSource(aggPhaseInput(), 2), Sink: h, Workers: 2}
	runErr := pipe.Run(ctx)
	restore()
	var d aggPhaseDigest
	if runErr == nil {
		d, runErr = drainAggPhase(ctx, h)
	}
	pipe.Close()
	req.finish(t)
	t.Logf("AGG %s: %+v selected_in_merge=%v relief_finished_in_merge=%v requester_err=%v used_after_close=%d",
		landing, d, out.selectedInPhase.Load(), out.reliefInPhase.Load(), req.err, tr.Used())
	if runErr != nil {
		t.Fatalf("aggregate pipeline: %v", runErr)
	}
	checkPhaseOutcome(t, PhaseAggMerge, out)
	if u := tr.Used(); u != 0 {
		t.Errorf("tracker holds %d bytes after the aggregate and the requester closed", u)
	}
	return d
}

// TestReliefDuringAggregateCloneMerge: one group per k, each counted once,
// whatever the relief call does. PostgreSQL 17.11 over the same rows:
//
//	SELECT count(*), sum(c), sum(s), sum(k) FROM (SELECT k, count(*) AS c, sum(3*k) AS s
//	  FROM generate_series(0, 8191) k GROUP BY k) q;   -- 8192 | 8192 | 100651008 | 33550336
func TestReliefDuringAggregateCloneMerge(t *testing.T) {
	want := aggPhaseDigest{groups: 8192, sumC: 8192, sumS: 100651008, sumK: 33550336}
	for _, landing := range phaseLandings {
		t.Run(string(landing), func(t *testing.T) {
			if got := runAggPhase(t, landing); got != want {
				t.Errorf("aggregate answered %+v, PostgreSQL 17.11: %+v", got, want)
			}
		})
	}
}

// ---- HashJoin build tail ----

// runJoinTailPhase builds the stale-relief victim (staleMain, every join
// kind) with room to finish without evicting, lands a relief call inside its
// build tail, then probes. The answer is compared with PostgreSQL 17.11's
// (staleReliefPG, measured for line 1706k over the same tables).
func runJoinTailPhase(t *testing.T, k staleKind, landing phaseLanding) (staleDigest, error) {
	t.Helper()
	_, usedFinal, _ := calibrateStale(t, staleMain, k, 0)
	// The build stays under the 40% proactive-eviction threshold, so every
	// partition is resident (relief-spillable) in the tail; the requester's
	// arrival (2.1× the build) does not fit beside it.
	budget := usedFinal * 100 / 38
	reqPad := reqPadFor(usedFinal * 21 / 10)
	tr, sm, cleanup := newPhaseSpill(t, budget)
	defer cleanup()

	victim := staleMain.newJoin(k)
	victim.Spill, victim.MemTracker = sm, tr
	var req phaseRequester
	out, restore := armPhaseSchedule(PhaseJoinBuildTail, victim, memory.AccountedOperator(victim), landing,
		startRequester(sm, tr, reqPad, &req))
	buildErr := victim.Build(context.Background(), NewSliceSource(staleBuildSchema, staleMain.buildRows()))
	restore()
	var d staleDigest
	err := buildErr
	if err == nil {
		var rows []map[string]any
		rows, err = runStaleProbe(victim, NewSliceSource(staleMain.probeSchema, staleMain.probeRows()))
		d = digestRows(rows)
	}
	victim.Close()
	req.finish(t)
	t.Logf("JOINTAIL %s/%s: %v err=%v selected_in_tail=%v relief_finished_in_tail=%v requester_err=%v used_after_close=%d",
		landing, k.name, d, err, out.selectedInPhase.Load(), out.reliefInPhase.Load(), req.err, tr.Used())
	checkPhaseOutcome(t, PhaseJoinBuildTail, out)
	if u := tr.Used(); u != 0 {
		t.Errorf("tracker holds %d bytes after both joins closed", u)
	}
	return d, err
}

// TestReliefDuringJoinBuildTail settles the build-tail row 1706k left open.
func TestReliefDuringJoinBuildTail(t *testing.T) {
	for _, landing := range phaseLandings {
		for _, k := range staleKinds {
			t.Run(string(landing)+"/"+k.name, func(t *testing.T) {
				got, err := runJoinTailPhase(t, k, landing)
				if err != nil {
					t.Fatalf("victim: %v", err)
				}
				if want := staleReliefPG[k.name]; got != want {
					t.Errorf("victim answer %v, PostgreSQL 17.11: %v", got, want)
				}
			})
		}
	}
}
