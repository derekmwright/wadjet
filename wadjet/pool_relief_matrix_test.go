// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/optswitch"
	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// Cross-statement relief at every lifecycle point of every registrant the
// embedded paths reach (1685e round 2; ruling 2026-11-24: relief across
// statements is KEPT, and safe only under each operator's lifecycle lock).
// Statement B is the victim, held at one point of one operator's life;
// statement A, on the same DB, is the requester: a self-join over `bigrg`
// whose first 2048-row arrival does not fit the pool, so its build asks the
// pool's registry for relief, which may pick any live registrant of any
// statement. Every answer is compared with PostgreSQL 17.11
// (1685e_evidence/pg_oracle_r2.txt).
//
// Three kinds of point:
//   - phase: B is INSIDE a phase that mutates relief-visible state while the
//     operator is still registered (a Sort's or a HashAggregate's clone
//     merge, a partitioned HashJoin's build tail: the sites 1706n named,
//     exec.PhaseSite). A is launched from inside the phase. The registry
//     SELECTING B's operator while B is in the phase is the defect (1706n:
//     it read and could drain state the phase was rewriting); with the
//     phase under the operator's lock the selection waits for the phase.
//   - held: B is held OUTSIDE its operator's locks with the operator live
//     (mid-consume). A's relief may legitimately evict B's state; the cell
//     requires that it reached B (cross-statement relief exercised) and that
//     B still answers exactly.
//   - after: B is held downstream of an operator that has finished (Finalize
//     or the end of a join build: 1706k's closed relief). The operator must
//     no longer be a live registrant, and B answers exactly.

// phaseSelectWait bounds the wait, inside B's phase, for the registry to
// select B's operator. Only the guarded code waits it out (A's ranking is
// held behind B's lock); the unguarded code selects in well under it.
const phaseSelectWait = time.Second

type reliefPoint string

const (
	pointPhase reliefPoint = "phase"
	pointHeld  reliefPoint = "held"
	pointAfter reliefPoint = "after"
)

type reliefMatrixCell struct {
	name   string
	victim string // the operator's footprint name prefix
	point  reliefPoint
	site   exec.PhaseSite // pointPhase only
	smj    bool           // route B's join through the sort-merge join (A stays a hash join)
	// switchOff names an optswitch turned off for the cell (restored after).
	switchOff string
	sql       string
	want      string // PostgreSQL 17.11
	// pointHeld holds B at pool_count_gate in B's input once the victim is
	// live with spillable bytes (armLiveGate); pointAfter holds B at its
	// first call of pool_gate_0 downstream of the operator.
}

// PostgreSQL 17.11 answers, 1685e_evidence/pg_oracle_r2.txt.
var reliefMatrixCells = []reliefMatrixCell{
	{name: "hashjoin/build-tail", victim: "HashJoin", point: pointPhase, site: exec.PhaseJoinBuildTail,
		sql: poolJoinSQL(0), want: poolJoinWant},
	// The default parallel aggregate merges by ADOPTION (disjoint key
	// partitions, under h.mu, no primary state rewritten); the state merge
	// the phase site sits in runs with the partitioned-agg kill switch off.
	{name: "hashagg/clone-merge", victim: "HashAggregate", point: pointPhase, site: exec.PhaseAggMerge,
		switchOff: "partitioned-agg",
		sql:       "SELECT count(*) AS n, sum(c) AS s FROM (SELECT pad, count(k) AS c FROM wide GROUP BY pad) q",
		want:      "[map[n:4000 s:4000]]"},
	{name: "hashagg/after-finalize", victim: "HashAggregate", point: pointAfter,
		sql:  "SELECT count(*) AS n, sum(pool_gate_0(c)) AS s FROM (SELECT pad, count(k) AS c FROM wide GROUP BY pad) q",
		want: "[map[n:4000 s:4000]]"},
	{name: "sort/mid-consume", victim: "Sort", point: pointHeld,
		sql:  "SELECT count(*) AS n, min(k) AS lo FROM (SELECT pool_count_gate(k) AS k FROM wide ORDER BY pad DESC LIMIT 3000) q",
		want: "[map[lo:1000 n:3000]]"},
	{name: "sort/clone-merge", victim: "Sort", point: pointPhase, site: exec.PhaseSortMerge,
		sql:  "SELECT count(*) AS n, min(k) AS lo FROM (SELECT k FROM wide ORDER BY pad DESC LIMIT 3000) q",
		want: "[map[lo:1000 n:3000]]"},
	{name: "sort/after-finalize", victim: "Sort", point: pointAfter,
		sql:  "SELECT count(*) AS n, min(pool_gate_0(k)) AS lo FROM (SELECT k FROM wide ORDER BY pad DESC LIMIT 3000) q",
		want: "[map[lo:1000 n:3000]]"},
	{name: "window/mid-consume", victim: "Window", point: pointHeld,
		sql:  "SELECT count(*) AS n, max(r) AS m FROM (SELECT row_number() OVER (ORDER BY pad DESC) AS r FROM wide WHERE pool_count_gate(k) >= 0) q",
		want: "[map[m:4000 n:4000]]"},
	{name: "window/after-finalize", victim: "Window", point: pointAfter,
		sql:  "SELECT count(*) AS n, max(pool_gate_0(r)) AS m FROM (SELECT row_number() OVER (ORDER BY pad DESC) AS r FROM wide) q",
		want: "[map[m:4000 n:4000]]"},
	{name: "sortmergejoin/mid-consume", victim: "SortMergeJoin", point: pointHeld, smj: true,
		sql:  "SELECT count(*) AS n, sum(a.k) AS s FROM wide a JOIN wide b ON a.k = b.k WHERE pool_count_gate(b.k) >= 0",
		want: poolJoinWant},
	{name: "sortmergejoin/after-finalize", victim: "SortMergeJoin", point: pointAfter, smj: true,
		sql: poolJoinSQL(0), want: poolJoinWant},
}

// TestPoolReliefEveryRegistrantAcrossStatements: the matrix above, one cell
// per (registrant, lifecycle point). The hash join's mid-build and
// after-build points and the aggregate's mid-consume point are
// TestPoolReliefCrossesStatementsSafely's cells.
func TestPoolReliefEveryRegistrantAcrossStatements(t *testing.T) {
	const budget = 614268
	for _, cell := range reliefMatrixCells {
		t.Run(cell.name, func(t *testing.T) {
			runReliefMatrixCell(t, cell, budget)
		})
	}
}

func runReliefMatrixCell(t *testing.T, cell reliefMatrixCell, budget int64) {
	ctx := context.Background()
	db := wideJoinDB(t, 4000, 512, 512, budget)
	addBigRowGroupCopy(t, db)
	gates := armPoolGates(2) // gate 1 is A's, always open
	gates[1].open()
	// Isolated baseline: A alone on this pool answers PostgreSQL's answer
	// (and is warm, so inside B's phase it reaches its relief request
	// promptly).
	if got, err := rowsText(t, db, reliefRequesterSQL); err != nil || got != reliefRequesterWant {
		t.Fatalf("A alone: %.80s %v", got, err)
	}
	assertDBPoolQuiescent(t, db)
	if cell.smj {
		db.sortMergeJoinBytes = 1
	}
	if cell.switchOff != "" {
		var found bool
		for _, tg := range optswitch.All() {
			if tg.Name == cell.switchOff {
				prev := tg.Set(false)
				defer tg.Set(prev)
				found = true
			}
		}
		if !found {
			t.Fatalf("no optswitch %q", cell.switchOff)
		}
	}
	log := &reliefLog{}

	var (
		bHeld    = make(chan struct{})
		releaseB = make(chan struct{})
		relOnce  sync.Once
	)
	release := func() { relOnce.Do(func() { close(releaseB) }) }
	defer release()

	// A, the requester.
	var (
		aDone    = make(chan struct{})
		aStarted atomic.Bool
		aAns     string
		aErr     error
	)
	startA := func() {
		if !aStarted.CompareAndSwap(false, true) {
			return
		}
		go func() {
			defer close(aDone)
			res, err := db.Query(ctx, reliefRequesterSQL)
			if err != nil {
				aErr = err
				return
			}
			aAns = fmt.Sprint(res.Rows)
		}()
	}

	// Phase cells: the victim is the first operator to enter the phase (A
	// has not started), and the relief hook records a selection of exactly
	// that operator while it is inside.
	var (
		victimOp        atomic.Value // the operator inside the phase
		inPhase         atomic.Bool
		reachedPhase    atomic.Bool
		selectedInPhase atomic.Bool
		selected        = make(chan struct{})
		selOnce         sync.Once
	)
	defer memory.SetReliefCallHook(func(site memory.ReliefSite, op memory.AccountedOperator) {
		log.hook(site, op)
		if v := victimOp.Load(); v != nil && inPhase.Load() && any(op) == v.(phaseVictim).op {
			selectedInPhase.Store(true)
			selOnce.Do(func() { close(selected) })
		}
	})()

	switch cell.point {
	case pointPhase:
		gates[0].open()
		var armed atomic.Bool
		defer exec.SetPhaseHook(func(site exec.PhaseSite, op any) {
			if site != cell.site || !armed.CompareAndSwap(false, true) {
				return
			}
			victimOp.Store(phaseVictim{op})
			inPhase.Store(true)
			reachedPhase.Store(true)
			startA()
			select {
			case <-selected:
			case <-time.After(phaseSelectWait):
			}
			inPhase.Store(false)
		})()
	case pointHeld:
		g := armLiveGate(db, cell.victim)
		gates[0].open()
		go func() {
			select {
			case <-g.reached:
				close(bHeld)
				<-releaseB
			case <-time.After(poolGateDeadline):
			}
			close(g.release)
		}()
	case pointAfter:
		go func() {
			select {
			case <-gates[0].reached:
				close(bHeld)
				<-releaseB
			case <-time.After(poolGateDeadline):
			}
			gates[0].open()
		}()
	}

	smjBefore := physical.SortMergeJoinsPlanned.Load()
	bDone := make(chan struct{})
	var bAns string
	var bErr error
	go func() {
		defer close(bDone)
		res, err := db.Query(ctx, cell.sql)
		if err != nil {
			bErr = err
			return
		}
		bAns = fmt.Sprint(res.Rows)
	}()

	var bLive map[uint64]string
	victimDeparted := 0
	switch cell.point {
	case pointPhase:
		select {
		case <-bDone:
		case <-time.After(poolGateDeadline):
			t.Fatalf("B hung at %s", cell.name)
		}
		if !reachedPhase.Load() {
			if scanWorkersOne() && cell.site != exec.PhaseJoinBuildTail {
				t.Skipf("no clone merge at one scan worker: B ran serially (%s %v)", bAns, bErr)
			}
			t.Fatalf("B never entered %s (answer %s %v): the cell did not reach its phase", cell.site, bAns, bErr)
		}
	default:
		select {
		case <-bHeld:
		case <-bDone:
			t.Fatalf("B ended before its %s point: %s %v", cell.name, bAns, bErr)
		case <-time.After(poolGateDeadline):
			t.Fatalf("B never reached its %s point", cell.name)
		}
		bLive = map[uint64]string{}
		for _, f := range db.pool.SpillManager().Inspect() {
			if f.State == memory.OpActive {
				bLive[f.InstanceID] = f.Name
			}
			// A departed summary: an instance of this name registered and
			// has unregistered (the fresh DB ran nothing else).
			if f.Departed > 0 && strings.HasPrefix(f.Name, cell.victim) {
				victimDeparted += f.Departed
			}
		}
		// Count only A's relief calls from here (B's own, before its
		// hold, are not cross-statement).
		log.reset()
		// B planned with the sort-merge route; A plans as a hash join (the
		// requester shape). B's read of the field happened before its gate.
		db.sortMergeJoinBytes = 0
		startA()
	}
	select {
	case <-aDone:
	case <-time.After(poolGateDeadline):
		t.Fatalf("A hung beside B at %s", cell.name)
	}
	release()
	select {
	case <-bDone:
	case <-time.After(poolGateDeadline):
		t.Fatalf("B hung after release at %s", cell.name)
	}

	if cell.smj && physical.SortMergeJoinsPlanned.Load() == smjBefore {
		t.Fatalf("B's join did not take the sort-merge path: the cell did not reach its registrant")
	}
	var reachedB, liveVictims []string
	for id, name := range bLive {
		if strings.HasPrefix(name, cell.victim) {
			liveVictims = append(liveVictims, fmt.Sprintf("%s#%d", name, id))
			if log.reached(id) {
				reachedB = append(reachedB, fmt.Sprintf("%s#%d", name, id))
			}
		}
	}
	sort.Strings(reachedB)
	sort.Strings(liveVictims)
	t.Logf("%s: B live %v; A's relief reached B's %v; selected in phase %v; all relief calls %v; A %s %v",
		cell.name, bLive, reachedB, selectedInPhase.Load(), log.names(), aAns, aErr)

	switch cell.point {
	case pointPhase:
		if selectedInPhase.Load() {
			t.Errorf("the registry selected B's %s for A's relief while B was inside %s: relief interleaved with the phase (1706n)",
				cell.victim, cell.site)
		}
	case pointHeld:
		if len(reachedB) == 0 {
			t.Errorf("A's relief never reached B's live %s %v (live %v): the cell did not exercise cross-statement relief",
				cell.victim, liveVictims, bLive)
		}
	case pointAfter:
		if len(liveVictims) != 0 {
			t.Errorf("B's finished %s is still a live registrant %v: its relief should be closed", cell.victim, liveVictims)
		}
		if victimDeparted == 0 {
			t.Errorf("B's %s never registered and departed before its hold: the cell did not reach its registrant", cell.victim)
		}
		for _, c := range log.names() {
			if strings.Contains(c, "->"+cell.victim) {
				t.Errorf("A's relief reached a %s after B's finished (%s)", cell.victim, c)
			}
		}
	}
	if bErr != nil || bAns != cell.want {
		t.Errorf("B at %s: %s %v, PostgreSQL 17.11 %s", cell.name, bAns, bErr, cell.want)
	}
	if aErr != nil && !isBudgetRefusal(aErr) {
		t.Errorf("A failed with an unnamed error: %v", aErr)
	}
	if aErr == nil && aAns != reliefRequesterWant {
		t.Errorf("A: %s, PostgreSQL 17.11 %s", aAns, reliefRequesterWant)
	}
	assertDBPoolQuiescent(t, db)
}

type phaseVictim struct{ op any }

// armLiveGate registers pool_count_gate as a hold conditioned on STATE, not on
// a call count: B is held at the first call at which a live registrant whose
// name starts with victim reports spillable bytes on db's pool (with parallel
// scan workers the gate expression runs ahead of the sink, so a count alone
// can trip before the victim holds anything relief could take). The gate
// must sit below the victim (a filter or projection feeding it), never inside
// it: it reads the registry, which takes the victim's lock.
func armLiveGate(db *DB, victim string) *countGate {
	g := &countGate{reached: make(chan struct{}), release: make(chan struct{})}
	var tripped atomic.Bool
	expr.RegisterFunc("pool_count_gate", func(args []any) any {
		if tripped.Load() {
			return args[0]
		}
		for _, f := range db.pool.SpillManager().Inspect() {
			if f.State == memory.OpActive && f.SpillableBytes > 0 && strings.HasPrefix(f.Name, victim) {
				if tripped.CompareAndSwap(false, true) {
					close(g.reached)
					select {
					case <-g.release:
					case <-time.After(poolGateDeadline):
					}
				}
				break
			}
		}
		return args[0]
	}, expr.RetTypeOf(batch.TypeInt64))
	return g
}

func (l *reliefLog) reset() {
	l.mu.Lock()
	l.calls, l.ids = nil, nil
	l.mu.Unlock()
}

// scanWorkersOne mirrors the planner's scan parallelism
// (physical.scanParallelism): WADJET_SCAN_WORKERS when positive, else
// runtime.NumCPU(), which honours the affinity mask (taskset -c 0 gives 1).
// At one worker a pipeline runs serially and never clone-merges.
func scanWorkersOne() bool {
	if n, err := strconv.Atoi(os.Getenv("WADJET_SCAN_WORKERS")); err == nil && n > 0 {
		return n == 1
	}
	return runtime.NumCPU() == 1
}

// TestPoolSortReliefAcrossStatementsAnswersExactly is round 1's reproduction
// of the wrong answer, kept: a top-K sort beside a relief requester on one
// pool, unheld, many rounds. Before 1706n put Sort.MergeSink under the sort's
// lock, cross-statement relief drained the primary while the clone merge
// appended to it, and the statement answered one scan batch's worth of rows
// (n=2048, lo=0; PostgreSQL 17.11: 3000 | 1000). It samples the scheduler
// (the deterministic schedule is the matrix's sort/clone-merge cell); it
// requires relief to have reached a Sort in the run.
func TestPoolSortReliefAcrossStatementsAnswersExactly(t *testing.T) {
	rounds := 40
	if testing.Short() {
		rounds = 10
	}
	if v, err := strconv.Atoi(os.Getenv("WADJET_POOL_SORT_ROUNDS")); err == nil && v > 0 {
		rounds = v
	}
	db := wideJoinDB(t, 4000, 512, 512, 1<<20)
	addBigRowGroupCopy(t, db)
	g := armPoolGates(2)
	g[0].open()
	g[1].open()
	var sortHits atomic.Int64
	defer memory.SetReliefCallHook(func(_ memory.ReliefSite, op memory.AccountedOperator) {
		if op.Inspect().Name == "Sort" {
			sortHits.Add(1)
		}
	})()
	const sortSQL = "SELECT count(*) AS n, min(k) AS lo FROM (SELECT k FROM wide ORDER BY pad DESC LIMIT 3000) q"
	const want = "[map[lo:1000 n:3000]]" // PostgreSQL 17.11
	wrong, refused := 0, 0
	for r := 0; r < rounds; r++ {
		var wg sync.WaitGroup
		var sAns string
		var sErr, aErr error
		var aAns string
		wg.Add(2)
		go func() {
			defer wg.Done()
			res, err := db.Query(context.Background(), sortSQL)
			sErr = err
			if err == nil {
				sAns = fmt.Sprint(res.Rows)
			}
		}()
		go func() {
			defer wg.Done()
			res, err := db.Query(context.Background(), reliefRequesterSQL)
			aErr = err
			if err == nil {
				aAns = fmt.Sprint(res.Rows)
			}
		}()
		wg.Wait()
		switch {
		case sErr != nil && isBudgetRefusal(sErr):
			refused++
		case sErr != nil:
			t.Errorf("round %d: the sort failed with an unnamed error: %v", r, sErr)
		case sAns != want:
			wrong++
			t.Errorf("round %d: the sort answered %s, PostgreSQL 17.11 %s", r, sAns, want)
		}
		if aErr != nil && !isBudgetRefusal(aErr) {
			t.Errorf("round %d: the requester failed with an unnamed error: %v", r, aErr)
		}
		if aErr == nil && aAns != reliefRequesterWant {
			t.Errorf("round %d: the requester answered %s, PostgreSQL 17.11 %s", r, aAns, reliefRequesterWant)
		}
	}
	t.Logf("rounds %d: wrong %d, refused %d, relief calls into a Sort %d", rounds, wrong, refused, sortHits.Load())
	if sortHits.Load() == 0 {
		t.Errorf("no relief call reached a Sort in %d rounds: the test did not exercise cross-statement relief", rounds)
	}
	assertDBPoolQuiescent(t, db)
}
