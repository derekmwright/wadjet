// SPDX-License-Identifier: MIT

package exec

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #1706, line 1706o, the sweep: the grace join marked a partition spilled
// BEFORE the fallible create of its run, and a relief eviction's failure was
// returned to the requester or discarded by the drift backstop, so the victim
// answered without the partition. This is the same question for every other
// spilling operator: HashAggregate (partial-state drains and their k-way
// merge), Sort (runs and their merge), Window (external runs), the
// sort-merge join (side runs), the materialized CTE spool and the batch
// collector. For each, and for each entry point that reaches its spill — its
// own spill under pressure, RequestRelief's first pass, and the drift
// backstop (whose call is the operator's SpillSome with the error discarded:
// `n, _ := best.SpillSome(target)`, memory/spill.go) — a fault is placed on
// the run the spill opens, and the statement must end LOUD (an error) or
// RIGHT (the reference answer); never WRONG.
//
// The references are PostgreSQL 17.11's answers (postgres:17.11-alpine,
// initdb --locale=C --encoding=UTF8, datcollate C, measured 2026-11-26) over
//
//	CREATE TABLE t AS SELECT (i % 5000)::bigint AS k, i::bigint AS v FROM generate_series(0, 39999) i;
//
// aggregate: staleAggPG; sort: SELECT count(*), sum((rn-1)*v), sum((rn-1)*k)
// FROM (SELECT k, v, row_number() OVER (ORDER BY k, v) rn FROM t) s;
// window: SELECT count(*), sum(rn), sum(rn*v) FROM (SELECT v, row_number()
// OVER (PARTITION BY k ORDER BY v) rn FROM t) s; the sort-merge join: the
// stale-relief tables' inner join (staleReliefPG["inner"]). The spool and
// the collector hand back their input in order: their reference is the
// input's own positional digest.
var (
	sweepSortPG   = [3]int64{40000, 16666916650000, 2666216650000}
	sweepWindowPG = [3]int64{40000, 180000, 4649910000}
	// SELECT count(*), sum(k), sum(nd) FROM (SELECT k, count(DISTINCT v) nd FROM t GROUP BY k) s
	sweepRawAggPG = [3]int64{5000, 12497500, 40000}
)

// sweepFault is one fault on the runs of one operator.
type sweepFault struct {
	name  string
	stage spillFaultStage
	once  bool
	// mergeRead: no write fault; the first run is truncated to half its
	// size after the spill and before the operator reads it back.
	mergeRead bool
}

var sweepFaults = []sweepFault{
	{name: "create", stage: spillFaultCreate, once: true},
	{name: "write", stage: spillFaultWrite},
	{name: "batch", stage: spillFaultBatch, once: true},
	{name: "flush", stage: spillFaultFlush, once: true},
	{name: "seek", stage: spillFaultSeek, once: true},
	{name: "count", stage: spillFaultCount, once: true},
	{name: "close", stage: spillFaultClose, once: true},
	{name: "merge-read", mergeRead: true},
}

// armSweepFault installs the seam for runs whose name starts with prefix and
// returns whether it fired.
func armSweepFault(t *testing.T, prefix string, f sweepFault) func() bool {
	t.Helper()
	var mu sync.Mutex
	fired := false
	hook := func(stage spillFaultStage, path string) error {
		if f.mergeRead || stage != f.stage || !strings.HasPrefix(filepath.Base(path), prefix) {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		if f.once && fired {
			return nil
		}
		fired = true
		return fmt.Errorf("%w (%s)", errInjectedSpill, f.name)
	}
	spillWriterFault.Store(&hook)
	t.Cleanup(func() { spillWriterFault.Store(nil) })
	return func() bool {
		mu.Lock()
		defer mu.Unlock()
		return fired
	}
}

// armAggFault places the aggregate's fault: its partial-state runs have their
// own writer, so the fault is a file-system obstacle at the name the next run
// takes (a directory: the create fails; /dev/full: every write that reaches
// the file fails). Only create and write apply.
func armAggFault(t *testing.T, dir string, f sweepFault) func() bool {
	t.Helper()
	p := filepath.Join(dir, fmt.Sprintf("agg-spill-%d.%d.bin", os.Getpid(), partialSpillSeq.Load()+1))
	var err error
	switch f.name {
	case "create":
		err = os.Mkdir(p, 0o700)
	case "write":
		err = os.Symlink("/dev/full", p)
	default:
		t.Fatalf("aggregate fault %s does not apply", f.name)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Remove(p) })
	return func() bool {
		// The obstacle is taken once a run with that sequence number was
		// attempted.
		return partialSpillSeq.Load() >= partialSpillSeqAt(p)
	}
}

// armRawRowFault places the fault on the spill manager's next SpillRows file.
// Its sequence number is the manager's own, so a one-row probe file is
// written first to learn it, and removed.
func armRawRowFault(t *testing.T, sm *memory.SpillManager, f sweepFault) func() bool {
	t.Helper()
	probe, err := sm.SpillRows([]map[string]any{{"k": int64(0)}})
	if err != nil {
		t.Fatal(err)
	}
	sm.RemoveSpilled(probe)
	var pid, seq int64
	if _, err := fmt.Sscanf(filepath.Base(probe), "spill-%d.%d.bin", &pid, &seq); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(filepath.Dir(probe), fmt.Sprintf("spill-%d.%d.bin", pid, seq+1))
	switch f.name {
	case "create":
		err = os.Mkdir(p, 0o700)
	case "write":
		err = os.Symlink("/dev/full", p)
	default:
		t.Fatalf("raw-row fault %s does not apply", f.name)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Remove(p) })
	return func() bool {
		// Taken once a later SpillRows file exists or was attempted: the
		// aggregate wrote at least one file name past the obstacle, or failed.
		return true
	}
}

func partialSpillSeqAt(p string) int64 {
	var pid, seq int64
	fmt.Sscanf(filepath.Base(p), "agg-spill-%d.%d.bin", &pid, &seq)
	return seq
}

func truncateHalf(t *testing.T, path string) {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, st.Size()/2); err != nil {
		t.Fatal(err)
	}
}

// sweepOp adapts one operator: feed consumes the whole input, op is what the
// registry would call, runs lists its run files, finish reads the answer.
type sweepOp struct {
	feed   func(ctx context.Context) error
	op     memory.AccountedOperator // nil: the operator takes no relief
	runs   func() []string
	finish func(ctx context.Context) (string, error)
	close  func()
}

type sweepTarget struct {
	name   string
	prefix string // the seam's run prefix; "" = the aggregate's obstacle
	ref    string
	faults []string // nil = every sweepFault
	relief bool     // registered for relief (RequestRelief, drift backstop)
	make   func(sm *memory.SpillManager) sweepOp
}

func sweepRows3(rows []map[string]any, f func(i int, r map[string]any) [3]int64) string {
	var d [3]int64
	for i, r := range rows {
		x := f(i, r)
		d[0] += x[0]
		d[1] += x[1]
		d[2] += x[2]
	}
	return fmt.Sprint(d)
}

func drainNext(ctx context.Context, next func(context.Context) (*batch.RecordBatch, error)) ([]map[string]any, error) {
	var rows []map[string]any
	for {
		b, err := next(ctx)
		if err != nil {
			return nil, err
		}
		if b == nil {
			return rows, nil
		}
		rows = append(rows, b.ToRows()...)
	}
}

func sweepInput() []*batch.RecordBatch { return staleAggBatches() }

func sweepPositional(rows []map[string]any) string {
	return sweepRows3(rows, func(i int, r map[string]any) [3]int64 {
		k, v := r["k"].(int64), r["v"].(int64)
		return [3]int64{1, int64(i) * v, int64(i) * k}
	})
}

var sweepTargets = []sweepTarget{
	{name: "aggregate", prefix: "", ref: fmt.Sprint(staleAggPG), faults: []string{"create", "write", "merge-read"}, relief: true,
		make: func(sm *memory.SpillManager) sweepOp {
			h := newStaleAgg()
			h.Spill = sm
			return sweepOp{
				feed: func(ctx context.Context) error {
					if err := h.Init(ctx); err != nil {
						return err
					}
					for _, b := range sweepInput() {
						if err := h.Consume(ctx, b); err != nil {
							return err
						}
					}
					return nil
				},
				op:   h,
				runs: func() []string { return h.partialSpillFiles },
				finish: func(ctx context.Context) (string, error) {
					if err := h.Finalize(ctx); err != nil {
						return "", err
					}
					rows, err := drainNext(ctx, h.Next)
					if err != nil {
						return "", err
					}
					return sweepRows3(rows, func(_ int, r map[string]any) [3]int64 {
						return [3]int64{1, r["k"].(int64), r["total"].(int64)}
					}), nil
				},
				close: func() { h.Close() },
			}
		}},
	{name: "aggregate-raw", prefix: "raw:", ref: fmt.Sprint(sweepRawAggPG), faults: []string{"create", "write", "merge-read"}, relief: false,
		make: func(sm *memory.SpillManager) sweepOp {
			// COUNT(DISTINCT) is not a simple aggregate: no partial-state
			// merge, so under pressure the aggregate buffers raw rows and
			// writes them through the spill manager's own SpillRows files.
			h := NewHashAggregate([]string{"k"}, []AggColumn{
				{Func: AggCountDistinct, InputCol: "v", OutputCol: "nd", OutputType: parquet.TypeInt64},
			})
			h.Spill = sm
			return sweepOp{
				feed: func(ctx context.Context) error {
					if err := h.Init(ctx); err != nil {
						return err
					}
					for _, b := range sweepInput() {
						if err := h.Consume(ctx, b); err != nil {
							return err
						}
					}
					return nil
				},
				runs: func() []string { return h.spillFiles },
				finish: func(ctx context.Context) (string, error) {
					if err := h.Finalize(ctx); err != nil {
						return "", err
					}
					rows, err := drainNext(ctx, h.Next)
					if err != nil {
						return "", err
					}
					return sweepRows3(rows, func(_ int, r map[string]any) [3]int64 {
						return [3]int64{1, r["k"].(int64), r["nd"].(int64)}
					}), nil
				},
				close: func() { h.Close() },
			}
		}},
	{name: "sort", prefix: "sort-run-", ref: fmt.Sprint(sweepSortPG), relief: true,
		make: func(sm *memory.SpillManager) sweepOp {
			s := NewSort([]SortKey{{Column: "k", Order: Ascending}, {Column: "v", Order: Ascending}})
			s.Spill = sm
			return sweepOp{
				feed: func(ctx context.Context) error {
					if err := s.Init(ctx); err != nil {
						return err
					}
					for _, b := range sweepInput() {
						if err := s.Consume(ctx, b); err != nil {
							return err
						}
					}
					return nil
				},
				op:   s,
				runs: func() []string { return s.runFiles },
				finish: func(ctx context.Context) (string, error) {
					if err := s.Finalize(ctx); err != nil {
						return "", err
					}
					rows, err := drainNext(ctx, s.Next)
					if err != nil {
						return "", err
					}
					return sweepPositional(rows), nil
				},
				close: func() { s.Close() },
			}
		}},
	{name: "window", prefix: "sort-run-", ref: fmt.Sprint(sweepWindowPG), relief: true,
		make: func(sm *memory.SpillManager) sweepOp {
			w := NewWindow([]WindowColumn{{
				Func: WinRowNumber, OutputCol: "rn", OutputType: parquet.TypeInt64,
				PartitionBy: []string{"k"}, OrderBy: []SortKey{{Column: "v", Order: Ascending}},
			}})
			w.Spill = sm
			return sweepOp{
				feed: func(ctx context.Context) error {
					if err := w.Init(ctx); err != nil {
						return err
					}
					for _, b := range sweepInput() {
						if err := w.Consume(ctx, b); err != nil {
							return err
						}
					}
					return nil
				},
				op:   w,
				runs: func() []string { return w.runFiles },
				finish: func(ctx context.Context) (string, error) {
					if err := w.Finalize(ctx); err != nil {
						return "", err
					}
					rows, err := drainNext(ctx, w.Next)
					if err != nil {
						return "", err
					}
					return sweepRows3(rows, func(_ int, r map[string]any) [3]int64 {
						rn := r["rn"].(int64)
						return [3]int64{1, rn, rn * r["v"].(int64)}
					}), nil
				},
				close: func() { w.Close() },
			}
		}},
	{name: "sort-merge-join", prefix: "sort-run-", ref: fmt.Sprint(staleReliefPG["inner"]), relief: true,
		make: func(sm *memory.SpillManager) sweepOp {
			j := NewSortMergeJoin([]string{"pk"}, []string{"bk"})
			j.Spill = sm
			return sweepOp{
				feed: func(ctx context.Context) error {
					if err := j.Init(ctx); err != nil {
						return err
					}
					if err := j.Build(ctx, NewSliceSource(staleBuildSchema, staleBuildRows())); err != nil {
						return err
					}
					probe := staleProbeRows()
					for start := 0; start < len(probe); start += batch.DefaultBatchSize {
						end := min(start+batch.DefaultBatchSize, len(probe))
						if err := j.Consume(ctx, batch.FromRows(staleProbeSchema, probe[start:end])); err != nil {
							return err
						}
					}
					return nil
				},
				op: j,
				runs: func() []string {
					return append(append([]string(nil), j.build.runFiles...), j.probe.runFiles...)
				},
				finish: func(ctx context.Context) (string, error) {
					if err := j.Finalize(ctx); err != nil {
						return "", err
					}
					rows, err := drainNext(ctx, j.Next)
					if err != nil {
						return "", err
					}
					return fmt.Sprint(digestRows(rows)), nil
				},
				close: func() { j.Close() },
			}
		}},
	{name: "batch-collector", prefix: "bridge-collect-", ref: "", relief: false,
		make: func(sm *memory.SpillManager) sweepOp {
			c := &SpillableBatchCollector{Spill: sm, RunBytes: 1}
			return sweepOp{
				feed: func(ctx context.Context) error {
					for _, b := range sweepInput() {
						if err := c.Consume(ctx, b); err != nil {
							return err
						}
					}
					return nil
				},
				runs: func() []string { return c.spillFiles },
				finish: func(ctx context.Context) (string, error) {
					rows, err := drainNext(ctx, c.NextReplay)
					if err != nil {
						return "", err
					}
					return sweepPositional(rows), nil
				},
				close: func() { c.Release() },
			}
		}},
	{name: "cte-spool", prefix: "cte-spool-", ref: "", relief: false,
		make: func(sm *memory.SpillManager) sweepOp {
			s := &SharedSpool{Source: NewBatchSource(sweepInput()), Spill: sm, RunBytes: 1}
			r := s.NewReader()
			var rows []map[string]any
			var readErr error
			return sweepOp{
				// The spool's body runs as the reader asks: reading is what
				// feeds (and drains) it.
				feed: func(ctx context.Context) error {
					rows, readErr = drainNext(ctx, r.Next)
					return readErr
				},
				runs: func() []string {
					s.mu.Lock()
					defer s.mu.Unlock()
					var ps []string
					for _, run := range s.runs {
						ps = append(ps, run.path)
					}
					return ps
				},
				// A second reader reads the spool again from its first
				// batch, through the runs the first one's reading drained.
				finish: func(ctx context.Context) (string, error) {
					r2 := s.NewReader()
					defer r2.Close()
					again, err := drainNext(ctx, r2.Next)
					if err != nil {
						return "", err
					}
					if a, b := sweepPositional(rows), sweepPositional(again); a != b {
						return "first read " + a + ", second read " + b, nil
					}
					return sweepPositional(again), nil
				},
				close: func() { r.Close(); s.Close() },
			}
		}},
}

type sweepOutcome struct {
	answer   string
	err      error
	reliefEr error
	fired    bool
	runs     int
	used     int64
	files    int
}

// runSweep runs one operator through one entry point with one fault.
// own: the operator spills by itself under a small budget. request / backstop:
// it holds its input resident under a budget it stays under (40% proactive
// threshold), then the registry's first pass (sm.RequestRelief) or the
// backstop's call (SpillSome, error discarded) spills it.
func runSweep(t *testing.T, tg sweepTarget, entry string, f sweepFault) sweepOutcome {
	t.Helper()
	ctx := context.Background()
	forceTinyRuns(t)
	// Calibrate the resident footprint.
	var resident int64
	{
		tr := memory.NewTracker("calib", 1<<30)
		sm, err := memory.NewSpillManager(t.TempDir(), tr)
		if err != nil {
			t.Fatal(err)
		}
		o := tg.make(sm)
		if err := o.feed(ctx); err != nil {
			t.Fatalf("calibration: %v", err)
		}
		resident = tr.Used()
		o.close()
		sm.Cleanup()
	}
	budget := resident * 100 / 30
	if entry == "own" {
		budget = max(resident/4, 64<<10)
	}
	if tg.prefix == "raw:" {
		// Flush the raw-row buffer to a file every 16 KiB.
		defer func(v int64) { spillFileTargetBytes = v }(spillFileTargetBytes)
		spillFileTargetBytes = 16 << 10
	}
	tr := memory.NewTracker("sweep", budget)
	sm, err := memory.NewSpillManager(t.TempDir(), tr)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()
	o := tg.make(sm)
	var out sweepOutcome
	var fired func() bool
	arm := func() {
		if f.mergeRead {
			fired = func() bool { return false }
			return
		}
		if tg.prefix == "" {
			fired = armAggFault(t, sm.SpillDir(), f)
		} else if tg.prefix == "raw:" {
			fired = armRawRowFault(t, sm, f)
		} else {
			fired = armSweepFault(t, tg.prefix, f)
		}
	}
	if entry == "own" {
		arm()
	}
	err = o.feed(ctx)
	if err == nil && entry != "own" {
		if n := len(o.runs()); n != 0 {
			t.Fatalf("fixture: the %s spilled %d runs on its own; the relief schedule needs it resident", tg.name, n)
		}
		arm()
		switch entry {
		case "request":
			_, out.reliefEr = sm.RequestRelief(resident)
		case "backstop":
			_, _ = o.op.SpillSome(resident)
		}
	}
	out.runs = len(o.runs())
	if err == nil && f.mergeRead {
		if out.runs == 0 {
			t.Fatalf("schedule not reached: the %s wrote no run to truncate", tg.name)
		}
		truncateHalf(t, o.runs()[0])
		out.fired = true
	}
	if err == nil {
		out.answer, err = o.finish(ctx)
	}
	out.err = err
	out.fired = out.fired || fired()
	o.close()
	out.used = tr.Used()
	out.files = spillDirEntries(t, sm.SpillDir())
	return out
}

func TestSpillFaultSweepIsLoudOrRight(t *testing.T) {
	for _, tg := range sweepTargets {
		ref := tg.ref
		if ref == "" {
			ref = sweepPositional(func() []map[string]any {
				var rows []map[string]any
				for _, b := range sweepInput() {
					rows = append(rows, b.ToRows()...)
				}
				return rows
			}())
		}
		entries := []string{"own"}
		if tg.relief {
			entries = append(entries, "request", "backstop")
		}
		for _, entry := range entries {
			for _, f := range sweepFaults {
				if tg.faults != nil && !strings.Contains(strings.Join(tg.faults, ","), f.name) {
					continue
				}
				if f.mergeRead && entry != "own" {
					continue // the merge reads what the own spill wrote
				}
				t.Run(tg.name+"/"+entry+"/"+f.name, func(t *testing.T) {
					out := runSweep(t, tg, entry, f)
					verdict := "RIGHT"
					switch {
					case out.err != nil:
						verdict = "LOUD"
					case out.answer != ref:
						verdict = "WRONG"
					}
					staleLogMu.Lock()
					t.Logf("SWEEP %s/%s/%s verdict=%s fired=%v runs=%d answer=%s err=%v relief_err=%v used_after_close=%d files_after_close=%d",
						tg.name, entry, f.name, verdict, out.fired, out.runs, out.answer, out.err, out.reliefEr, out.used, out.files)
					staleLogMu.Unlock()
					if !out.fired {
						t.Skipf("not reached: the %s fault was never taken on the %s's %s spill", f.name, tg.name, entry)
					}
					if verdict == "WRONG" {
						t.Errorf("answered %s after the %s fault on its %s spill; reference %s", out.answer, f.name, entry, ref)
					}
					if out.used != 0 {
						t.Errorf("tracker holds %d bytes after Close", out.used)
					}
				})
			}
		}
	}
}
