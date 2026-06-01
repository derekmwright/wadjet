package memory

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Binary spill format type tags
const (
	spillTagNull      byte = 0
	spillTagBoolFalse byte = 1
	spillTagBoolTrue  byte = 2
	spillTagInt64     byte = 3
	spillTagFloat64   byte = 4
	spillTagString    byte = 5
	spillTagInt32     byte = 6
	spillTagFloat32   byte = 7
	spillRowMarker    byte = 0x01
	spillEndMarker    byte = 0x00
)

// spillFileSeq is a global atomic counter for unique spill file names.
// Prevents filename collisions when multiple SpillManagers (from concurrent
// tasks on the same worker) write to the same directory.
var spillFileSeq atomic.Int64

// SpillManager handles spilling data to disk when memory budget is exceeded.
type SpillManager struct {
	dir     string
	tracker *Tracker
	mu      sync.Mutex
	files   []string

	// --- AccountedOperator registry (Phase 2) ---
	// The new registry replacing spillables/departed. Operators register via
	// RegisterAccounted and the floating-budget RequestRelief ranks them by
	// SpillableBytes. accountedPeak survives an operator's Close so a closed
	// operator's peak OwnedBytes is still reported by Inspect.
	accountedMu        sync.Mutex
	accounted          []AccountedOperator
	accountedPeak      map[uint64]int64    // InstanceID -> peak OwnedBytes
	departedFootprints []OperatorFootprint // final snapshot of closed operators

	// reservoirs sources the floating operator budget (GOMEMLIMIT − Σreservoir
	// actual − GC headroom). In Phase 3 it is wired for ACCOUNTING/observability
	// (Available()/Inspect) even while the floating threshold stays dormant. nil
	// falls back to the static tracker.Budget().
	reservoirs *ReservoirRegistry

	// floatingBudgetActive gates whether ShouldSpillFor thresholds against the
	// floating budget. Default false: reservoirs may be wired for accounting
	// while the spill DECISION still uses the tuned static 40%/90% path. Flipped
	// true only by SetFloatingBudgetActive — deploy-gated, safe only once the
	// system reservoirs (and Phase-4 mmap RSS-sampling) account for the
	// untracked transient heap that makes a 0.90 threshold safe.
	floatingBudgetActive bool

	cheapFrac float64 // --spill-cheap-fraction, default 0.90

	// relief bookkeeping for RequestRelief: per-instance cooldown after a
	// delivered<claimed shortfall, and a round-robin cursor for the
	// rotate-on-unrelieved-pressure pass.
	reliefMu       sync.Mutex
	lastReliefAt   map[uint64]time.Time
	rotationCursor int
}

// defaultSpillCheapFraction is the fraction of the floating operator budget at
// which SpillCheap operators begin spilling. The deploy can override it via
// --spill-cheap-fraction; the SF100 A/B tunes it (design memo R1).
const defaultSpillCheapFraction = 0.90

// NewSpillManager creates a spill manager that writes temp files to the given directory.
func NewSpillManager(dir string, tracker *Tracker) (*SpillManager, error) {
	spillDir := filepath.Join(dir, "wadjet-spill")
	if err := os.MkdirAll(spillDir, 0700); err != nil {
		return nil, fmt.Errorf("creating spill dir: %w", err)
	}
	return &SpillManager{
		dir:           spillDir,
		tracker:       tracker,
		cheapFrac:     defaultSpillCheapFraction,
		accountedPeak: make(map[uint64]int64),
		lastReliefAt:  make(map[uint64]time.Time),
	}, nil
}

// SetReservoirs wires the reservoir registry for ACCOUNTING — it makes
// Available()/Inspect reflect live reservoir occupancy. It does NOT activate the
// floating spill threshold; ShouldSpillFor stays on the tuned static 40%/90%
// path until SetFloatingBudgetActive(true) is called separately. Safe to call
// once at construction; nil leaves the static fallback.
func (sm *SpillManager) SetReservoirs(rr *ReservoirRegistry) { sm.reservoirs = rr }

// SetFloatingBudgetActive enables (true) or disables (false, default) the
// floating-budget spill threshold in ShouldSpillFor. When false, ShouldSpillFor
// uses the tuned static 40%/90% thresholds even if a reservoir registry is
// wired. When true, SpillCheap triggers at cheapFrac of the live floating budget
// (GOMEMLIMIT − Σreservoir actual − GC headroom) and SpillExpensive at 0.98.
// Deploy-gated: safe only once the system reservoirs account for the untracked
// transient heap (mmap working set via Phase-4 RSS-sampling).
func (sm *SpillManager) SetFloatingBudgetActive(active bool) { sm.floatingBudgetActive = active }

// SetCheapFraction overrides the SpillCheap threshold fraction (default 0.90).
// Ignored unless 0 < f < 1.
func (sm *SpillManager) SetCheapFraction(f float64) {
	if f > 0 && f < 1 {
		sm.cheapFrac = f
	}
}

// RegisterAccounted adds an AccountedOperator to the Phase-2 relief registry
// and returns an unregister closure. The closure captures the operator's final
// footprint (peak OwnedBytes) into departedFootprints so a closed operator's
// peak is still surfaced by Inspect.
func (sm *SpillManager) RegisterAccounted(op AccountedOperator) func() {
	sm.accountedMu.Lock()
	sm.accounted = append(sm.accounted, op)
	if sm.accountedPeak == nil {
		sm.accountedPeak = make(map[uint64]int64)
	}
	sm.accountedMu.Unlock()
	return func() {
		sm.accountedMu.Lock()
		defer sm.accountedMu.Unlock()
		for i, x := range sm.accounted {
			if x == op {
				f := op.Inspect() // Closed-zeros; peak comes from accountedPeak
				f.OwnedBytes = sm.accountedPeak[f.InstanceID]
				sm.departedFootprints = append(sm.departedFootprints, f)
				sm.accounted = append(sm.accounted[:i], sm.accounted[i+1:]...)
				return
			}
		}
	}
}

// SpillUrgency describes how much pressure is needed before this operator
// should spill. Operators self-classify based on the cost of their spill path.
//
// SpillCheap is for spill paths that are bounded and recoverable: build-side
// hash tables, hash-aggregate hash tables. Triggering slightly early costs
// little.
//
// SpillExpensive is for spill paths that stream large data to disk just to
// read it back: probe-side bridge collectors. Triggering this unnecessarily
// destroys wall-clock proportional to the probe table size.
type SpillUrgency int

const (
	SpillCheap     SpillUrgency = iota // spill when budget is 60% used
	SpillExpensive                     // spill when budget is 90% used
)

// ShouldSpillFor returns true when an operator with the given spill cost
// class should spill. SpillCheap operators trigger at 40% of the per-tracker
// budget; SpillExpensive operators trigger at 90%. Either class also triggers
// if the global heap-pressure circuit breaker fires.
//
// The 40% SpillCheap threshold (was 60% pre-2026-05-19) is sized so that
// 3 concurrent fragment tasks at SF100 mc=3 — each holding ~5.7 GB peak
// HashJoin/build=lineitem state on a 14.8 GB GOMEMLIMIT worker — stay
// cumulatively under the process heap limit. With per-task share = budget
// / 3 ≈ 5 GB, a 60% threshold lets each task ramp to 3 GB before spilling,
// and the 1.1× untracked overhead pushes 3 × 3 = 9 GB to ~10 GB heap. A
// 40% threshold caps the per-task pre-spill peak at 2 GB; cumulative
// 3 × 2 × 1.1 ≈ 6.6 GB heap. The architectural mechanism is documented
// in project_q17_sf100_instrumented_2026-05-17.md and
// project_bufio_fix_sf100_2026-05-18.md (worker-w0 hitting 19.7 GB
// during shuffle-stage-7 from concurrent fragment-task heap stacking).
//
// Threshold sweep on the local Q17 probe (TestHashJoin_Q17ShapeRepro):
//
//	threshold   peak heap (vs tracker budget)
//	30%         0.63×
//	40% (now)   0.74×
//	50%         0.92×
//	60% (was)   1.10×
//	70%         1.27×
//
// Wall time was flat across thresholds — earlier spill is essentially
// free on local NVMe. Spill bytes 67-80 MB across the sweep (lower
// threshold → more bytes spilled, as expected).
func (sm *SpillManager) ShouldSpillFor(urgency SpillUrgency) bool {
	// Floating-budget path: the threshold is a fraction of the LIVE available
	// operator budget (GOMEMLIMIT − Σreservoir actual − GC headroom) rather than
	// the static pool budget. cheapFrac (default 0.90) gates SpillCheap;
	// SpillExpensive uses 0.98. This path is DORMANT until floatingBudgetActive
	// is flipped (deploy-gated) — merely wiring a reservoir registry for
	// accounting does NOT activate it, because the floating budget is mmap-blind
	// until Phase-4 RSS-sampling and a 0.90 threshold against it would risk the
	// Q17/Q18 worker reap. Until activated, the static path below preserves the
	// tuned 40%/90% behavior.
	if sm.floatingBudgetActive && sm.reservoirs != nil {
		budget := sm.reservoirs.Available()
		if budget > 0 && budget != math.MaxInt64 {
			frac := sm.cheapFrac
			if urgency == SpillExpensive {
				frac = 0.98
			}
			if sm.tracker != nil && sm.tracker.Used() > int64(float64(budget)*frac) {
				return true
			}
		}
		return heapPressureExceeded()
	}
	if sm.tracker != nil && sm.tracker.Budget() > 0 {
		used := sm.tracker.Used()
		budget := sm.tracker.Budget()
		var threshold int64
		switch urgency {
		case SpillExpensive:
			threshold = budget * 90 / 100
		default:
			threshold = budget * 40 / 100
		}
		if used > threshold {
			return true
		}
	}
	return heapPressureExceeded()
}

// ShouldSpill returns true when the operator should spill to disk.
//
// It checks two independent signals:
//
//  1. **Per-tracker budget** — the original cooperative signal: each operator
//     reports its tracked allocations and spills when its share of the budget
//     is exhausted. Cheap (atomic load) and accurate when every allocation
//     paths through the tracker.
//
//  2. **Process-wide heap pressure** — checks runtime.MemStats.HeapAlloc
//     against GOMEMLIMIT and triggers spill when the heap approaches the
//     soft limit. This catches allocations that bypass the tracker (probe
//     pipeline batches, gather buffers, scan source channel buffers, every
//     non-build operator that doesn't currently report memory). Without
//     this signal, the SF100 deploy would hit 31 GB anon-rss with a 1.4 GB
//     tracker budget — the tracker's view of memory was 22× smaller than
//     reality, and the per-tracker spill check stayed under threshold while
//     the process climbed past physical RAM.
//
// runtime.ReadMemStats is moderately expensive (sub-millisecond), so the
// reading is rate-limited to once per 100 ms across all callers.
func (sm *SpillManager) ShouldSpill() bool {
	if sm.tracker != nil && sm.tracker.Budget() > 0 &&
		sm.tracker.Used() > sm.tracker.Budget()*60/100 {
		return true
	}
	return heapPressureExceeded()
}

// heapPressureRatio is the fraction of GOMEMLIMIT at which the global
// heap-pressure circuit breaker fires. This is a backstop for allocation
// paths that bypass the per-operator memory tracker. After Phase 1 of
// the spill-trigger redesign, the tracker should be accurate enough that
// this circuit breaker rarely or never fires; when it does, the WARN log
// is a signal that there's an unaccounted allocation site to fix.
//
// Set to 0.95 (was 0.5 in PR #38, 0.7 originally) so we only spill for
// genuine OOM-imminent situations.
const heapPressureRatio = 0.95

// heapBackpressureRatio is the fraction of GOMEMLIMIT at which fragment
// runners pause briefly before reading the next batch. Lower than the
// 0.95 spill threshold because backpressure is non-destructive — it just
// slows the producer to let GC catch up and downstream operators drain.
//
// 0.70 lines up with the GOGC=100 collection cycle: the GC keeps live
// heap to roughly half of the trigger point, so pausing when total heap
// crosses 70% of GOMEMLIMIT gives one full collection cycle of headroom
// before the 95% spill backstop or the cgroup MemoryHigh ceiling fire.
//
// Set via WADJET_HEAP_BACKPRESSURE_RATIO env var (e.g. "0.6") for tuning.
// Disable with "0" or by leaving GOMEMLIMIT unset.
const heapBackpressureRatio = 0.70

var (
	heapPressureMu        sync.Mutex
	heapPressureLastCheck time.Time
	heapPressureLastValue bool
	heapPressureMemLimit  int64 // cached debug.SetMemoryLimit value
)

var (
	heapBackpressureMu        sync.Mutex
	heapBackpressureLastCheck time.Time
	heapBackpressureLastValue bool
	heapBackpressureRatioOnce sync.Once
	heapBackpressureRatioEff  float64
)

func effectiveHeapBackpressureRatio() float64 {
	heapBackpressureRatioOnce.Do(func() {
		heapBackpressureRatioEff = heapBackpressureRatio
		if v := os.Getenv("WADJET_HEAP_BACKPRESSURE_RATIO"); v != "" {
			if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 && f < 1 {
				heapBackpressureRatioEff = f
			}
		}
	})
	return heapBackpressureRatioEff
}

// HeapBackpressurePauseDuration is how long PauseOnHeapBackpressure
// sleeps when HeapBackpressureActive fires. 50ms is one to two GC cycles
// at typical SF100 allocation rates — long enough for live heap to drop,
// short enough that downstream consumers don't time out.
const HeapBackpressurePauseDuration = 50 * time.Millisecond

// PauseOnHeapBackpressure is a one-line helper that callers can invoke
// between batches: if heap pressure is high, sleep briefly so GC can
// catch up. Returns ctx.Err() if the context is cancelled during the
// pause, nil otherwise (including when no pressure is detected).
//
// Cheap when no pressure: one cached-atomic check.
func PauseOnHeapBackpressure(ctx context.Context) error {
	if !HeapBackpressureActive() {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(HeapBackpressurePauseDuration):
	}
	return nil
}

// HeapBackpressureActive reports whether process heap usage is high
// enough that batch producers in fragment runners should pause briefly
// to let GC catch up and downstream operators drain. Cached for 100 ms
// across all callers so the per-batch overhead stays near zero.
//
// The signal is intentionally a coarse tide gauge — a process-wide
// HeapAlloc check, NOT a per-operator tracker check. The motivating
// failure mode (Q17 SF100, 2026-05-07) was tasks heap-thrashing while
// the tracker reported only ~80MB used because the actual 20GB+ of
// per-task heap lived in transient parquet decode + hash routing
// allocations that no operator owns long enough to be Spillable.
//
// Use this between batches in the consume loop, not in per-row hot
// paths. Returns false when GOMEMLIMIT is unset.
func HeapBackpressureActive() bool {
	ratio := effectiveHeapBackpressureRatio()
	if ratio <= 0 {
		return false
	}
	heapBackpressureMu.Lock()
	defer heapBackpressureMu.Unlock()

	if time.Since(heapBackpressureLastCheck) < 100*time.Millisecond {
		return heapBackpressureLastValue
	}
	heapBackpressureLastCheck = time.Now()

	// Reuse the spill-side cached limit lookup to avoid a second
	// debug.SetMemoryLimit call. Both functions share the same value once
	// cached.
	heapPressureMu.Lock()
	if heapPressureMemLimit == 0 {
		heapPressureMemLimit = debug.SetMemoryLimit(-1)
	}
	limit := heapPressureMemLimit
	heapPressureMu.Unlock()

	if limit <= 0 || limit == math.MaxInt64 {
		heapBackpressureLastValue = false
		return false
	}

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	threshold := int64(float64(limit) * ratio)
	heapBackpressureLastValue = int64(ms.HeapAlloc) > threshold
	return heapBackpressureLastValue
}

// heapPressureExceeded reads runtime.MemStats and returns true if the
// heap is using more than heapPressureRatio of GOMEMLIMIT. Result is
// cached for 100 ms to keep the per-call overhead near zero on hot paths.
func heapPressureExceeded() bool {
	heapPressureMu.Lock()
	defer heapPressureMu.Unlock()

	if time.Since(heapPressureLastCheck) < 100*time.Millisecond {
		return heapPressureLastValue
	}
	heapPressureLastCheck = time.Now()

	if heapPressureMemLimit == 0 {
		// debug.SetMemoryLimit(-1) is the standard "read current limit" idiom.
		heapPressureMemLimit = debug.SetMemoryLimit(-1)
	}
	if heapPressureMemLimit <= 0 || heapPressureMemLimit == math.MaxInt64 {
		// No GOMEMLIMIT set — heap pressure check is disabled.
		heapPressureLastValue = false
		return false
	}

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	threshold := int64(float64(heapPressureMemLimit) * heapPressureRatio)
	prev := heapPressureLastValue
	heapPressureLastValue = int64(ms.HeapAlloc) > threshold
	if heapPressureLastValue && !prev {
		// Transition false→true: log loudly because the tracker missed something.
		// This indicates an allocation site that should be added to the tracker.
		slog.Warn("heap-pressure spill triggered (likely tracker accounting gap)",
			"heap_alloc_mb", ms.HeapAlloc/(1<<20),
			"threshold_mb", threshold/(1<<20),
			"gomemlimit_mb", heapPressureMemLimit/(1<<20),
		)
	}
	return heapPressureLastValue
}

// TrackBatch adds an estimated batch size to the memory tracker.
// Unlike Reserve(), this always succeeds — it accumulates usage past the
// budget so ShouldSpill() can detect the threshold crossing.
func (sm *SpillManager) TrackBatch(bytes int64) {
	if sm.tracker != nil && bytes > 0 {
		sm.tracker.ForceReserve(bytes)
	}
}

// ReleaseTracking releases the given amount from the memory tracker after
// spilling frees memory. Callers must track their own reserved amount and
// pass the delta. Do NOT use Reset() on shared trackers — it wipes other
// concurrent operators' accounting.
func (sm *SpillManager) ReleaseTracking(bytes int64) {
	if sm.tracker != nil && bytes > 0 {
		sm.tracker.Release(bytes)
	}
}

// SpillRows writes rows to a temporary binary file on disk and returns the file path.
// Format: [column names header] [row marker + typed values per column]... [end marker]
func (sm *SpillManager) SpillRows(rows []map[string]any) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}

	id := spillFileSeq.Add(1)
	path := filepath.Join(sm.dir, fmt.Sprintf("spill-%d.%d.bin", os.Getpid(), id))

	f, err := os.Create(path)
	if err != nil {
		return "", fmt.Errorf("creating spill file: %w", err)
	}
	defer f.Close()

	w := bufio.NewWriterSize(f, 64*1024) // 64KB write buffer

	// Derive stable column order from the first row
	columns := make([]string, 0, len(rows[0]))
	for k := range rows[0] {
		columns = append(columns, k)
	}
	sort.Strings(columns) // deterministic order

	// Write header: column count + column names
	var buf [8]byte
	binary.LittleEndian.PutUint32(buf[:4], uint32(len(columns)))
	w.Write(buf[:4])
	for _, name := range columns {
		binary.LittleEndian.PutUint16(buf[:2], uint16(len(name)))
		w.Write(buf[:2])
		w.WriteString(name)
	}

	// Write rows
	for _, row := range rows {
		w.WriteByte(spillRowMarker)
		for _, col := range columns {
			v := row[col]
			if v == nil {
				w.WriteByte(spillTagNull)
				continue
			}
			switch val := v.(type) {
			case bool:
				if val {
					w.WriteByte(spillTagBoolTrue)
				} else {
					w.WriteByte(spillTagBoolFalse)
				}
			case int64:
				w.WriteByte(spillTagInt64)
				binary.LittleEndian.PutUint64(buf[:8], uint64(val))
				w.Write(buf[:8])
			case int:
				w.WriteByte(spillTagInt64)
				binary.LittleEndian.PutUint64(buf[:8], uint64(val))
				w.Write(buf[:8])
			case int32:
				w.WriteByte(spillTagInt32)
				binary.LittleEndian.PutUint32(buf[:4], uint32(val))
				w.Write(buf[:4])
			case float64:
				w.WriteByte(spillTagFloat64)
				binary.LittleEndian.PutUint64(buf[:8], math.Float64bits(val))
				w.Write(buf[:8])
			case float32:
				w.WriteByte(spillTagFloat32)
				binary.LittleEndian.PutUint32(buf[:4], math.Float32bits(val))
				w.Write(buf[:4])
			case string:
				w.WriteByte(spillTagString)
				binary.LittleEndian.PutUint32(buf[:4], uint32(len(val)))
				w.Write(buf[:4])
				w.WriteString(val)
			default:
				// Fallback: encode as string via fmt
				s := fmt.Sprintf("%v", val)
				w.WriteByte(spillTagString)
				binary.LittleEndian.PutUint32(buf[:4], uint32(len(s)))
				w.Write(buf[:4])
				w.WriteString(s)
			}
		}
	}
	w.WriteByte(spillEndMarker)
	if err := w.Flush(); err != nil {
		return "", fmt.Errorf("flushing spill file: %w", err)
	}

	sm.mu.Lock()
	sm.files = append(sm.files, path)
	sm.mu.Unlock()

	return path, nil
}

// ReadSpilledRows reads all rows from a binary spilled file.
func ReadSpilledRows(path string) ([]map[string]any, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening spill file: %w", err)
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 64*1024)
	var buf [8]byte

	// Read header: column names
	if _, err := io.ReadFull(r, buf[:4]); err != nil {
		return nil, fmt.Errorf("reading column count: %w", err)
	}
	numCols := int(binary.LittleEndian.Uint32(buf[:4]))
	columns := make([]string, numCols)
	for i := 0; i < numCols; i++ {
		if _, err := io.ReadFull(r, buf[:2]); err != nil {
			return nil, fmt.Errorf("reading column name length: %w", err)
		}
		nameLen := int(binary.LittleEndian.Uint16(buf[:2]))
		nameBuf := make([]byte, nameLen)
		if _, err := io.ReadFull(r, nameBuf); err != nil {
			return nil, fmt.Errorf("reading column name: %w", err)
		}
		columns[i] = string(nameBuf)
	}

	// Read rows
	var rows []map[string]any
	for {
		marker, err := r.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("reading row marker: %w", err)
		}
		if marker == spillEndMarker {
			break
		}

		row := make(map[string]any, numCols)
		for _, col := range columns {
			tag, err := r.ReadByte()
			if err != nil {
				return nil, fmt.Errorf("reading type tag for %q: %w", col, err)
			}
			switch tag {
			case spillTagNull:
				row[col] = nil
			case spillTagBoolFalse:
				row[col] = false
			case spillTagBoolTrue:
				row[col] = true
			case spillTagInt64:
				if _, err := io.ReadFull(r, buf[:8]); err != nil {
					return nil, err
				}
				row[col] = int64(binary.LittleEndian.Uint64(buf[:8]))
			case spillTagInt32:
				if _, err := io.ReadFull(r, buf[:4]); err != nil {
					return nil, err
				}
				row[col] = int32(binary.LittleEndian.Uint32(buf[:4]))
			case spillTagFloat64:
				if _, err := io.ReadFull(r, buf[:8]); err != nil {
					return nil, err
				}
				row[col] = math.Float64frombits(binary.LittleEndian.Uint64(buf[:8]))
			case spillTagFloat32:
				if _, err := io.ReadFull(r, buf[:4]); err != nil {
					return nil, err
				}
				row[col] = math.Float32frombits(binary.LittleEndian.Uint32(buf[:4]))
			case spillTagString:
				if _, err := io.ReadFull(r, buf[:4]); err != nil {
					return nil, err
				}
				strLen := int(binary.LittleEndian.Uint32(buf[:4]))
				strBuf := make([]byte, strLen)
				if _, err := io.ReadFull(r, strBuf); err != nil {
					return nil, err
				}
				row[col] = string(strBuf)
			default:
				return nil, fmt.Errorf("unknown spill type tag %d", tag)
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// Cleanup removes all spill files.
func (sm *SpillManager) Cleanup() error {
	sm.mu.Lock()
	files := sm.files
	sm.files = nil
	sm.mu.Unlock()

	var firstErr error
	for _, f := range files {
		if err := os.Remove(f); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// SpillDir returns the directory used for spill files.
func (sm *SpillManager) SpillDir() string {
	return sm.dir
}

// SpilledFiles returns the list of current spill files.
func (sm *SpillManager) SpilledFiles() []string {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	result := make([]string, len(sm.files))
	copy(result, sm.files)
	return result
}

// Tracker returns the underlying memory tracker. May return nil if the
// SpillManager was constructed without one. Used by call sites that need
// to report tracker accounting outside of the operator-level spill API.
func (sm *SpillManager) Tracker() *Tracker {
	return sm.tracker
}

// reliefCooldown is how long an operator is rested after it delivers
// materially less relief than it claimed (a contract violation), so the
// advisor stops re-hammering a victim that can't actually free what it
// reports.
const reliefCooldown = 30 * time.Second

// nowFunc is the clock used by the relief cooldown. A package var so tests can
// advance time deterministically without sleeping.
var nowFunc = time.Now

// RequestRelief asks registered AccountedOperators to free at least target
// bytes by spilling. It ranks candidates by SpillableBytes descending and
// ACCUMULATES relief across all willing operators — it never skips an operator
// for being individually too small (that skip reanimated the chained
// build/probe deadlock, see project_admission_control_rejected_2026-05-18). It
// never gates entry, never sleeps on the streaming path, and never refuses an
// operator the chance to spill.
//
// After pass 1, if pressure persists, a rotate pass advances past the
// most-recently-tried victims so we don't re-hammer the same one. Finally, if
// nothing was freed AND the tracker is drifting (OwnedTotal materially exceeds
// tracker.Used, i.e. there is an accounting gap), the drift-backstop
// force-spills the largest-OwnedBytes operator regardless of its SpillableBytes
// claim — the reactive floor that honest accounting must not delete.
func (sm *SpillManager) RequestRelief(target int64) (int64, error) {
	if target <= 0 {
		return 0, nil
	}
	var freed int64

	for freed < target {
		cands := sm.rankCandidates()
		if len(cands) == 0 {
			break
		}
		progressed := false
		for _, c := range cands {
			if freed >= target {
				break
			}
			op := sm.opByID(c.InstanceID)
			if op == nil {
				continue
			}
			want := target - freed
			claimed := op.EstimateRelief(want)
			if claimed <= 0 {
				continue
			}
			before := op.Inspect().OwnedBytes
			n, err := op.SpillSome(want)
			if err != nil {
				return freed, err
			}
			after := op.Inspect().OwnedBytes
			delivered := before - after
			if n > 0 {
				freed += n
				progressed = true
			}
			// Cool down an operator that delivered materially less than it
			// claimed — its SpillableBytes is a lie and re-asking wastes work.
			if delivered < claimed/2 {
				sm.cooldown(c.InstanceID)
			}
		}
		if !progressed {
			break
		}
	}

	if freed >= target {
		return freed, nil
	}

	// Drift-backstop: pass 1 freed nothing and the tracker is under-counting
	// real heap. Force-spill the largest-OwnedBytes operator regardless of its
	// SpillableBytes claim. Operators that genuinely cannot spill return 0
	// (harmless); the point is to spill SOMETHING reactively rather than let
	// drifted bytes OOM the worker.
	if freed == 0 && sm.driftExceeds(0.20) {
		if n := sm.forceSpillLargestOwned(target); n > 0 {
			freed += n
		}
	}
	return freed, nil
}

// rankCandidates snapshots the accounted registry, refreshes per-instance peak
// OwnedBytes, and returns Active operators with SpillableBytes > 0 that are not
// in cooldown, sorted by SpillableBytes descending.
func (sm *SpillManager) rankCandidates() []OperatorFootprint {
	sm.accountedMu.Lock()
	snaps := make([]OperatorFootprint, 0, len(sm.accounted))
	for _, op := range sm.accounted {
		f := op.Inspect()
		if f.OwnedBytes > sm.accountedPeak[f.InstanceID] {
			sm.accountedPeak[f.InstanceID] = f.OwnedBytes
		}
		snaps = append(snaps, f)
	}
	sm.accountedMu.Unlock()

	out := snaps[:0]
	for _, f := range snaps {
		if f.State == OpActive && f.SpillableBytes > 0 && !sm.inCooldown(f.InstanceID) {
			out = append(out, f)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SpillableBytes > out[j].SpillableBytes })
	return out
}

// opByID returns the live AccountedOperator with the given InstanceID, or nil.
func (sm *SpillManager) opByID(id uint64) AccountedOperator {
	sm.accountedMu.Lock()
	defer sm.accountedMu.Unlock()
	for _, op := range sm.accounted {
		if op.Inspect().InstanceID == id {
			return op
		}
	}
	return nil
}

func (sm *SpillManager) cooldown(id uint64) {
	sm.reliefMu.Lock()
	if sm.lastReliefAt == nil {
		sm.lastReliefAt = make(map[uint64]time.Time)
	}
	sm.lastReliefAt[id] = nowFunc()
	sm.reliefMu.Unlock()
}

func (sm *SpillManager) inCooldown(id uint64) bool {
	sm.reliefMu.Lock()
	defer sm.reliefMu.Unlock()
	last, ok := sm.lastReliefAt[id]
	if !ok {
		return false
	}
	return nowFunc().Sub(last) < reliefCooldown
}

// HeapDrift returns the Phase-4 accounting drift in bytes:
//
//	HeapInuse − (Tracker.OwnedTotal + Σreservoir.Actual)
//
// the unaccounted Go-heap residual (parquet decode arenas, kernel scratch,
// shuffle decode buffers) that no operator or reservoir owns. The caller
// supplies heapInuse (from runtime.MemStats) so this method never pays the STW
// ReadMemStats cost — the worker stats loop already samples it. mmap'd files and
// spill-readback page cache are absent from HeapInuse AND from the accounting
// terms, so they're symmetrically excluded (no category error). May be negative
// (sample skew / eviction); callers report it as-is.
//
// NOTE: this is HeapInuse-shaped and deliberately DISTINCT from driftExceeds,
// which measures OwnedTotal−Used (a within-ledger reserve-vs-published gap that
// drives the relief backstop). They share only OwnedTotal(); do not unify them.
func (sm *SpillManager) HeapDrift(heapInuse int64) int64 {
	var owned int64
	if sm.tracker != nil {
		owned = sm.tracker.OwnedTotal()
	}
	var reservoirActual int64
	if sm.reservoirs != nil {
		reservoirActual = sm.reservoirs.TotalActual()
	}
	return heapInuse - owned - reservoirActual
}

// driftExceeds reports whether the published owned total materially exceeds the
// tracker's reserved total — i.e. operators own more real heap than the tracker
// reserved for, signalling an accounting gap. Returns false when the tracker
// reports nothing (no basis to compare).
//
// This is the within-ledger reserve-vs-published gap (OwnedTotal−Used) that
// gates the RequestRelief backstop — NOT the same as HeapDrift, which is the
// heap-truth residual (HeapInuse − accounted). Keep the two distinct.
func (sm *SpillManager) driftExceeds(frac float64) bool {
	if sm.tracker == nil {
		return false
	}
	used := sm.tracker.Used()
	if used <= 0 {
		return false
	}
	owned := sm.tracker.OwnedTotal()
	drift := float64(owned-used) / float64(used)
	return drift > frac
}

// forceSpillLargestOwned spills the operator with the largest OwnedBytes,
// ignoring its SpillableBytes claim. The backstop for the accounting-gap case.
func (sm *SpillManager) forceSpillLargestOwned(target int64) int64 {
	sm.accountedMu.Lock()
	var best AccountedOperator
	var bestOwned int64
	for _, op := range sm.accounted {
		f := op.Inspect()
		if f.State == OpActive && f.OwnedBytes > bestOwned {
			best = op
			bestOwned = f.OwnedBytes
		}
	}
	sm.accountedMu.Unlock()
	if best == nil {
		return 0
	}
	n, _ := best.SpillSome(target)
	return n
}

// Inspect returns the per-operator footprints for the AccountedOperator
// registry — live operators plus the final snapshots of departed ones (with
// their peak OwnedBytes restored from accountedPeak). Replaces Inspect() once
// the executor migrates to the new wire format.
func (sm *SpillManager) Inspect() []OperatorFootprint {
	sm.accountedMu.Lock()
	defer sm.accountedMu.Unlock()
	out := make([]OperatorFootprint, 0, len(sm.accounted)+len(sm.departedFootprints))
	out = append(out, sm.departedFootprints...)
	for _, op := range sm.accounted {
		f := op.Inspect()
		if f.OwnedBytes > sm.accountedPeak[f.InstanceID] {
			sm.accountedPeak[f.InstanceID] = f.OwnedBytes
		}
		out = append(out, f)
	}
	return out
}
