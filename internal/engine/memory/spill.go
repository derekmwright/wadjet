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
	spillTagNull       byte = 0
	spillTagBoolFalse  byte = 1
	spillTagBoolTrue   byte = 2
	spillTagInt64      byte = 3
	spillTagFloat64    byte = 4
	spillTagString     byte = 5
	spillTagInt32      byte = 6
	spillTagFloat32    byte = 7
	spillRowMarker     byte = 0x01
	spillEndMarker     byte = 0x00
)

// spillFileSeq is a global atomic counter for unique spill file names.
// Prevents filename collisions when multiple SpillManagers (from concurrent
// tasks on the same worker) write to the same directory.
var spillFileSeq atomic.Int64

// Spillable is implemented by operators (today: HashJoin) that hold
// reclaimable in-memory state and can write a portion of it to disk on
// request. The cooperative-spill advisor uses these methods to relieve
// shared-pool pressure when one operator can't make progress because another
// concurrent operator holds the bulk of the pool. Each operator's per-self
// reactive spill is still the first line of defense; cross-operator
// coordination only fires when an operator has exhausted its own partitions
// but tracker pressure remains.
type Spillable interface {
	// SpillFootprint reports how many bytes of in-memory reclaimable state
	// the operator currently holds. Used to pick the largest contributor
	// when the worker is over the spill threshold.
	SpillFootprint() int64

	// SpillSome attempts to free at least target bytes by writing in-memory
	// state to disk. Returns the number of bytes actually released back to
	// the shared tracker. May return less than target if the operator runs
	// out of evictable state. Must be safe to call from a goroutine other
	// than the one driving the operator's main pipeline.
	SpillSome(target int64) (int64, error)
}

// SpillManager handles spilling data to disk when memory budget is exceeded.
type SpillManager struct {
	dir     string
	tracker *Tracker
	mu      sync.Mutex
	files   []string

	// spillables registry — operators register on Build entry, deregister
	// on Close. RequestRelief picks the largest registered contributor and
	// asks it to spill. Independent of mu so registration doesn't contend
	// with file-list mutations.
	spillablesMu sync.Mutex
	spillables   []Spillable
}

// NewSpillManager creates a spill manager that writes temp files to the given directory.
func NewSpillManager(dir string, tracker *Tracker) (*SpillManager, error) {
	spillDir := filepath.Join(dir, "wadjet-spill")
	if err := os.MkdirAll(spillDir, 0700); err != nil {
		return nil, fmt.Errorf("creating spill dir: %w", err)
	}
	return &SpillManager{
		dir:     spillDir,
		tracker: tracker,
	}, nil
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
// class should spill. SpillCheap operators trigger at 60% of the per-tracker
// budget; SpillExpensive operators trigger at 90%. Either class also triggers
// if the global heap-pressure circuit breaker fires.
func (sm *SpillManager) ShouldSpillFor(urgency SpillUrgency) bool {
	if sm.tracker != nil && sm.tracker.Budget() > 0 {
		used := sm.tracker.Used()
		budget := sm.tracker.Budget()
		var threshold int64
		switch urgency {
		case SpillExpensive:
			threshold = budget * 90 / 100
		default:
			threshold = budget * 60 / 100
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

// RegisterSpillable adds an operator to the cooperative-spill registry.
// Returns an unregister function the caller must call (defer is fine) when
// the operator stops being a spill source — typically at Build completion
// or Close. Unregister is idempotent and safe to call after the SpillManager
// is no longer in use.
func (sm *SpillManager) RegisterSpillable(s Spillable) func() {
	sm.spillablesMu.Lock()
	sm.spillables = append(sm.spillables, s)
	sm.spillablesMu.Unlock()
	return func() {
		sm.spillablesMu.Lock()
		defer sm.spillablesMu.Unlock()
		for i, x := range sm.spillables {
			if x == s {
				sm.spillables = append(sm.spillables[:i], sm.spillables[i+1:]...)
				return
			}
		}
	}
}

// RequestRelief asks registered Spillables to free at least target bytes
// by spilling. Picks the largest contributor first and iterates until either
// target is met or every spillable returns 0. Returns the total bytes freed
// and any error from a Spillable's SpillSome call.
//
// Caller must NOT hold any operator's mutex when calling this — the
// requester operator is implicitly skipped via the largest-first ordering
// and zero-progress break, so a self-call where no other Spillable holds
// reclaimable state simply returns 0 cleanly. But to be safe under future
// callers, RequestRelief takes no operator-side locks itself; it only locks
// the registry briefly to snapshot.
func (sm *SpillManager) RequestRelief(target int64) (int64, error) {
	if target <= 0 {
		return 0, nil
	}
	var freed int64
	for freed < target {
		// Snapshot the registry, pick the largest. The snapshot is short-
		// lived so registrations during a long-running spill don't deadlock
		// the registry mutex.
		sm.spillablesMu.Lock()
		var best Spillable
		var bestSize int64
		for _, s := range sm.spillables {
			sz := s.SpillFootprint()
			if sz > bestSize {
				best = s
				bestSize = sz
			}
		}
		sm.spillablesMu.Unlock()
		if best == nil || bestSize == 0 {
			break
		}
		n, err := best.SpillSome(target - freed)
		if err != nil {
			return freed, err
		}
		if n == 0 {
			break
		}
		freed += n
	}
	return freed, nil
}
