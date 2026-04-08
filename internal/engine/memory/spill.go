package memory

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
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

// SpillManager handles spilling data to disk when memory budget is exceeded.
type SpillManager struct {
	dir     string
	tracker *Tracker
	mu      sync.Mutex
	files   []string
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

// heapPressureRatio is the fraction of GOMEMLIMIT at which we trigger
// spill. 0.7 = spill when the heap is at 70% of the soft memory limit.
// Tunable upward for more aggressive caching, downward for more aggressive
// spilling on memory-tight workloads.
const heapPressureRatio = 0.7

var (
	heapPressureMu        sync.Mutex
	heapPressureLastCheck time.Time
	heapPressureLastValue bool
	heapPressureMemLimit  int64 // cached debug.SetMemoryLimit value
)

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
	heapPressureLastValue = int64(ms.HeapAlloc) > threshold
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
