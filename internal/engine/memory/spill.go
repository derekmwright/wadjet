package memory

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// SpillManager handles spilling data to disk when memory budget is exceeded.
type SpillManager struct {
	dir     string
	tracker *Tracker
	mu      sync.Mutex
	files   []string
	nextID  atomic.Int64
}

// NewSpillManager creates a spill manager that writes temp files to the given directory.
func NewSpillManager(dir string, tracker *Tracker) (*SpillManager, error) {
	spillDir := filepath.Join(dir, "caelum-spill")
	if err := os.MkdirAll(spillDir, 0700); err != nil {
		return nil, fmt.Errorf("creating spill dir: %w", err)
	}
	return &SpillManager{
		dir:     spillDir,
		tracker: tracker,
	}, nil
}

// ShouldSpill returns true if the tracker's memory budget is exhausted.
func (sm *SpillManager) ShouldSpill() bool {
	if sm.tracker == nil || sm.tracker.Budget() <= 0 {
		return false
	}
	return sm.tracker.Used() > sm.tracker.Budget()*80/100 // spill at 80%
}

// SpillRows writes rows to a temporary file on disk and returns the file path.
func (sm *SpillManager) SpillRows(rows []map[string]any) (string, error) {
	id := sm.nextID.Add(1)
	path := filepath.Join(sm.dir, fmt.Sprintf("spill-%d.json", id))

	f, err := os.Create(path)
	if err != nil {
		return "", fmt.Errorf("creating spill file: %w", err)
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	for _, row := range rows {
		if err := enc.Encode(row); err != nil {
			return "", fmt.Errorf("encoding row to spill: %w", err)
		}
	}

	sm.mu.Lock()
	sm.files = append(sm.files, path)
	sm.mu.Unlock()

	return path, nil
}

// ReadSpilledRows reads all rows from a spilled file.
func ReadSpilledRows(path string) ([]map[string]any, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening spill file: %w", err)
	}
	defer f.Close()

	var rows []map[string]any
	dec := json.NewDecoder(f)
	for dec.More() {
		var row map[string]any
		if err := dec.Decode(&row); err != nil {
			return nil, fmt.Errorf("decoding spilled row: %w", err)
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
