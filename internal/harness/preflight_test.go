package harness

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCheckFreeRAMDetectsExcessiveRequest(t *testing.T) {
	err := checkFreeRAM(1 << 50) // 1 PB
	if err == nil {
		t.Error("expected failure for 1PB request")
	}
}

func TestCheckFreeRAMTinyRequestSucceeds(t *testing.T) {
	err := checkFreeRAM(1 * int64(MB))
	if err != nil {
		t.Errorf("1 MB request failed: %v", err)
	}
}

func TestCheckFreeDiskTinyRequestSucceeds(t *testing.T) {
	dir := t.TempDir() + "/foo/bar"
	err := checkFreeDisk(dir, 1*int64(MB))
	if err != nil {
		t.Errorf("1 MB disk request failed: %v", err)
	}
}

// TestSweepCompactionOrphans (issue #282): a previous launch's
// compacted_*.parquet outputs must be removed from the tables dir while
// chunk files — the data the next launch adopts — survive, along with
// anything newer than the prune threshold (a sibling harness's fresh
// compaction).
func TestSweepCompactionOrphans(t *testing.T) {
	dataDir := t.TempDir()
	tbl := filepath.Join(dataDir, "wadjet", "tables", "lineitem")
	if err := os.MkdirAll(tbl, 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(name string) string {
		p := filepath.Join(tbl, name)
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	keepChunk := write("chunk_0001.parquet")
	oldOrphan := write("compacted_1786400000000000000.parquet")
	freshCompact := write("compacted_1786500000000000000.parquet")
	stale := time.Now().Add(-time.Hour)
	if err := os.Chtimes(oldOrphan, stale, stale); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(keepChunk, stale, stale); err != nil {
		t.Fatal(err)
	}

	SweepStaleRunArtifacts(filepath.Join(dataDir, "unused-harness-root"), dataDir, 30*time.Second, nil)

	if _, err := os.Stat(oldOrphan); !os.IsNotExist(err) {
		t.Fatalf("old compaction orphan survived the sweep (err=%v)", err)
	}
	if _, err := os.Stat(keepChunk); err != nil {
		t.Fatalf("chunk file swept: %v", err)
	}
	if _, err := os.Stat(freshCompact); err != nil {
		t.Fatalf("fresh compaction output swept despite prune threshold: %v", err)
	}
}
