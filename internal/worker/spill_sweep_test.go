package worker

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

// TestSweepStaleSpillArtifacts covers the startup sweep of orphaned spill-dir
// artifacts from a prior worker process. Regression for SF100 run
// 20260610-203304: the sweep previously only matched build-cache-*.wshf, so
// shuffle-<taskID>/ partition dirs and parquet-stream-*.parquet downloads
// left by a crashed or killed worker survived restarts and silently ate the
// spill volume.
func TestSweepStaleSpillArtifacts(t *testing.T) {
	dir := t.TempDir()

	mustWrite := func(rel string) string {
		t.Helper()
		full := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		return full
	}

	// Orphans the sweep must remove.
	swept := []string{
		mustWrite("build-cache-123.wshf"),
		mustWrite("build-cache-load-456.wshf"),
		mustWrite("parquet-stream-789.parquet"),
		mustWrite("shuffle-deadbeef/part-0000.wshf"),
	}
	sweptDir := filepath.Join(dir, "shuffle-deadbeef")

	// Files the sweep must NOT touch.
	kept := []string{
		mustWrite("stage-task1.wshf"),               // stage sink output naming differs
		mustWrite("sort-run-1.1.bin"),               // operator spill (task-scoped lifecycle)
		mustWrite("stage-cache/q1/abc_part.wshf"),   // LocalStageCache tree (wiped by its own constructor)
		mustWrite("parquet-stream-789.parquet.tmp"), // wrong suffix
	}

	w := &Worker{
		config: Config{SpillDir: dir},
		logger: slog.Default(),
	}
	w.sweepStaleBuildCacheFiles()

	for _, p := range swept {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("sweep should have removed %s (stat err=%v)", p, err)
		}
	}
	if _, err := os.Stat(sweptDir); !os.IsNotExist(err) {
		t.Errorf("sweep should have removed dir %s (stat err=%v)", sweptDir, err)
	}
	for _, p := range kept {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("sweep must not touch %s: %v", p, err)
		}
	}
}
