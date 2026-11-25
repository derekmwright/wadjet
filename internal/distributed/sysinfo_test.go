// SPDX-License-Identifier: AGPL-3.0-only

package distributed

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/memory"
)

// TestProcessRSS_NonZero asserts ProcessRSS returns a plausible non-zero figure
// on every platform: real RSS via /proc/self/statm on Linux, the HeapInuse
// fallback elsewhere. (At SF0.01/local this is the whole working set in heap;
// the mmap-vs-heap split only becomes interesting at SF100.)
func TestProcessRSS_NonZero(t *testing.T) {
	if got := ProcessRSS(); got <= 0 {
		t.Fatalf("ProcessRSS() = %d, want > 0 (statm on linux, HeapInuse fallback otherwise)", got)
	}
}

// TestProcessRSS_TracksAllocation is a soft check that RSS reflects a large live
// allocation. Skipped off Linux (the fallback is HeapInuse, which may or may not
// grow depending on GC). The floor assertion in TestProcessRSS_NonZero is the
// load-bearing one; this is best-effort.
func TestProcessRSS_TracksAllocation(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("RSS-growth check is Linux-only (non-linux uses the HeapInuse fallback)")
	}
	before := ProcessRSS()
	const n = 64 << 20
	buf := make([]byte, n)
	for i := 0; i < n; i += 4096 { // touch every page so it's resident
		buf[i] = 1
	}
	after := ProcessRSS()
	runtime.KeepAlive(buf)
	if after <= before {
		// Not a hard failure — page reclaim / scheduling can confound it —
		// but log so a regression in the reader is visible.
		t.Logf("RSS did not grow after 64MB touch: before=%d after=%d (informational)", before, after)
	}
}

func TestProcessRSSFromStatm(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want int64 // in pages (multiplied by pageSize in the assert)
		ok   bool
	}{
		{"typical", "839 252 228 4 0 124 0", 252, true},
		{"two_fields", "1000 500", 500, true},
		{"one_field", "1000", 0, false},
		{"empty", "", 0, false},
		{"malformed_resident", "1000 notanint 5", 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := processRSSFromStatm([]byte(tc.in))
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v", ok, tc.ok)
			}
			if ok && got != tc.want*pageSize {
				t.Fatalf("bytes = %d, want %d (%d pages × %d)", got, tc.want*pageSize, tc.want, pageSize)
			}
		})
	}
}

// TestDirDiskUsage_CountsStatementScopeFiles pins that the worker heartbeat's
// SpillDiskUsed sees operator spill. Every worker task's planner spills through
// SharedSpillMgr.StatementScope(), whose files live in
// <dir>/wadjet-spill/stmt-<pid>-<n>/ rather than directly in wadjet-spill/; a
// count that skipped those directories reported ~0 for operator spill, and the
// harness's spill_bytes / RunPeakSpillBytes read that figure.
func TestDirDiskUsage_CountsStatementScopeFiles(t *testing.T) {
	dir := t.TempDir()
	sm, err := memory.NewSpillManager(dir, memory.NewTracker("test", 1<<30))
	if err != nil {
		t.Fatalf("NewSpillManager: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "root.bin"), make([]byte, 1000), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sm.SpillDir(), "pool.bin"), make([]byte, 200), 0o600); err != nil {
		t.Fatal(err)
	}
	scope := sm.StatementScope()
	defer scope.Cleanup()
	stmtDir := scope.SpillDir()
	if filepath.Dir(stmtDir) != sm.SpillDir() {
		t.Fatalf("statement scope dir %q is not directly under the pool's %q", stmtDir, sm.SpillDir())
	}
	if err := os.WriteFile(filepath.Join(stmtDir, "run.bin"), make([]byte, 5000), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, want := DirDiskUsage(dir), int64(1000+200+5000); got != want {
		t.Fatalf("DirDiskUsage = %d, want %d (root file + pool file + statement-scope file)", got, want)
	}
	if err := scope.Cleanup(); err != nil {
		t.Fatalf("scope Cleanup: %v", err)
	}
	if got, want := DirDiskUsage(dir), int64(1000+200); got != want {
		t.Fatalf("after scope Cleanup DirDiskUsage = %d, want %d", got, want)
	}
}

// TestDirDiskUsage_WalksTheWholeSpillTreeWithoutFollowingLinks pins that the
// count does not depend on how deep under wadjet-spill an operator writes, and
// that it neither follows a symbolic link below wadjet-spill (to a directory
// outside it, or back up to make a cycle) nor counts dir's other
// subdirectories.
func TestDirDiskUsage_WalksTheWholeSpillTreeWithoutFollowingLinks(t *testing.T) {
	dir := t.TempDir()
	deep := filepath.Join(dir, "wadjet-spill", "stmt-1-1", "op", "part-3")
	if err := os.MkdirAll(deep, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(deep, "run.bin"), make([]byte, 7000), 0o600); err != nil {
		t.Fatal(err)
	}
	// Not spill: a sibling of wadjet-spill, and a big directory outside dir.
	other := filepath.Join(dir, "stage-cache")
	if err := os.MkdirAll(other, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(other, "cached.bin"), make([]byte, 50000), 0o600); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "big.bin"), make([]byte, 90000), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(dir, "wadjet-spill", "stmt-1-1", "to-outside")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if err := os.Symlink(filepath.Join(dir, "wadjet-spill"), filepath.Join(deep, "cycle")); err != nil {
		t.Fatal(err)
	}
	var links int64
	for _, l := range []string{filepath.Join(dir, "wadjet-spill", "stmt-1-1", "to-outside"), filepath.Join(deep, "cycle")} {
		fi, err := os.Lstat(l)
		if err != nil {
			t.Fatal(err)
		}
		links += fi.Size()
	}
	if got, want := DirDiskUsage(dir), 7000+links; got != want {
		t.Fatalf("DirDiskUsage = %d, want %d (the file three levels below a statement scope + the two links' own sizes; nothing through a link, nothing in stage-cache)", got, want)
	}
}
