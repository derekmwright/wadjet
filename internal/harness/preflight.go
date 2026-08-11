package harness

import (
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// PreflightResult holds the results of all preflight checks.
type PreflightResult struct {
	OK     bool
	Errors []string
}

func (r PreflightResult) Error() string {
	return strings.Join(r.Errors, "\n  - ")
}

// CheckPreflight runs all checks for the given slice and run dir.
func CheckPreflight(slice SliceConfig, runDir string, workerCount int) PreflightResult {
	var errs []string

	if runtime.GOOS != "linux" {
		errs = append(errs, fmt.Sprintf("harness requires linux (got %s)", runtime.GOOS))
	}

	// Free RAM: (GoMemLimit * workers) + 2 GB coord/harness overhead.
	// SF0.01 data is ~10 MB so the actual heap pressure is minimal;
	// GOMEMLIMIT is a soft cap, not a reservation.
	requiredRAMBytes := slice.GoMemLimit*int64(workerCount) + 2*int64(GB)
	if err := checkFreeRAM(requiredRAMBytes); err != nil {
		errs = append(errs, err.Error())
	}

	// Free disk: 20 GB at the spill mount.
	if err := checkFreeDisk(runDir, 20*int64(GB)); err != nil {
		errs = append(errs, err.Error())
	}

	// No orphaned wadjet processes.
	if err := checkNoOrphanedWadjet(); err != nil {
		errs = append(errs, err.Error())
	}

	return PreflightResult{OK: len(errs) == 0, Errors: errs}
}

func checkFreeRAM(required int64) error {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return fmt.Errorf("read /proc/meminfo: %w", err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "MemAvailable:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			break
		}
		kb, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return fmt.Errorf("parsing MemAvailable: %w", err)
		}
		availBytes := kb * 1024
		if availBytes < required {
			return fmt.Errorf("insufficient free RAM: have %d MB, need %d MB",
				availBytes/int64(MB), required/int64(MB))
		}
		return nil
	}
	return fmt.Errorf("MemAvailable not found in /proc/meminfo")
}

func checkFreeDisk(runDir string, required int64) error {
	parent := filepath.Dir(runDir)
	if err := os.MkdirAll(parent, 0755); err != nil {
		return fmt.Errorf("creating run dir parent %s: %w", parent, err)
	}
	var stat syscall.Statfs_t
	if err := syscall.Statfs(parent, &stat); err != nil {
		return fmt.Errorf("statfs %s: %w", parent, err)
	}
	availBytes := int64(stat.Bavail) * int64(stat.Bsize)
	if availBytes < required {
		return fmt.Errorf("insufficient free disk at %s: have %d MB, need %d MB",
			parent, availBytes/int64(MB), required/int64(MB))
	}
	return nil
}

// SweepStaleRunArtifacts removes leftover transient state from prior
// harness runs that crashed, timed out, or otherwise didn't reach
// the deferred cleanup paths. Called from Run() *before* CheckPreflight
// so the disk-space check sees a clean slate.
//
// Two sources of leakage we observe in practice:
//
//   - /tmp/wadjet-harness/run-<unix>/  - per-run logs + spill + JetStream
//     store. The harness removes it on success unless WADJET_HARNESS_KEEP=1
//     is set, but a panic / external SIGKILL leaves it. Each abandoned
//     SF1 run dir is several GB.
//
//   - <dataDir>/wadjet/queries/<query_id>/ - per-query intermediates that
//     the coordinator's cleanupQuery now removes on completion (committed
//     2542260), but a coordinator killed mid-flight by harness teardown
//     never reaches that hook. Each orphan query is ~1 GB at SF1 and
//     ~100 GB at SF10.
//
// Safety: the caller must invoke checkNoOrphanedWadjet first OR otherwise
// guarantee no concurrent wadjet process is touching these paths. With
// pruneOlderThan = 0 the function deletes everything; otherwise only
// entries with mtime older than that threshold are removed (use this to
// avoid sweeping a sibling harness's just-created run dir).
func SweepStaleRunArtifacts(harnessRoot, dataDir string, pruneOlderThan time.Duration, logger *slog.Logger) {
	if logger == nil {
		logger = slog.Default()
	}
	cutoff := time.Now().Add(-pruneOlderThan)

	sweep := func(dir string) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			if !os.IsNotExist(err) {
				logger.Debug("sweep: skipping", "dir", dir, "error", err)
			}
			return
		}
		var freed int64
		var removed int
		for _, e := range entries {
			full := filepath.Join(dir, e.Name())
			info, err := e.Info()
			if err != nil {
				continue
			}
			if pruneOlderThan > 0 && info.ModTime().After(cutoff) {
				continue
			}
			size, _ := dirBytes(full)
			if rmErr := os.RemoveAll(full); rmErr != nil {
				logger.Debug("sweep: remove failed", "path", full, "error", rmErr)
				continue
			}
			freed += size
			removed++
		}
		if removed > 0 {
			logger.Info("swept stale harness artifacts",
				"dir", dir, "entries_removed", removed,
				"bytes_freed", freed)
		}
	}

	if harnessRoot == "" {
		harnessRoot = "/tmp/wadjet-harness"
	}
	sweep(harnessRoot)
	if dataDir != "" {
		sweep(filepath.Join(dataDir, "wadjet", "queries"))
		sweepCompactionOrphans(filepath.Join(dataDir, "wadjet", "tables"), cutoff, pruneOlderThan, logger)
	}
}

// sweepCompactionOrphans removes compacted_*.parquet files a previous
// launch's coordinator left in the shared tables dir (issue #282). Each
// harness launch builds a fresh catalog over the chunk_* files it writes
// or adopts, so a prior launch's compaction outputs are referenced by no
// live catalog — and they accumulate fast: 180 orphans / 35 GB over four
// SF10 launches on 2026-08-10, filling the disk twice mid-validation.
// Same clean-before-generate rule as the EC2 GENERATE_DATA path. The
// caller guarantees no concurrent wadjet touches the dir (see
// SweepStaleRunArtifacts safety note); pruneOlderThan spares files a
// sibling harness may have just written.
func sweepCompactionOrphans(tablesDir string, cutoff time.Time, pruneOlderThan time.Duration, logger *slog.Logger) {
	var freed int64
	var removed int
	err := filepath.Walk(tablesDir, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil //nolint:nilerr // best-effort sweep: skip unreadable entries
		}
		base := filepath.Base(p)
		if !strings.HasPrefix(base, "compacted_") || !strings.HasSuffix(base, ".parquet") {
			return nil
		}
		if pruneOlderThan > 0 && info.ModTime().After(cutoff) {
			return nil
		}
		if rmErr := os.Remove(p); rmErr == nil {
			freed += info.Size()
			removed++
		}
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		logger.Debug("compaction-orphan sweep: walk ended early", "dir", tablesDir, "error", err)
	}
	if removed > 0 {
		logger.Info("swept orphaned compaction outputs",
			"dir", tablesDir, "files_removed", removed, "bytes_freed", freed)
	}
}

// dirBytes returns the cumulative size of regular files under path.
func dirBytes(path string) (int64, error) {
	var total int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // best-effort
		}
		if info.Mode().IsRegular() {
			total += info.Size()
		}
		return nil
	})
	return total, err
}

// checkNoOrphanedWadjet looks for wadjet processes spawned by a prior
// harness run (matching "wadjet serve --nats-url"). User-started standalone
// servers or unrelated processes are ignored.
func checkNoOrphanedWadjet() error {
	cmd := exec.Command("pgrep", "-f", "wadjet serve --nats-url")
	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() == 1 {
			return nil // no matches
		}
		return fmt.Errorf("pgrep failed: %w", err)
	}
	pids := strings.TrimSpace(string(out))
	if pids == "" {
		return nil
	}
	myPID := strconv.Itoa(os.Getpid())
	var others []string
	for _, p := range strings.Split(pids, "\n") {
		if p != myPID {
			others = append(others, p)
		}
	}
	if len(others) > 0 {
		return fmt.Errorf("orphaned harness-spawned wadjet processes detected: pids %s — kill them and re-run",
			strings.Join(others, " "))
	}
	return nil
}
