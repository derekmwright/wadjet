package harness

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
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

	// Free RAM: (GoMemLimit * workers) + 3 GB coord/harness + 4 GB OS headroom
	requiredRAMBytes := slice.GoMemLimit*int64(workerCount) + 3*int64(GB) + 4*int64(GB)
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

func checkNoOrphanedWadjet() error {
	cmd := exec.Command("pgrep", "-f", "wadjet")
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
		return fmt.Errorf("orphaned wadjet processes detected: pids %s — kill them and re-run",
			strings.Join(others, " "))
	}
	return nil
}
