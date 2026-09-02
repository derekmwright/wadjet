package exec

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Every caller of CloneSink consults the clone fence.
//
// #703's first fix guarded exec.Pipeline.runParallel and stopped there. There
// was a SECOND caller — the worker's runBreakerConsumeParallel — and with it
// unguarded the whole defect stayed reachable on the stage DAG: four morsel
// workers made `SUM(DISTINCT a)` answer 64.96 for 16.24, exactly four times
// over. Correctness-fix protocol rule 6 is "enumerate the callers"; this test
// is that enumeration, mechanised, so a third call site cannot be added
// without either consulting SinkSurvivesCloning or editing this list and
// saying why.
//
// It greps rather than parses on purpose: the property is about the SOURCE a
// reviewer reads, and a caller that reaches CloneSink through an alias is
// exactly the shape a type-aware check would wave through.
func TestCloneSinkCallersConsultTheCloneFence(t *testing.T) {
	// Every file that CALLS CloneSink, with the fence it consults. A file
	// listed here with an empty reason must contain SinkSurvivesCloning.
	known := map[string]string{
		"internal/engine/exec/pipeline.go":     "",
		"internal/worker/executor_fragment.go": "",
		"internal/engine/exec/exec.go":         "declares the interface, does not call it",
		"internal/engine/exec/aggregate.go":    "implements CloneSink for HashAggregate",
		"internal/engine/exec/sort.go":         "implements CloneSink for Sort",
		"internal/engine/exec/window.go":       "implements CloneSink for Window",
		"internal/engine/exec/join.go":         "implements CloneSink for HashJoin",
	}
	root := repoRootForCloneFence(t)
	call := regexp.MustCompile(`\.CloneSink\(\)`)
	var offenders []string
	var seen []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			// Skip what the Go toolchain itself skips: a directory whose name
			// begins with "." or "_" holds no package of this module. That is
			// not a tidiness rule here, it is the gate's correctness. A git
			// WORKTREE lives at .claude/worktrees/<name>/ and is a full second
			// copy of this source tree, so without this the walk reported the
			// call sites of every checked-out branch beside its own — and the
			// gate's verdict depended on whether somebody happened to have a
			// worktree open. It passed in the two agent worktrees that wrote
			// and reviewed it (neither contains one) and failed on main, which
			// does. Nested worktrees under other projects reached it too.
			if info != nil && info.IsDir() && path != root {
				if n := info.Name(); strings.HasPrefix(n, ".") || strings.HasPrefix(n, "_") ||
					n == "node_modules" || n == "dist" {
					return filepath.SkipDir
				}
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		body, rerr := os.ReadFile(path)
		if rerr != nil {
			return nil
		}
		if !call.Match(body) {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		seen = append(seen, rel)
		reason, listed := known[rel]
		switch {
		case !listed:
			offenders = append(offenders, rel+" calls CloneSink and is not in the known list")
		case reason == "" && !strings.Contains(stripLineComments(string(body)), "SinkSurvivesCloning("):
			offenders = append(offenders, rel+" calls CloneSink without consulting SinkSurvivesCloning")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(seen) == 0 {
		t.Fatalf("found no caller of CloneSink at all under %s — the walk is not looking where it thinks", root)
	}
	t.Logf("CloneSink callers: %v", seen)
	for _, o := range offenders {
		t.Errorf("%s\n  A sink carrying a DISTINCT aggregate has no mergeable partial form (#291, "+
			"#703): cloning one double-counts every shared value, silently. Consult "+
			"exec.SinkSurvivesCloning at the site, or add the file to this test's list with the "+
			"reason it cannot reach a distinct sink.", o)
	}
}

// stripLineComments removes `//` comments so a file that merely NAMES the
// fence in prose cannot satisfy the check. The first draft of this test did
// exactly that: the worker's comment explains the fence, so deleting the CALL
// left the test green — rule 2 pointed at the gate itself.
func stripLineComments(src string) string {
	var b strings.Builder
	for _, line := range strings.Split(src, "\n") {
		if i := strings.Index(line, "//"); i >= 0 {
			line = line[:i]
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	return b.String()
}

// repoRootForCloneFence walks up from the package directory to the module root.
func repoRootForCloneFence(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for i := 0; i < 8; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		dir = filepath.Dir(dir)
	}
	t.Fatal("could not find the module root above the exec package")
	return ""
}
