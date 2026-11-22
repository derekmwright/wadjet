// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"crypto/sha256"
	"fmt"
	"io/fs"
	"os"
	osexec "os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
)

// r0WriterSwitches is every R0 output switch (the third independent review of
// R0, getenv.txt's inventory). TestR0WritersRefuseCommittedTargets reconciles
// it with the R0 test sources, so a new switch cannot escape the checks.
var r0WriterSwitches = []string{"R0_FIXTURE_WRITE", "R0_PG_WRITE", "R0_PINS_WRITE", "R0_REPORT", "R0_COST_WRITE",
	"R0_TPCH_REPORT", "R0_CENSUS", "R0_CHECKER_REPORT", "R0_BINDING_REPORT", "R0_ARMDIFF"}

// r0FileState is one file's identity and content, to show it was never opened
// for writing: same file (inode), same modification time, same bytes.
type r0FileState struct {
	fi  fs.FileInfo
	sum [32]byte
}

// r0SnapshotTree records every regular file under root.
func r0SnapshotTree(t *testing.T, root string) map[string]r0FileState {
	t.Helper()
	out := map[string]r0FileState{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		fi, err := os.Lstat(p)
		if err != nil {
			return err
		}
		b, err := os.ReadFile(p)
		if err != nil && fi.Mode().IsRegular() {
			return err
		}
		out[p] = r0FileState{fi: fi, sum: sha256.Sum256(b)}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// r0SameTree reports every file of before that is gone, is another file, was
// modified, or changed content in after.
func r0SameTree(before, after map[string]r0FileState) []string {
	var bad []string
	for p, b := range before {
		a, ok := after[p]
		switch {
		case !ok:
			bad = append(bad, p+": removed")
		case !os.SameFile(a.fi, b.fi):
			bad = append(bad, p+": replaced by another file")
		case !a.fi.ModTime().Equal(b.fi.ModTime()) || a.sum != b.sum:
			bad = append(bad, p+": written")
		}
	}
	sort.Strings(bad)
	return bad
}

// TestR0WritersRefuseCommittedTargets: no R0 writer can write a committed
// oracle, pin or baseline file, because no writer takes a path (the third
// independent review of R0, section B: a symbolic link followed by ".."
// defeated the path guard this replaces). It shows, in order:
//
//   - the switch inventory equals the R0_* writer switches the R0 test sources
//     read;
//   - every switch refuses every value but 1 (unset is off), with the usage:
//     the review's bypass spelling (a link to testdata/ followed by
//     ../testdata/<file>), its eight other spellings (relative, ./, absolute,
//     file symlink, hard link, testdata/../testdata/, a directory-symlink
//     alias, another package's testdata) and non-path values;
//   - the writer itself, with TMPDIR pointed INSIDE this package's testdata/
//     and a file name equal to a committed file's, creates a fresh uniquely
//     named directory and a new file there, and opens no pre-existing file for
//     writing (every file under testdata/ keeps its inode, mtime and bytes);
//   - the R0_ARMDIFF hook (no *testing.T) panics with the usage on a non-1
//     value and creates nothing until it records a cell;
//   - end to end, every writer test run in a child process with its switch set
//     to the bypass spelling, a relative and an absolute spelling, and "true"
//     fails with the usage before it measures or connects (an unreachable
//     PostgreSQL DSN is supplied; its TMPDIR stays empty) and leaves every
//     testdata file unchanged.
func TestR0WritersRefuseCommittedTargets(t *testing.T) {
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	td := filepath.Join(cwd, "testdata")

	// The inventory, reconciled with the sources.
	srcs, err := filepath.Glob("r0_*_test.go")
	if err != nil {
		t.Fatal(err)
	}
	read := map[string]bool{}
	re := regexp.MustCompile(`"(R0_[A-Z_]+)"`)
	for _, f := range srcs {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range re.FindAllStringSubmatch(string(b), -1) {
			if m[1] != "R0_ARMDIFF_TABLE" { // a report label, not an output
				read[m[1]] = true
			}
		}
	}
	for _, n := range r0WriterSwitches {
		if !read[n] {
			t.Errorf("%s is in the inventory but no R0 test source names it", n)
		}
		delete(read, n)
	}
	for n := range read {
		t.Errorf("the R0 test sources name %s, which is not in r0WriterSwitches", n)
	}

	// Every switch refuses every spelling but 1.
	tmp := t.TempDir()
	dirLink := filepath.Join(tmp, "TD")
	if err := os.Symlink(td, dirLink); err != nil {
		t.Fatal(err)
	}
	const f = "r0_corpus_pins.tsv"
	fileLink := filepath.Join(tmp, "sym-"+f)
	if err := os.Symlink(filepath.Join(td, f), fileLink); err != nil {
		t.Fatal(err)
	}
	bypass := dirLink + "/../testdata/" + f // the review's exact composition
	values := []string{bypass, "testdata/" + f, "./testdata/" + f, filepath.Join(td, f), fileLink,
		"testdata/../testdata/" + f, filepath.Join(dirLink, f), "dagplan/testdata/tpch_stage_dump.golden",
		filepath.Join(tmp, "out.tsv"), "true", "0", "yes", " 1", "1 ", "01"}
	if hard := filepath.Join(tmp, "hard-"+f); os.Link(filepath.Join(td, f), hard) == nil {
		values = append(values, hard)
	} else {
		t.Logf("no hard link from %s into testdata here: the hard-link value is still a non-1 value", tmp)
	}
	for _, n := range r0WriterSwitches {
		for _, v := range values {
			t.Setenv(n, v)
			if on, err := r0WriterOn(n); on || err == nil || !strings.Contains(err.Error(), "takes no path") {
				t.Errorf("%s=%q: on=%v err=%v, want the usage refusal", n, v, on, err)
			}
		}
		t.Setenv(n, "1")
		if on, err := r0WriterOn(n); !on || err != nil {
			t.Errorf("%s=1: on=%v err=%v", n, on, err)
		}
		t.Setenv(n, "")
		if on, err := r0WriterOn(n); on || err != nil {
			t.Errorf("%s unset: on=%v err=%v", n, on, err)
		}
	}

	// The writer with TMPDIR inside testdata/, named like committed files.
	before := r0SnapshotTree(t, td)
	t.Setenv("TMPDIR", td)
	names := []string{"r0_corpus_pins.tsv", "r0_corpus_pg17.tsv", "r0_fixture_pg17.tsv", "r0_cost_baseline.tsv", "r0_tpch_pins.tsv"}
	for i, n := range r0WriterSwitches {
		t.Setenv(n, "1")
		r0WriteArtifact(t, n, names[i%len(names)], "artifact of "+n+"\n")
		r0WriteArtifact(t, n, names[i%len(names)], "second artifact of "+n+"\n")
		t.Setenv(n, "")
	}
	after := r0SnapshotTree(t, td)
	if bad := r0SameTree(before, after); len(bad) > 0 {
		t.Errorf("the writer opened pre-existing files under testdata/ for writing:\n  %s", strings.Join(bad, "\n  "))
	}
	newDirs := map[string]bool{}
	for p := range after {
		if _, ok := before[p]; ok {
			continue
		}
		dir := filepath.Dir(p)
		if filepath.Dir(dir) != td || !strings.HasPrefix(filepath.Base(dir), "wadjet-r0-") || newDirs[dir] {
			t.Errorf("unexpected new file %s (want one file per fresh wadjet-r0-* directory)", p)
		}
		newDirs[dir] = true
	}
	for dir := range newDirs {
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
	}
	if len(newDirs) != 2*len(r0WriterSwitches) {
		t.Errorf("%d fresh artifact directories, want %d (one per write)", len(newDirs), 2*len(r0WriterSwitches))
	}
	t.Setenv("TMPDIR", tmp)

	// The R0_ARMDIFF hook.
	for _, v := range []string{bypass, "testdata/" + f, "true"} {
		func() {
			t.Setenv("R0_ARMDIFF", v)
			defer func() {
				if r := recover(); r == nil || !strings.Contains(fmt.Sprint(r), "takes no path") {
					t.Errorf("R0_ARMDIFF=%q: recovered %v, want the usage panic", v, r)
				}
			}()
			r0ArmDiffRecord(nil, "SELECT 1")
		}()
	}
	empty := t.TempDir()
	t.Setenv("TMPDIR", empty)
	t.Setenv("R0_ARMDIFF", "1")
	r0ArmDiffRecord(nil, "SELECT 1") // on, but no cell recorded: nothing is created
	if es, _ := os.ReadDir(empty); len(es) != 0 {
		t.Errorf("R0_ARMDIFF=1 with no cell recorded created %v", es)
	}
	t.Setenv("R0_ARMDIFF", "")

	if testing.Short() {
		t.Log("-short: the end-to-end writer runs are skipped; the switch and writer checks above ran")
		return
	}
	dsn := "postgres://r0-guard.invalid:1/never" // refused before any connection
	writers := []struct{ test, env, target string }{
		{"TestR0CorpusMeasurePostgres", "R0_PG_WRITE", "r0_corpus_pg17.tsv"},
		{"TestR0FixtureRowsMatchPostgres", "R0_FIXTURE_WRITE", "r0_fixture_pg17.tsv"},
		{"TestR0CorpusOwn", "R0_PINS_WRITE", "r0_corpus_pins.tsv"},
		{"TestR0CorpusOwn", "R0_REPORT", "r0_corpus_pins.tsv"},
		{"TestR0TPCHDifferential", "R0_COST_WRITE", "r0_cost_baseline.tsv"},
		{"TestR0TPCHDifferential", "R0_TPCH_REPORT", "r0_tpch_pins.tsv"},
		{"TestR0CorpusCostBaseline", "R0_COST_WRITE", "r0_cost_baseline.tsv"},
		{"TestR0BindingCensus", "R0_CENSUS", "r0_corpus_pins.tsv"},
		{"TestR0CheckerFeasibility", "R0_CHECKER_REPORT", "r0_corpus_pins.tsv"},
		{"TestR0BindingSwitchReplicaIsExecuteSQL", "R0_BINDING_REPORT", "r0_corpus_pins.tsv"},
		{"TestR0BindingSwitchGroupKeyTable", "R0_BINDING_REPORT", "r0_corpus_pins.tsv"},
	}
	base := r0SnapshotTree(t, td)
	runs := 0
	for _, w := range writers {
		for _, v := range []string{dirLink + "/../testdata/" + w.target, "testdata/" + w.target, filepath.Join(td, w.target), "true"} {
			childTmp := t.TempDir()
			cmd := osexec.Command(os.Args[0], "-test.run", "^"+w.test+"$", "-test.count=1", "-test.timeout=5m")
			cmd.Env = append(os.Environ(), w.env+"="+v, "WADJET_PG_DSN="+dsn, "TMPDIR="+childTmp)
			start := time.Now()
			out, err := cmd.CombinedOutput()
			if err == nil || !strings.Contains(string(out), "R0 writer switch "+w.env) || !strings.Contains(string(out), "takes no path") {
				t.Errorf("%s with %s=%s: exit %v, want the usage refusal; output:\n%s", w.test, w.env, v, err, out)
			}
			if es, _ := os.ReadDir(childTmp); len(es) != 0 {
				t.Errorf("%s with %s=%s wrote into its TMPDIR: %v", w.test, w.env, v, es)
			}
			if bad := r0SameTree(base, r0SnapshotTree(t, td)); len(bad) > 0 {
				t.Fatalf("%s with %s=%s CHANGED testdata:\n  %s", w.test, w.env, v, strings.Join(bad, "\n  "))
			}
			if d := time.Since(start); d > time.Minute {
				t.Errorf("%s with %s=%s took %s: the refusal should come before any measurement", w.test, w.env, v, d)
			}
			runs++
		}
	}
	t.Logf("%d writer runs refused (%d writer/test pairs x 4 values), testdata unchanged", runs, len(writers))
}
