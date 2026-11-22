// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"fmt"
	"os"
	osexec "os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestR0WritersRefuseCommittedTargets: no R0 writer can write a committed
// oracle, pin or baseline file, however the path is spelled (the second
// independent review of R0, section B: R0_FIXTURE_WRITE pointed at the
// committed oracle made a fixture fault pass). First the guard itself
// (r0ArtifactTarget) over relative, ./, absolute, ../, dot-dot, doubled
// separator, symbolic-link (file, directory, dangling), hard-link, new-file
// and other-package spellings, and the artifact targets it must still accept;
// then every writer switch end to end, in a child process running the test
// that reads it, pointed at a committed file by its relative, ./ and absolute
// spellings: each must fail with the refusal and leave the file byte-for-byte
// unchanged.
func TestR0WritersRefuseCommittedTargets(t *testing.T) {
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	tmp := t.TempDir()
	committed := []string{"r0_corpus_pg17.tsv", "r0_corpus_pins.tsv", "r0_fixture_pg17.tsv", "r0_cost_baseline.tsv", "r0_tpch_pins.tsv", "r0_corpus_cells.tsv"}
	var refuse []string
	for _, f := range committed {
		refuse = append(refuse, "testdata/"+f, "./testdata/"+f, filepath.Join(cwd, "testdata", f),
			"../coordinator/testdata/"+f, "testdata/../testdata/"+f, "testdata//"+f)
		link := filepath.Join(tmp, "link-"+f)
		if err := os.Symlink(filepath.Join(cwd, "testdata", f), link); err != nil {
			t.Fatal(err)
		}
		refuse = append(refuse, link)
		hard := filepath.Join(tmp, "hard-"+f)
		if err := os.Link(filepath.Join(cwd, "testdata", f), hard); err == nil {
			refuse = append(refuse, hard)
		} else {
			t.Logf("hard link into %s not possible here (%v): that spelling is not exercised", tmp, err)
		}
	}
	dirLink := filepath.Join(tmp, "td")
	if err := os.Symlink(filepath.Join(cwd, "testdata"), dirLink); err != nil {
		t.Fatal(err)
	}
	dangling := filepath.Join(tmp, "dangling")
	if err := os.Symlink(filepath.Join(tmp, "nowhere", "x.tsv"), dangling); err != nil {
		t.Fatal(err)
	}
	refuse = append(refuse, filepath.Join(dirLink, "r0_corpus_pins.tsv"), filepath.Join(dirLink, "new.tsv"),
		"testdata/new_artifact.tsv", "testdata/sub/dir/new.tsv", "dagplan/testdata/tpch_stage_dump.golden",
		"../../internal/worker/testdata/x.tsv", dangling, "")
	for _, p := range refuse {
		if err := r0ArtifactTarget(p); err == nil || !strings.Contains(err.Error(), "R0 writer target") {
			t.Errorf("r0ArtifactTarget(%q) = %v, want a refusal", p, err)
		}
	}
	for _, p := range []string{filepath.Join(tmp, "out.tsv"), filepath.Join(tmp, "new", "sub", "out.tsv"), filepath.Join(tmp, "testdata-like.tsv")} {
		if err := r0ArtifactTarget(p); err != nil {
			t.Errorf("r0ArtifactTarget(%q) refused an artifact target: %v", p, err)
		}
	}

	// The R0_ARMDIFF hook has no *testing.T: it panics on a refused target.
	func() {
		t.Setenv("R0_ARMDIFF", "testdata/r0_corpus_pins.tsv")
		defer func() {
			if r := recover(); r == nil || !strings.Contains(fmt.Sprint(r), "R0 writer target") {
				t.Errorf("R0_ARMDIFF at a committed file: recovered %v, want a refusal panic", r)
			}
		}()
		r0ArmDiffRecord(nil, "SELECT 1")
	}()

	if testing.Short() {
		t.Log("-short: the end-to-end writer runs are skipped; the guard above ran")
		return
	}
	dsn := "postgres://r0-guard.invalid:1/never" // the guard refuses before any connection
	writers := []struct{ test, env, target, extra string }{
		{"TestR0CorpusMeasurePostgres", "R0_PG_WRITE", "r0_corpus_pg17.tsv", "WADJET_PG_DSN=" + dsn},
		{"TestR0FixtureRowsMatchPostgres", "R0_FIXTURE_WRITE", "r0_fixture_pg17.tsv", "WADJET_PG_DSN=" + dsn},
		{"TestR0CorpusOwn", "R0_PINS_WRITE", "r0_corpus_pins.tsv", ""},
		{"TestR0CorpusOwn", "R0_REPORT", "r0_corpus_pins.tsv", ""},
		{"TestR0TPCHDifferential", "R0_COST_WRITE", "r0_cost_baseline.tsv", ""},
		{"TestR0TPCHDifferential", "R0_TPCH_REPORT", "r0_tpch_pins.tsv", ""},
		{"TestR0CorpusCostBaseline", "R0_COST_WRITE", "r0_cost_baseline.tsv", ""},
		{"TestR0BindingCensus", "R0_CENSUS", "r0_corpus_pins.tsv", ""},
		{"TestR0CheckerFeasibility", "R0_CHECKER_REPORT", "r0_corpus_pins.tsv", ""},
		{"TestR0BindingSwitchReplicaIsExecuteSQL", "R0_BINDING_REPORT", "r0_corpus_pins.tsv", ""},
		{"TestR0BindingSwitchGroupKeyTable", "R0_BINDING_REPORT", "r0_corpus_pins.tsv", ""},
	}
	runs := 0
	for _, w := range writers {
		for _, p := range []string{"testdata/" + w.target, "./testdata/" + w.target, filepath.Join(cwd, "testdata", w.target)} {
			before, err := os.ReadFile(filepath.Join(cwd, "testdata", w.target))
			if err != nil {
				t.Fatal(err)
			}
			cmd := osexec.Command(os.Args[0], "-test.run", "^"+w.test+"$", "-test.count=1", "-test.timeout=5m")
			cmd.Env = append(os.Environ(), w.env+"="+p)
			if w.extra != "" {
				cmd.Env = append(cmd.Env, w.extra)
			}
			out, err := cmd.CombinedOutput()
			if err == nil || !strings.Contains(string(out), "R0 writer target") {
				t.Errorf("%s with %s=%s: exit %v, want a refusal; output:\n%s", w.test, w.env, p, err, out)
			}
			after, err := os.ReadFile(filepath.Join(cwd, "testdata", w.target))
			if err != nil {
				t.Fatal(err)
			}
			if string(after) != string(before) {
				t.Fatalf("%s with %s=%s CHANGED testdata/%s", w.test, w.env, p, w.target)
			}
			runs++
		}
	}
	t.Logf("%d writer runs refused (%d switches x 3 spellings)", runs, len(writers))
}
