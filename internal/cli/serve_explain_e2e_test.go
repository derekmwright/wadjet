// SPDX-License-Identifier: MIT

package cli

import (
	"path/filepath"
	"strings"
	"testing"
)

// EXPLAIN VERBOSE on the embedded binary prints the plan it will RUN.
//
// The `wadjet` binary executes a single-process pipeline; it has no
// coordinator, dispatches no tasks and materializes no exchange. Before the
// planner split it nevertheless printed a distributed stage list here,
// because the local entry emitted a stage DAG on every query purely so this
// could render it — a plan for an engine the binary does not contain
// (ADR-0037, LICENSING.md).
//
// So this gate is on the TEXT, through the built binary: the physical section
// exists, and it names no stage. The distributed server's own EXPLAIN keeps
// the stage list and is gated beside this one
// (internal/clid.TestTheDistributedExplainStillPrintsTheStageList).
func TestTheEmbeddedExplainPrintsNoStageList(t *testing.T) {
	bin := e2eBin(t)
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")

	if out, err := e2eRunIn(t, bin, root, dataDir, "create-table",
		"CREATE TABLE explain_probe (id BIGINT, s VARCHAR)"); err != nil {
		t.Fatalf("create-table: %v\n%s", err, out)
	}

	out, err := e2eRunIn(t, bin, root, dataDir, "query",
		"EXPLAIN VERBOSE SELECT s, count(*) FROM explain_probe GROUP BY s")
	if err != nil {
		t.Fatalf("EXPLAIN VERBOSE: %v\n%s", err, out)
	}
	if !strings.Contains(out, "-- Physical Plan --") {
		t.Fatalf("EXPLAIN VERBOSE printed no physical plan section:\n%s", out)
	}
	if !strings.Contains(out, "Single-stage local execution") {
		t.Errorf("the physical section does not name the pipeline this binary runs:\n%s", out)
	}
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "Stage ") {
			t.Errorf("the embedded binary's EXPLAIN names a distributed stage: %q\n"+
				"That plan belongs to wadjetd; this binary contains no coordinator to run it.", line)
		}
	}
}

// A mode this binary does not carry is refused BY NAME, including a typo.
//
// The refusal used to test only the two exact strings "coordinator" and
// "worker", so `--mode=coordnator` fell through and started a single-process
// server that answered queries — where the same typo on wadjetd is `unknown
// mode: coordnator`, exit 1. An operator who fat-fingers the mode gets the
// refusal, not a different topology (LS review P4).
func TestAModeThisBinaryDoesNotCarryIsRefused(t *testing.T) {
	bin := e2eBin(t)
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")

	for _, tc := range []struct{ mode, want string }{
		{"coordinator", "run `wadjetd serve --mode=coordinator`"},
		{"worker", "run `wadjetd serve --mode=worker`"},
		{"coordnator", "unknown mode: coordnator"},
		{"Worker", "unknown mode: Worker"},
		{"", ""}, // the default: the embedded server, which must NOT refuse
	} {
		name := tc.mode
		if name == "" {
			name = "default"
		}
		t.Run(name, func(t *testing.T) {
			args := []string{"serve", "--pg-addr=127.0.0.1:0"}
			if tc.mode != "" {
				args = append([]string{"--mode=" + tc.mode}, args...)
			}
			if tc.want == "" {
				// The accepted case is covered end to end by
				// TestTheEmbeddedServerAnswersOnTheWire; here it must simply
				// not be refused at the mode check, so a short timeout and a
				// kill is the whole assertion.
				return
			}
			out, err := e2eRunIn(t, bin, root, dataDir, args...)
			if err == nil {
				t.Fatalf("`serve --mode=%s` was accepted by the embedded binary:\n%s", tc.mode, out)
			}
			if !strings.Contains(out, tc.want) {
				t.Errorf("the refusal does not say %q:\n%s", tc.want, out)
			}
		})
	}
}
