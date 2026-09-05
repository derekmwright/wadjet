package main

import (
	"bytes"
	"errors"
	"os/exec"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/compaction"
)

// TestCompactCommandExposesRewrite pins the migration entry point.
//
// ADR-0018's DECIMAL(p > 18) compatibility note tells an operator to rewrite
// the table, and until this command existed there was no way to: compaction's
// thresholds refuse a partition holding one already-large file, which is
// precisely the shape a healthy table is in. The command is the documented
// remedy, so the flag it hangs on is part of the contract.
func TestCompactCommandExposesRewrite(t *testing.T) {
	cmd := compactCmd()

	if got := cmd.Name(); got != "compact" {
		t.Fatalf("command name = %q, want compact", got)
	}
	f := cmd.Flags().Lookup("rewrite")
	if f == nil {
		t.Fatal("compact has no --rewrite flag: ADR-0018's migration has no entry point")
	}
	if f.Value.String() != "false" {
		t.Errorf("--rewrite defaults to %q, want false — a rewrite of every file is opt-in", f.Value.String())
	}
	if !strings.Contains(strings.ToLower(f.Usage), "every file") {
		t.Errorf("--rewrite usage %q does not say it rewrites every file", f.Usage)
	}

	// It takes exactly one table: a rewrite is scoped, never "the whole
	// catalog" by omission.
	if err := cmd.Args(cmd, nil); err == nil {
		t.Error("compact accepted no table argument")
	}
	if err := cmd.Args(cmd, []string{"a", "b"}); err == nil {
		t.Error("compact accepted two table arguments")
	}
	if err := cmd.Args(cmd, []string{"orders"}); err != nil {
		t.Errorf("compact rejected a single table: %v", err)
	}
}

// TestCompactCommandIsRegistered: the command tree must actually carry it.
// Driven through the binary because main() builds the tree in a local
// variable — the same subprocess idiom as TestRuntimeErrorsDoNotDumpUsage.
func TestCompactCommandIsRegistered(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns the CLI binary")
	}
	bin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go tool unavailable")
	}
	out, _ := exec.Command(bin, "run", ".", "compact", "--help").CombinedOutput()
	if !strings.Contains(string(out), "--rewrite") {
		t.Fatalf("`wadjet compact --help` does not mention --rewrite: %s", out)
	}
	out, _ = exec.Command(bin, "run", ".", "--help").CombinedOutput()
	if !strings.Contains(string(out), "compact") {
		t.Fatalf("`wadjet --help` does not list compact: %s", out)
	}
}

// TestPrintCompactResultPrintsEverySummaryLine closes the reporting chain the
// round-1 review opened. `internal/storage/compaction` gates that a run which
// LOST a publication race produces a Summary saying so; this gates that the
// CLI prints every line of it.
//
// The two halves are separate on purpose. Summary lives beside the Result it
// describes and beside the fixture that can drive a real losing race; this
// side only has to prove that nothing is dropped in the printing, which is
// exactly how PublicationConflicts went unreported when it was added — the
// printer named its fields one at a time, so a new field was invisible by
// omission. Asserting equality against Summary is what makes that
// unrepeatable: a field the Result reports cannot be silently left out here.
func TestPrintCompactResultPrintsEverySummaryLine(t *testing.T) {
	result := &compaction.Result{
		Table:                "events",
		PublicationConflicts: 2,
		Failed: []compaction.PartitionFailure{
			{Partition: "dt=2026-09-05", Err: errors.New("boom")},
		},
	}

	var out, errOut bytes.Buffer
	printCompactResult(&out, &errOut, result)

	want := strings.Join(result.Summary(), "\n") + "\n"
	if out.String() != want {
		t.Errorf("stdout = %q, want every Summary line: %q", out.String(), want)
	}
	// The specific line the review found missing, spelled out so a future
	// Summary rewrite that drops it fails here too and not only upstream.
	if !strings.Contains(out.String(), "refused because another writer changed the same files first") {
		t.Errorf("the CLI does not report a lost publication race: %q", out.String())
	}
	if !strings.Contains(out.String(), "run again") {
		t.Errorf("a run that published nothing must tell the operator to run again: %q", out.String())
	}

	// Failures stay on stderr: one stream per audience.
	if !strings.Contains(errOut.String(), "dt=2026-09-05 FAILED") {
		t.Errorf("stderr = %q, want the partition failure", errOut.String())
	}
	if strings.Contains(out.String(), "FAILED") {
		t.Errorf("a partition failure must not reach stdout: %q", out.String())
	}
}

// TestPrintCompactResultToleratesNoResult: CompactTable can return a nil
// Result alongside an error (a bad table name), and the error itself is what
// the command reports then.
func TestPrintCompactResultToleratesNoResult(t *testing.T) {
	var out, errOut bytes.Buffer
	printCompactResult(&out, &errOut, nil)
	if out.Len() != 0 || errOut.Len() != 0 {
		t.Errorf("a nil result printed %q / %q", out.String(), errOut.String())
	}
}
