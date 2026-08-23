package main

import (
	"os/exec"
	"strings"
	"testing"
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
