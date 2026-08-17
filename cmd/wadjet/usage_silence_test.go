package main

import (
	"os/exec"
	"strings"
	"testing"
)

// Runtime failures must print the error alone; the full flag listing
// after "context deadline exceeded"-class errors buries the message.
// Flag mistakes keep the usage dump. Pinned by driving the built binary
// is overkill here — asserting on the cobra wiring via a subprocess of
// the test binary would be fragile; instead assert the two behaviors
// through `go run`-equivalent invocation of the command tree.
func TestRuntimeErrorsDoNotDumpUsage(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns the CLI binary")
	}
	bin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go tool unavailable")
	}
	// Runtime error: catalog table with unreachable store.
	out, _ := exec.Command(bin, "run", ".", "query", "SELECT * FROM no_such_table").CombinedOutput()
	s := string(out)
	if !strings.Contains(s, "Error:") {
		t.Fatalf("runtime error output missing Error line: %q", s)
	}
	if strings.Contains(s, "Usage:") || strings.Contains(s, "--format") {
		t.Fatalf("runtime error output dumped usage: %q", s)
	}
	// Flag error: usage must still appear.
	out, _ = exec.Command(bin, "run", ".", "query", "--no-such-flag").CombinedOutput()
	if !strings.Contains(string(out), "Usage:") {
		t.Fatalf("flag error output missing usage: %q", string(out))
	}
}
