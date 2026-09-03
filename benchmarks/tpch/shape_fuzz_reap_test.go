package tpch

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// When the oracle REJECTS a generated statement, the child's stderr is the
// whole diagnostic — there is no other record of why that shape could not be
// compared. os/exec copies a non-pipe Stderr on a goroutine that only
// cmd.Wait() joins, so reading the buffer before the reap is both a data race
// (reported under -race against bytes.(*Buffer).grow) and a truncated message:
// the reap sat in a bare defer, which runs after the return expression has
// already called stderr.String() (#701).
//
// This runs everywhere, without the DuckDB CLI: the harness's oracle binary is
// pointed at a stub that writes a long message to stderr and no CSV to stdout,
// which is exactly the rejected-statement branch. The stub's last line is the
// assertion — before the fix the message came back empty or cut off, because
// the copier had not finished (usually had not started) when the child's exit
// closed stdout and released the CSV reader.
//
// Reverting the reap into a bare defer fails this on essentially every run,
// and reports the race under -race as well.
func TestFuzzDuckDBReapsTheChildBeforeReadingItsStderr(t *testing.T) {
	if _, err := os.Stat("/bin/sh"); err != nil {
		t.Skip("needs /bin/sh for the stub oracle")
	}
	// The stub closes stdout BEFORE writing its message, so the parent's CSV
	// reader reaches EOF — the trigger for the rejection branch — while the
	// message is still in flight. DuckDB reaches the same state at exit, which
	// makes the window a scheduler coin-flip; closing stdout early makes the
	// ORDERING the test is about observable instead of lucky. The property is
	// the same either way: the harness must not read a buffer it has not
	// joined. (The -race arm over the real binary is the issue's own repro.)
	const lines = 2000
	stub := filepath.Join(t.TempDir(), "stub-oracle")
	script := "#!/bin/sh\n" +
		// Drain stdin: fuzzDuckDB writes the whole script to the child, and a
		// child that exits without reading it gives the parent an EPIPE
		// instead of the rejection this is about.
		"cat > /dev/null\n" +
		"exec 1>&-\n" +
		fmt.Sprintf("i=0\nwhile [ $i -lt %d ]; do printf 'stub oracle rejection line %%d\\n' $i >&2; i=$((i+1)); done\n", lines) +
		"exit 1\n"
	if err := os.WriteFile(stub, []byte(script), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}

	// The stub is passed, not assigned to a package var: a global the gate
	// reassigned would be one a future parallel test could race.
	//
	// Replicated: a single agreeing run of an ordering gate proves nothing.
	for i := 0; i < 20; i++ {
		rows, cols, err := fuzzDuckDBWithBin(stub, "", "SELECT 1")
		if err == nil {
			t.Fatalf("attempt %d: stub oracle returned rows=%d cols=%v and no error; the rejection branch was not reached", i, len(rows), cols)
		}
		first := fmt.Sprintf("stub oracle rejection line %d", 0)
		last := fmt.Sprintf("stub oracle rejection line %d", lines-1)
		if !strings.Contains(err.Error(), first) || !strings.Contains(err.Error(), last) {
			t.Fatalf("attempt %d: the rejection message is not the child's whole stderr — "+
				"the buffer was read before cmd.Wait() joined the copier.\n  got: %.200q…", i, err.Error())
		}
	}
}
