package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// A CLI command holding the shared catalog runs an embedded NATS server over a
// JetStream FILE store, and a process killed outright skips every `defer`: the
// connection is not closed, the server is not shut down and the store lock is
// left to the kernel. `shell` trapped nothing at all, so a SIGTERM — what a
// process manager, a container runtime and Ctrl-C-during-a-query all send —
// ended the process where it stood (arc E2's follow-up 8).
//
// The handler lives in sharedCatalogKV, the ONE place the store is opened, so
// every command that holds it carries it rather than the one command the
// follow-up happened to name.
//
// WHAT THIS GATE ASSERTS, and why it is not the log line E2 saw. E2 recorded
// `Filestore ... will rebuild` on the next open. That does NOT reproduce here:
// measured at debug level on both arms, an unhandled SIGTERM leaves a store
// JetStream restores normally (`Starting restore for stream ... Restored 3
// messages`), and the CLI logs at WARN, so neither line would reach a client
// anyway. The observable, level-independent property is the one the fix is:
// the release RUNS. A process that runs it exits 128+signal; one that does not
// is killed BY the signal and has no exit status at all. That difference is
// what fails on revert.

func shellKilledBy(t *testing.T, bin, root, dataDir string, sig syscall.Signal) *os.ProcessState {
	t.Helper()
	cmd := exec.Command(bin,
		"--storage-type=file", "--data-dir="+dataDir, "--bucket=wadjet", "shell")
	home := filepath.Join(root, "home")
	if err := os.MkdirAll(home, 0o755); err != nil {
		t.Fatal(err)
	}
	cmd.Env = append(os.Environ(), "HOME="+home)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting the shell: %v", err)
	}

	// A statement first, so the catalog is genuinely open and written to
	// before the signal arrives: a store with nothing in it has nothing to
	// leave behind.
	fmt.Fprintln(stdin, "CREATE TABLE sigterm_probe (id BIGINT);")
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(dataDir, "_catalog", "jetstream")); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(1500 * time.Millisecond)

	if err := cmd.Process.Signal(sig); err != nil {
		t.Fatalf("signalling the shell: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		cmd.Process.Kill()
		t.Fatalf("the shell did not exit within 30s of %v:\n%s", sig, out.String())
	}
	stdin.Close()
	return cmd.ProcessState
}

// TestASignalledCLIShutsItsCatalogDown is E2-S's gate.
func TestASignalledCLIShutsItsCatalogDown(t *testing.T) {
	bin := e2eBin(t)

	for _, sig := range []syscall.Signal{syscall.SIGTERM, syscall.SIGINT} {
		t.Run(sig.String(), func(t *testing.T) {
			root := t.TempDir()
			dataDir := filepath.Join(root, "data")
			st := shellKilledBy(t, bin, root, dataDir, sig)

			// The handler ran: the process CHOSE its exit status instead of
			// being killed by the signal. Without it, Exited() is false and
			// ExitCode() is -1 — that is the revert's signature.
			if !st.Exited() {
				t.Fatalf("the shell was killed BY %v (%s) rather than shutting down: "+
					"the catalog's release never ran", sig, st.String())
			}
			if want := 128 + int(sig); st.ExitCode() != want {
				t.Errorf("the shell exited %d after %v, want %d (128+signal)",
					st.ExitCode(), sig, want)
			}

			// And the session's work is there, on a store the next process
			// opens without complaint. A clean shutdown that lost the catalog
			// would satisfy the status check and nothing else.
			out, err := e2eRunIn(t, bin, root, dataDir, "tables")
			if err != nil {
				t.Fatalf("reopening the data dir after %v: %v\n%s", sig, err, out)
			}
			if !strings.Contains(out, "sigterm_probe") {
				t.Errorf("the table the signalled session created is gone:\n%s", out)
			}
			for _, marker := range []string{"will rebuild", "corrupt"} {
				if strings.Contains(out, marker) {
					t.Errorf("reopening the catalog after %v logged %q:\n%s", sig, marker, out)
				}
			}
		})
	}
}

// TestASignalledCLIReleasesItsCatalogLock is the release's other half: the
// handler runs the WHOLE release, so the next command takes the lock without
// waiting for anything.
func TestASignalledCLIReleasesItsCatalogLock(t *testing.T) {
	bin := e2eBin(t)
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	shellKilledBy(t, bin, root, dataDir, syscall.SIGTERM)

	out, err := e2eRunIn(t, bin, root, dataDir,
		"create-table", "CREATE TABLE after_sig (id BIGINT)")
	if err != nil {
		t.Fatalf("the catalog is still locked after the holder was signalled: %v\n%s", err, out)
	}
	list, err := e2eRunIn(t, bin, root, dataDir, "tables")
	if err != nil {
		t.Fatalf("tables: %v\n%s", err, list)
	}
	for _, want := range []string{"sigterm_probe", "after_sig"} {
		if !strings.Contains(list, want) {
			t.Errorf("table %q missing after the signalled session:\n%s", want, list)
		}
	}
}
