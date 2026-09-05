package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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
// follow-up named.
//
// WHAT THIS GATE ASSERTS, and why it is not the log line E2 saw. E2 recorded
// `Filestore ... will rebuild` on the next open. That does NOT reproduce:
// measured at debug level on both arms, an unhandled SIGTERM leaves a store
// JetStream restores normally (`Starting restore for stream ... Restored 3
// messages`), and the CLI logs at WARN, so neither line would reach a client
// anyway. The observable, level-independent property is the one the fix is:
// the release RUNS. A process that runs it exits 128+signal; one that does not
// is killed BY the signal and has no exit status at all. That difference is
// what fails on revert.

// shellSession is a running `wadjet shell` with a pipe on its stdin and a
// buffer collecting everything it prints.
type shellSession struct {
	t   *testing.T
	cmd *exec.Cmd
	in  io.WriteCloser
	mu  sync.Mutex
	out bytes.Buffer
}

func startShell(t *testing.T, bin, root, dataDir string) *shellSession {
	t.Helper()
	cmd := exec.Command(bin,
		"--storage-type=file", "--data-dir="+dataDir, "--bucket=wadjet", "shell")
	home := filepath.Join(root, "home")
	if err := os.MkdirAll(home, 0o755); err != nil {
		t.Fatal(err)
	}
	cmd.Env = append(os.Environ(), "HOME="+home)
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	s := &shellSession{t: t, cmd: cmd, in: in}
	cmd.Stdout, cmd.Stderr = &lockedWriter{s: s}, &lockedWriter{s: s}
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting the shell: %v", err)
	}
	return s
}

type lockedWriter struct{ s *shellSession }

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	return w.s.out.Write(p)
}

func (s *shellSession) text() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.out.String()
}

// send writes one statement and waits for `want` to appear in the shell's
// output. That is the readiness signal: the first version of this gate slept
// a fixed 1500 ms after a CREATE TABLE and called it settled, which is a
// timing dependency inside a gate (round-1 P7).
func (s *shellSession) send(sql, want string, deadline time.Duration) {
	s.t.Helper()
	before := len(s.text())
	fmt.Fprintln(s.in, sql)
	until := time.Now().Add(deadline)
	for time.Now().Before(until) {
		if strings.Contains(s.text()[before:], want) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	s.t.Fatalf("the shell did not answer %q with %q within %v:\n%s",
		sql, want, deadline, s.text())
}

// signalAndWait sends sig and returns the process state.
func (s *shellSession) signalAndWait(sig syscall.Signal) *os.ProcessState {
	s.t.Helper()
	if err := s.cmd.Process.Signal(sig); err != nil {
		s.t.Fatalf("signalling the shell: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- s.cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		s.cmd.Process.Kill()
		s.t.Fatalf("the shell did not exit within 60s of %v:\n%s", sig, s.text())
	}
	s.in.Close()
	return s.cmd.ProcessState
}

// cpuTicks is the process's accumulated user+system time from /proc, in clock
// ticks. It is how this gate knows a QUERY is running rather than guessing:
// a shell waiting at its prompt burns none.
func cpuTicks(t *testing.T, pid int) int64 {
	t.Helper()
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return -1
	}
	// Fields after the (comm) parenthesis; utime is field 14, stime 15 (1-based).
	i := strings.LastIndex(string(raw), ") ")
	if i < 0 {
		return -1
	}
	f := strings.Fields(string(raw)[i+2:])
	if len(f) < 13 {
		return -1
	}
	u, err1 := strconv.ParseInt(f[11], 10, 64)
	sy, err2 := strconv.ParseInt(f[12], 10, 64)
	if err1 != nil || err2 != nil {
		return -1
	}
	return u + sy
}

// assertCleanShutdown is the whole disposition, for one signal.
func assertCleanShutdown(t *testing.T, st *os.ProcessState, sig syscall.Signal) {
	t.Helper()
	if !st.Exited() {
		t.Fatalf("the shell was killed BY %v (%s) rather than shutting down: "+
			"the catalog's release never ran", sig, st.String())
	}
	if want := 128 + int(sig); st.ExitCode() != want {
		t.Errorf("the shell exited %d after %v, want %d (128+signal)", st.ExitCode(), sig, want)
	}
}

// TestASignalledCLIShutsItsCatalogDown is E2-S's gate: the signal arrives while
// the shell sits at its prompt with a settled catalog.
func TestASignalledCLIShutsItsCatalogDown(t *testing.T) {
	bin := e2eBin(t)

	for _, sig := range []syscall.Signal{syscall.SIGTERM, syscall.SIGINT} {
		t.Run(sig.String(), func(t *testing.T) {
			root := t.TempDir()
			dataDir := filepath.Join(root, "data")
			sh := startShell(t, bin, root, dataDir)
			sh.send("CREATE TABLE sigterm_probe (id BIGINT);", "created", 60*time.Second)
			assertCleanShutdown(t, sh.signalAndWait(sig), sig)

			// The session's work is there, on a store the next process opens
			// without complaint. A clean shutdown that lost the catalog would
			// satisfy the status check and nothing else.
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

// TestASignalledCLIShutsDownMidStatement is the case the handler's own
// justification names — "the ^C that arrives while a QUERY is running", when
// the terminal is back in cooked mode and the process today dies where it
// stands. The prompt case above cannot show it: a shell waiting on stdin is
// not executing anything (round-1 P7).
//
// The query is a scan of a large JSON file through read_json, which needs no
// catalog and whose cost is set by the fixture rather than by the machine.
// "Is it running" is measured, not assumed: the process's CPU time has to
// ADVANCE after the statement is sent, and the gate fails with an actionable
// message if it does not, rather than signalling an idle shell and quietly
// testing the case above a second time.
func TestASignalledCLIShutsDownMidStatement(t *testing.T) {
	bin := e2eBin(t)
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")

	// Two million rows, counted DISTINCT. Sized from a measurement, not a
	// guess: a plain COUNT(*) over 400k rows burned 9 ticks of CPU on this
	// machine — under the threshold below, so the gate would have signalled an
	// idle shell and silently re-tested the prompt case. This shape is an
	// order of magnitude past it and its cost comes from the fixture rather
	// than from the machine.
	const probeRows = 2000000
	src := filepath.Join(root, "big.json")
	var b bytes.Buffer
	for i := 0; i < probeRows; i++ {
		fmt.Fprintf(&b, "{\"a\":%d,\"s\":\"row-%d\"}\n", i, i)
	}
	if err := os.WriteFile(src, b.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}

	sh := startShell(t, bin, root, dataDir)
	sh.send("CREATE TABLE midprobe (id BIGINT);", "created", 60*time.Second)

	pid := sh.cmd.Process.Pid
	idle := cpuTicks(t, pid)
	if idle < 0 {
		t.Skip("/proc is unavailable; this gate measures the process's CPU time")
	}
	fmt.Fprintf(sh.in, "SELECT COUNT(DISTINCT s) AS c FROM read_json('%s') WHERE a > 0;\n", src)

	// Wait for the process to be BURNING CPU: that is the statement running.
	const wantTicks = 15 // ~150 ms of CPU at the usual 100 Hz
	deadline := time.Now().Add(60 * time.Second)
	running := false
	var maxDelta int64
	for time.Now().Before(deadline) {
		if d := cpuTicks(t, pid) - idle; d > maxDelta {
			maxDelta = d
		}
		if maxDelta >= wantTicks {
			running = true
			break
		}
		if strings.Contains(sh.text(), "(0 rows)") ||
			strings.Contains(sh.text(), strconv.Itoa(probeRows-1)) {
			break // it finished; the fixture is too small for this machine
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !running {
		t.Fatalf("the probe query accumulated %d ticks of CPU, want %d, so this run did not "+
			"signal mid-statement. Make the fixture bigger rather than accepting the "+
			"weaker case.\n%s", maxDelta, wantTicks, sh.text())
	}

	assertCleanShutdown(t, sh.signalAndWait(syscall.SIGINT), syscall.SIGINT)

	out, err := e2eRunIn(t, bin, root, dataDir, "tables")
	if err != nil {
		t.Fatalf("reopening the data dir after a mid-statement signal: %v\n%s", err, out)
	}
	if !strings.Contains(out, "midprobe") {
		t.Errorf("the table created before the signalled statement is gone:\n%s", out)
	}
}

// TestASignalledCLIReleasesItsCatalogLock is the release's other half: the
// handler runs the WHOLE release, so the next command takes the lock without
// waiting for anything.
//
// It is a CONTROL rather than a gate for the handler: the kernel drops a dead
// process's flock too, so this passes with the handler removed. It is here to
// catch a release that runs and leaves the lock file claimed.
func TestASignalledCLIReleasesItsCatalogLock(t *testing.T) {
	bin := e2eBin(t)
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	sh := startShell(t, bin, root, dataDir)
	sh.send("CREATE TABLE sigterm_probe (id BIGINT);", "created", 60*time.Second)
	sh.signalAndWait(syscall.SIGTERM)

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
