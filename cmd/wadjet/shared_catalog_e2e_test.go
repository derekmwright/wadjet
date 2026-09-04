package main

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// #842 end to end, through the BUILT binary and across PROCESSES, because
// process boundaries are the whole of the defect: `tables` failed with
// "reading catalog meta: key not found" on every invocation, and `query`,
// `create-table`, `drop-table` and `shell` each opened a fresh in-memory
// catalog, so a table one invocation created was invisible to the next and to
// a running server while its parquet files piled up in the data directory.
//
// Nothing in-process can catch that. wadjet.Open with no Config.MetaKV is
// perfectly coherent inside one process — test/filestore_test.go creates a
// table, ingests and queries it, and would pass unchanged if cross-process
// persistence never worked at all.

// e2eBin builds dist/wadjet once per test binary and returns its path, or
// skips when the toolchain cannot build.
func e2eBin(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("-short: this gate builds and spawns the CLI binary")
	}
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go tool unavailable")
	}
	// Never `go build -o wadjet` at the repo root: wadjet/ is a package
	// directory and the toolchain writes the binary INTO it (CLAUDE.md).
	bin := filepath.Join(t.TempDir(), "wadjet")
	out, err := exec.Command(goBin, "build", "-o", bin, ".").CombinedOutput()
	if err != nil {
		t.Skipf("building the CLI: %v\n%s", err, out)
	}
	return bin
}

// e2eRun runs one command with the storage and catalog flags pinned to dirs
// under root, and returns its combined output.
func e2eRun(t *testing.T, bin, root string, args ...string) (string, error) {
	t.Helper()
	full := append([]string{
		"--storage-type=file",
		"--data-dir=" + filepath.Join(root, "data"),
		"--bucket=wadjet",
		"--nats-store-dir=" + filepath.Join(root, "nats"),
		// A port nothing is listening on, so the embedded fallback is what
		// runs and the test never reaches a developer's own server.
		"--nats-port=45871",
	}, args...)
	out, err := exec.Command(bin, full...).CombinedOutput()
	return string(out), err
}

// TestCLICommandsShareOnePersistedCatalog is the issue's own gate: "create-table
// then query in two processes sees the table; tables lists it."
func TestCLICommandsShareOnePersistedCatalog(t *testing.T) {
	bin := e2eBin(t)
	root := t.TempDir()

	// `tables` on an empty data directory: EMPTY, and successful. It used to
	// fail here — the first thing docs/disaster-recovery.md tells an operator
	// to run to verify a restore.
	out, err := e2eRun(t, bin, root, "tables")
	if err != nil {
		t.Fatalf("`tables` on an empty catalog failed: %v\n%s", err, out)
	}
	if strings.TrimSpace(out) != "" {
		t.Fatalf("`tables` on an empty catalog listed %q", out)
	}

	if out, err := e2eRun(t, bin, root, "create-table",
		"CREATE TABLE e2e_flows (id BIGINT, src_ip VARCHAR)"); err != nil {
		t.Fatalf("create-table: %v\n%s", err, out)
	}

	// A SECOND process must see it.
	out, err = e2eRun(t, bin, root, "tables")
	if err != nil {
		t.Fatalf("`tables` after create-table failed: %v\n%s", err, out)
	}
	if !strings.Contains(out, "e2e_flows") {
		t.Fatalf("`tables` does not list the table a previous process created: %q", out)
	}

	// A THIRD process must be able to write it, and a FOURTH to read it back.
	if out, err := e2eRun(t, bin, root, "query", "--format=csv",
		"INSERT INTO e2e_flows VALUES (1, '10.0.0.1'), (2, '10.0.0.2')"); err != nil {
		t.Fatalf("insert: %v\n%s", err, out)
	}
	out, err = e2eRun(t, bin, root, "query", "--format=csv",
		"SELECT src_ip FROM e2e_flows ORDER BY id")
	if err != nil {
		t.Fatalf("select: %v\n%s", err, out)
	}
	for _, want := range []string{"10.0.0.1", "10.0.0.2"} {
		if !strings.Contains(out, want) {
			t.Errorf("`query` in a later process does not see %q: %q", want, out)
		}
	}

	// A catalog-free query still needs no catalog and no store (#303).
	if out, err := e2eRun(t, bin, root, "query", "--format=csv", "SELECT 1 AS x"); err != nil {
		t.Errorf("catalog-free query: %v\n%s", err, out)
	}

	// And a drop is visible across processes too.
	if out, err := e2eRun(t, bin, root, "drop-table", "e2e_flows"); err != nil {
		t.Fatalf("drop-table: %v\n%s", err, out)
	}
	out, err = e2eRun(t, bin, root, "tables")
	if err != nil {
		t.Fatalf("`tables` after drop-table failed: %v\n%s", err, out)
	}
	if strings.Contains(out, "e2e_flows") {
		t.Errorf("`tables` still lists a dropped table: %q", out)
	}
}

// TestCLIRefusesASecondProcessOnTheSameCatalogStore is the boundary, attempted
// (rule 11): two commands that both fall back to an embedded catalog server
// over one store directory must not both open it.
//
// nats-server does not lock its own store directory, so before
// lockCatalogStoreDir the second process opened the same JetStream file store
// and the two wrote over each other's metadata — reachable the moment the CLI
// gained an embedded fallback. The answer is a refusal naming the way out, and
// the catalog must survive it intact.
func TestCLIRefusesASecondProcessOnTheSameCatalogStore(t *testing.T) {
	bin := e2eBin(t)
	root := t.TempDir()

	if out, err := e2eRun(t, bin, root, "create-table", "CREATE TABLE e2e_held (a BIGINT)"); err != nil {
		t.Fatalf("create-table: %v\n%s", err, out)
	}

	// A shell holds the store for as long as its stdin stays open.
	held := exec.Command(bin,
		"--storage-type=file", "--data-dir="+filepath.Join(root, "data"), "--bucket=wadjet",
		"--nats-store-dir="+filepath.Join(root, "nats"), "--nats-port=45871", "shell")
	stdin, err := held.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := held.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	held.Stderr = held.Stdout
	if err := held.Start(); err != nil {
		t.Fatalf("starting the holding shell: %v", err)
	}
	defer func() {
		stdin.Close()
		held.Wait()
	}()

	// Wait for the shell to hold the lock, deterministically rather than by
	// sleeping: it prints its banner only after openSharedDB has returned,
	// which is after lockCatalogStoreDir took the lock. The lock FILE is no
	// signal — create-table above already made it.
	banner := make(chan string, 1)
	go func() {
		buf := make([]byte, 4096)
		var seen strings.Builder
		for {
			n, rerr := stdout.Read(buf)
			seen.Write(buf[:n])
			if strings.Contains(seen.String(), "Wadjet SQL Shell") {
				banner <- seen.String()
				return
			}
			if rerr != nil {
				banner <- "shell exited before its banner: " + seen.String()
				return
			}
		}
	}()
	select {
	case line := <-banner:
		if !strings.Contains(line, "Wadjet SQL Shell") {
			t.Fatalf("the holding shell could not open the catalog: %s", line)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("the holding shell never started")
	}

	out, err := e2eRun(t, bin, root, "tables")
	if err == nil {
		t.Fatalf("a second process opened the same catalog store directory while a shell held "+
			"it; nats-server does not lock it, so both would write over each other's "+
			"metadata:\n%s", out)
	}
	if !strings.Contains(out, "held by another process") {
		t.Fatalf("the second process failed without saying why:\n%s", out)
	}
	if !strings.Contains(out, "--nats-url") {
		t.Errorf("the refusal does not name the way out (--nats-url):\n%s", out)
	}

	// The catalog survives the refusal.
	stdin.Close()
	held.Wait()
	out, err = e2eRun(t, bin, root, "tables")
	if err != nil {
		t.Fatalf("`tables` after the refusal: %v\n%s", err, out)
	}
	if !strings.Contains(out, "e2e_held") {
		t.Fatalf("the catalog lost its table across the refusal: %q", out)
	}
}

// TestGettingStartedCLITranscriptRuns runs the sequence docs/getting-started.md
// documents, in the order it documents it, so the guide cannot drift back into
// promising a flow that does not work — which is what #842 was filed for.
func TestGettingStartedCLITranscriptRuns(t *testing.T) {
	bin := e2eBin(t)
	root := t.TempDir()

	for _, step := range [][]string{
		{"create-table", "CREATE TABLE flow_logs (ts TIMESTAMP, src_ip VARCHAR, bytes_in BIGINT)"},
		{"tables"},
		{"query", "--format=table", "SELECT * FROM flow_logs LIMIT 10"},
	} {
		out, err := e2eRun(t, bin, root, step...)
		if err != nil {
			t.Fatalf("documented step `wadjet %s` failed: %v\n%s",
				strings.Join(step, " "), err, out)
		}
		if strings.Contains(out, "Error:") {
			t.Errorf("documented step `wadjet %s` printed an error:\n%s",
				strings.Join(step, " "), out)
		}
	}

	out, err := e2eRun(t, bin, root, "tables")
	if err != nil || !strings.Contains(out, "flow_logs") {
		t.Fatalf("the guide's table is not listed after its own steps: %v\n%s", err, out)
	}
}
