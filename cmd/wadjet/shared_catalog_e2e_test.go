package main

import (
	"os"
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

// e2eRun runs one command against a data directory under root, with the
// STORAGE flags the guide documents and NO catalog flags at all, and returns
// its combined output.
//
// The absent flags are the point (round-1 P3). The first pass pinned
// --nats-store-dir and --nats-port on every invocation, so no gate ever ran
// the command line docs/getting-started.md shows, and the default catalog
// store — `~/.wadjet/nats`, which does not vary with --data-dir — was never
// exercised. That is exactly where B3 lived: two data directories shared one
// catalog, and a `SELECT *` returned half the rows the same invocation's
// COUNT reported.
//
// HOME is redirected under root instead, so the default path is what runs
// while a developer's real ~/.wadjet is never touched. --nats-port is passed
// ONLY where a test needs the fallback dial address to be unreachable, and it
// is a separate helper for that reason.
func e2eRun(t *testing.T, bin, root string, args ...string) (string, error) {
	t.Helper()
	return e2eRunIn(t, bin, root, filepath.Join(root, "data"), args...)
}

// e2eRunIn is e2eRun against an explicit data directory, so one root can hold
// two of them.
func e2eRunIn(t *testing.T, bin, root, dataDir string, args ...string) (string, error) {
	t.Helper()
	full := append([]string{
		"--storage-type=file",
		"--data-dir=" + dataDir,
		"--bucket=wadjet",
	}, args...)
	cmd := exec.Command(bin, full...)
	home := filepath.Join(root, "home")
	if err := os.MkdirAll(home, 0o755); err != nil {
		t.Fatal(err)
	}
	cmd.Env = append(os.Environ(), "HOME="+home)
	out, err := cmd.CombinedOutput()
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

// TestTwoDataDirsDoNotShareACatalog is round-1 B3, and it is the census's
// worst cell: loud → SILENT-WRONG on the flow the guide documents.
//
// The catalog store used to default to `~/.wadjet/nats` no matter what
// `--data-dir` said, so two data directories shared one catalog while their
// files stayed apart. A table created against A was listed against B; `SELECT
// COUNT(*)` against B answered 1 from A's metadata; and `SELECT *` returned
// half the rows the same invocation's COUNT reported, because the scan drops a
// file it cannot read unless every file fails. Before the CLI reached a
// persisted catalog at all, B answered `relation "ta" does not exist` — loud.
//
// The catalog now lives under the data directory it describes, so the two
// cannot drift apart, and B is loud again. No catalog flags are given: this is
// the documented command line.
func TestTwoDataDirsDoNotShareACatalog(t *testing.T) {
	bin := e2eBin(t)
	root := t.TempDir()
	dirA := filepath.Join(root, "A")
	dirB := filepath.Join(root, "B")

	if out, err := e2eRunIn(t, bin, root, dirA, "create-table",
		"CREATE TABLE ta (id BIGINT, s VARCHAR)"); err != nil {
		t.Fatalf("create-table in A: %v\n%s", err, out)
	}
	if out, err := e2eRunIn(t, bin, root, dirA, "query", "--format=csv",
		"INSERT INTO ta VALUES (1,'x')"); err != nil {
		t.Fatalf("insert into A: %v\n%s", err, out)
	}

	// B must know nothing about A's table.
	out, err := e2eRunIn(t, bin, root, dirB, "tables")
	if err != nil {
		t.Fatalf("`tables` in B: %v\n%s", err, out)
	}
	if strings.Contains(out, "ta") {
		t.Fatalf("data dir B lists a table that belongs to data dir A: %q\n"+
			"The catalog and the data have to be one thing; sharing a machine-global "+
			"catalog across data dirs answers COUNT(*) from one and SELECT * from the "+
			"other (round-1 B3).", out)
	}
	for _, sql := range []string{"SELECT * FROM ta", "SELECT COUNT(*) FROM ta"} {
		out, err := e2eRunIn(t, bin, root, dirB, "query", "--format=csv", sql)
		if err == nil {
			t.Errorf("`%s` in data dir B ANSWERED, from data dir A's catalog: %q\n"+
				"A relation whose files are not in this store must be refused, loudly.", sql, out)
		}
		if !strings.Contains(out, "does not exist") {
			t.Errorf("`%s` in data dir B failed without saying the relation is unknown: %q", sql, out)
		}
	}

	// A still answers its own, and answers it completely: the COUNT and the
	// rows have to agree, which is the half that went silently wrong.
	out, err = e2eRunIn(t, bin, root, dirA, "query", "--format=csv", "SELECT COUNT(*) FROM ta")
	if err != nil || !strings.Contains(out, "1") {
		t.Fatalf("COUNT in A: %v\n%s", err, out)
	}
	out, err = e2eRunIn(t, bin, root, dirA, "query", "--format=csv", "SELECT * FROM ta")
	if err != nil || !strings.Contains(out, "x") {
		t.Fatalf("`SELECT *` in A returned fewer rows than its own COUNT: %v\n%s", err, out)
	}

	// And the machine-global default was never touched.
	if _, err := os.Stat(filepath.Join(root, "home", ".wadjet")); err == nil {
		t.Errorf("a file-backed deployment wrote its catalog to ~/.wadjet, which does not vary " +
			"with --data-dir")
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
	//
	// --nats-port here and nowhere else: when the lock is held the command
	// looks for the HOLDER at that address, and a port nothing listens on
	// keeps the test off whatever a developer happens to be running on 4222.
	// It does not change which catalog is chosen — only --nats-url does that.
	held := exec.Command(bin,
		"--storage-type=file", "--data-dir="+filepath.Join(root, "data"), "--bucket=wadjet",
		"--nats-port=45871", "shell")
	held.Env = append(os.Environ(), "HOME="+filepath.Join(root, "home"))
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

	out, err := e2eRunIn(t, bin, root, filepath.Join(root, "data"), "--nats-port=45871", "tables")
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
