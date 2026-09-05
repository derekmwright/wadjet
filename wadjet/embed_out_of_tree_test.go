package wadjet_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// #805: `go get github.com/derekmwright/wadjet/wadjet` succeeded and then the
// first line of docs/getting-started.md's embedded program did not compile.
// Config.Store is objstore.Store, CreateTable takes parquet.Schema and
// NewIngester takes ingest.Config — all three under internal/, which Go
// forbids another module from importing. The engine was embeddable only from
// inside its own repository, and no test could notice, because every test in
// this repository IS inside it.
//
// The gate is therefore a BUILD of a different module. test/embed/ has its own
// go.mod (module embedcheck, with a replace pointing at this checkout), so the
// internal rule applies to it exactly as it applies to a user's program, and
// it contains the guide's program. If the public surface stops being enough,
// this stops compiling.
//
// It is a build and a RUN: a surface that compiles and then fails at the first
// call would be a worse promise than none.

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd() // .../wadjet
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Dir(wd)
}

func TestTheGuidesProgramBuildsAndRunsOutOfTree(t *testing.T) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skipf("no go toolchain on PATH: %v", err)
	}
	dir := filepath.Join(repoRoot(t), "test", "embed")
	if _, err := os.Stat(filepath.Join(dir, "go.mod")); err != nil {
		t.Fatalf("test/embed must be its own module — that is what makes the "+
			"internal rule apply to it: %v", err)
	}

	bin := filepath.Join(t.TempDir(), "embedcheck")
	build := exec.Command(goBin, "build", "-o", bin, ".")
	build.Dir = dir
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("the guide's program does not build from a module that is not this one.\n"+
			"Every type it names has to be reachable from package wadjet without importing\n"+
			"github.com/derekmwright/wadjet/internal/... (#805).\n\n%s\n%v", out, err)
	}

	run := exec.Command(bin)
	out, err := run.CombinedOutput()
	if err != nil {
		t.Fatalf("the guide's program builds out of tree and then fails to run:\n%s\n%v", out, err)
	}
	// The row it ingested, read back through the public API.
	for _, want := range []string{"10.0.1.50", "12.34", "tables: [flow_logs]"} {
		if !strings.Contains(string(out), want) {
			t.Errorf("the out-of-tree program's output does not contain %q:\n%s", want, out)
		}
	}
}

// TestTheOutOfTreeModuleImportsNothingInternal states the property directly.
//
// The build above already enforces it — Go refuses the import — but an import
// added in a file the build happens not to reach would slip through, and the
// point of test/embed is that it is a user's-eye view. This says so in one
// place a reader can check.
func TestTheOutOfTreeModuleImportsNothingInternal(t *testing.T) {
	dir := filepath.Join(repoRoot(t), "test", "embed")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	saw := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") {
			continue
		}
		saw++
		src, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(src), `"github.com/derekmwright/wadjet/internal/`) {
			t.Errorf("%s imports an internal package; test/embed stands in for a user's "+
				"module, which cannot", e.Name())
		}
	}
	if saw == 0 {
		t.Fatal("test/embed has no Go files — the out-of-tree proof is gone")
	}
}
