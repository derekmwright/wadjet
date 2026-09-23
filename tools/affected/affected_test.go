// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// TestAffectedFixtureDiff builds a tiny disposable module in a temp git
// repo — base (no deps) <- mid (imports base) <- top (imports mid), plus
// an unrelated package with no relation to any of them, all four with
// their own tests — commits it, changes only base/base.go, commits again,
// and asserts Affected reports exactly {base, mid, top}: everything whose
// test binary transitively reaches the change, and nothing that doesn't.
func TestAffectedFixtureDiff(t *testing.T) {
	dir := t.TempDir()
	baseline := writeFixtureModule(t, dir)
	changeAndCommit(t, dir, "base/base.go", `package base

// Value is the fixture's leaf constant. Changed for the affected test.
const Value = 2
`, "change base")

	got, err := Affected(dir, baseline)
	if err != nil {
		t.Fatalf("Affected: %v", err)
	}
	sort.Strings(got)
	want := []string{"fixture.test/base", "fixture.test/mid", "fixture.test/top"}
	if len(got) != len(want) {
		t.Fatalf("Affected = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Affected = %v, want %v", got, want)
		}
	}
}

// TestAffectedNoChanges asserts an empty diff reports nothing to test.
func TestAffectedNoChanges(t *testing.T) {
	dir := t.TempDir()
	baseline := writeFixtureModule(t, dir)

	got, err := Affected(dir, baseline)
	if err != nil {
		t.Fatalf("Affected: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Affected = %v, want none (no diff since baseline)", got)
	}
}

// TestAffectedUnrelatedChangeStaysUnrelated asserts a change to the
// unrelated package never pulls in base/mid/top.
func TestAffectedUnrelatedChangeStaysUnrelated(t *testing.T) {
	dir := t.TempDir()
	baseline := writeFixtureModule(t, dir)
	changeAndCommit(t, dir, "unrelated/unrelated.go", `package unrelated

const Value = 99
`, "change unrelated")

	got, err := Affected(dir, baseline)
	if err != nil {
		t.Fatalf("Affected: %v", err)
	}
	if len(got) != 1 || got[0] != "fixture.test/unrelated" {
		t.Fatalf("Affected = %v, want [fixture.test/unrelated]", got)
	}
}

// writeFixtureModule lays down the fixture module, commits it, and
// returns the commit hash to diff against.
func writeFixtureModule(t *testing.T, dir string) string {
	t.Helper()
	git(t, dir, "init", "-q", "-b", "main")
	git(t, dir, "config", "user.email", "fixture@example.com")
	git(t, dir, "config", "user.name", "Fixture")

	write(t, dir, "go.mod", "module fixture.test\n\ngo 1.26\n")

	write(t, dir, "base/base.go", "package base\n\nconst Value = 1\n")
	write(t, dir, "base/base_test.go", "package base\n\nimport \"testing\"\n\nfunc TestValue(t *testing.T) { _ = Value }\n")

	write(t, dir, "mid/mid.go", "package mid\n\nimport \"fixture.test/base\"\n\nfunc Sum() int { return base.Value + 1 }\n")
	write(t, dir, "mid/mid_test.go", "package mid\n\nimport \"testing\"\n\nfunc TestSum(t *testing.T) { _ = Sum() }\n")

	write(t, dir, "top/top.go", "package top\n\nimport \"fixture.test/mid\"\n\nfunc Sum() int { return mid.Sum() + 1 }\n")
	write(t, dir, "top/top_test.go", "package top\n\nimport \"testing\"\n\nfunc TestSum(t *testing.T) { _ = Sum() }\n")

	write(t, dir, "unrelated/unrelated.go", "package unrelated\n\nconst Value = 0\n")
	write(t, dir, "unrelated/unrelated_test.go", "package unrelated\n\nimport \"testing\"\n\nfunc TestValue(t *testing.T) { _ = Value }\n")

	git(t, dir, "add", "-A")
	git(t, dir, "-c", "commit.gpgsign=false", "commit", "-q", "--no-gpg-sign", "-m", "baseline")
	return gitOutput(t, dir, "rev-parse", "HEAD")
}

// changeAndCommit overwrites relPath with content and commits it as a new
// HEAD.
func changeAndCommit(t *testing.T, dir, relPath, content, msg string) {
	t.Helper()
	write(t, dir, relPath, content)
	git(t, dir, "add", "-A")
	git(t, dir, "-c", "commit.gpgsign=false", "commit", "-q", "--no-gpg-sign", "-m", msg)
}

func write(t *testing.T, dir, relPath, content string) {
	t.Helper()
	full := filepath.Join(dir, filepath.FromSlash(relPath))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(full), err)
	}
	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", full, err)
	}
}

func git(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

// gitOutput runs git and returns its trimmed stdout.
func gitOutput(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git %v: %v", args, err)
	}
	return strings.TrimSpace(string(out))
}
