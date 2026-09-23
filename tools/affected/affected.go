// SPDX-License-Identifier: MIT

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

// listPkg is the subset of `go list -json` fields Affected reads.
type listPkg struct {
	ImportPath string
	Deps       []string
}

// Affected returns, sorted, every package under dir whose test binary
// transitively depends on a package that changed between base and HEAD
// (`git diff --name-only base...HEAD`), plus every changed package that
// has tests of its own — the set `go test -p 2` should run instead of the
// full suite for this change.
//
// A package with no test files never appears: there is nothing for
// `go test` to run there, so it would be noise in a list meant to be fed
// straight into one.
func Affected(dir, base string) ([]string, error) {
	changedFiles, err := changedGoFiles(dir, base)
	if err != nil {
		return nil, err
	}
	if len(changedFiles) == 0 {
		return nil, nil
	}

	changedPkgs, err := importPathsOf(dir, changedDirs(changedFiles))
	if err != nil {
		return nil, err
	}
	if len(changedPkgs) == 0 {
		return nil, nil
	}

	testPkgs, err := testBinaryDeps(dir)
	if err != nil {
		return nil, err
	}

	affected := map[string]bool{}
	for root, deps := range testPkgs {
		if changedPkgs[root] {
			affected[root] = true
			continue
		}
		for _, d := range deps {
			if changedPkgs[d] {
				affected[root] = true
				break
			}
		}
	}

	out := make([]string, 0, len(affected))
	for p := range affected {
		out = append(out, p)
	}
	sort.Strings(out)
	return out, nil
}

// changedGoFiles returns every .go file `git diff --name-only` reports
// between base and HEAD, relative to dir.
func changedGoFiles(dir, base string) ([]string, error) {
	cmd := exec.Command("git", "diff", "--name-only", base+"...HEAD")
	cmd.Dir = dir
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git diff --name-only %s...HEAD: %w", base, err)
	}
	var files []string
	sc := bufio.NewScanner(strings.NewReader(string(out)))
	for sc.Scan() {
		if f := strings.TrimSpace(sc.Text()); strings.HasSuffix(f, ".go") {
			files = append(files, f)
		}
	}
	return files, nil
}

// changedDirs returns the unique directories containing the given files,
// as the "./"-relative patterns `go list` accepts.
func changedDirs(files []string) []string {
	seen := map[string]bool{}
	var dirs []string
	for _, f := range files {
		d := path.Dir(filepath.ToSlash(f))
		pattern := "./" + d
		if d == "." {
			pattern = "."
		}
		if !seen[pattern] {
			seen[pattern] = true
			dirs = append(dirs, pattern)
		}
	}
	return dirs
}

// importPathsOf resolves each of the given directory patterns to its
// package import path via `go list`, skipping any that is not a
// buildable package (a directory whose only changed file was deleted, or
// isn't Go source).
func importPathsOf(dir string, patterns []string) (map[string]bool, error) {
	result := map[string]bool{}
	if len(patterns) == 0 {
		return result, nil
	}
	args := append([]string{"list", "-e", "-f", "{{if .Error}}{{else}}{{.ImportPath}}{{end}}"}, patterns...)
	cmd := exec.Command("go", args...)
	cmd.Dir = dir
	out, _ := cmd.Output() // -e: keep going past unresolvable patterns; their errors land on stderr, not here
	sc := bufio.NewScanner(strings.NewReader(string(out)))
	for sc.Scan() {
		if p := strings.TrimSpace(sc.Text()); p != "" {
			result[p] = true
		}
	}
	return result, nil
}

// testBinaryDeps returns, for every package under dir that has a test
// binary, its import path mapped to every package that binary
// transitively depends on. `go list -test -json ./...` emits, per
// package, a synthetic "<import path>.test" entry whose Deps field is
// exactly that transitive closure (regular imports and test-only imports
// alike) — the per-package equivalent of `go list -deps -test <pkg>`,
// computed for the whole module in one call instead of one per package.
func testBinaryDeps(dir string) (map[string][]string, error) {
	cmd := exec.Command("go", "list", "-test", "-json", "./...")
	cmd.Dir = dir
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}

	result := map[string][]string{}
	dec := json.NewDecoder(bufio.NewReader(stdout))
	for {
		var p listPkg
		if err := dec.Decode(&p); err != nil {
			break
		}
		if strings.HasSuffix(p.ImportPath, ".test") {
			result[strings.TrimSuffix(p.ImportPath, ".test")] = p.Deps
		}
	}
	// go list exits non-zero if some package elsewhere in the module fails
	// to build; that's a pre-existing break this tool doesn't own, and the
	// packages it did manage to list are still a valid partial answer.
	_ = cmd.Wait()
	return result, nil
}
