// SPDX-License-Identifier: MIT

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

const modulePath = "github.com/derekmwright/wadjet"

// Violation is one finding, named by the file or package it is about.
type Violation struct {
	Where string
	What  string
}

func (v Violation) String() string { return v.Where + ": " + v.What }

// pkg is one package of this module as `go list` reports it.
type pkg struct {
	importPath string
	dir        string // relative to the module root
	imports    []string
	testImps   []string
}

// loadPackages reads the module's own package graph — direct imports for the
// boundary, test imports for the crossings — in one `go list` invocation.
func loadPackages(root string) ([]pkg, error) {
	cmd := exec.Command("go", "list",
		"-f", "{{.ImportPath}}\t{{join .Imports \",\"}}\t{{join .TestImports \",\"}}\t{{join .XTestImports \",\"}}",
		"./...")
	cmd.Dir = root
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("go list: %w: %s", err, stderr.String())
	}
	var pkgs []pkg
	sc := bufio.NewScanner(bytes.NewReader(out))
	sc.Buffer(make([]byte, 0, 1<<20), 1<<22)
	for sc.Scan() {
		f := strings.Split(sc.Text(), "\t")
		if len(f) != 4 {
			continue
		}
		p := pkg{importPath: f[0], dir: relDir(f[0])}
		p.imports = ownPackages(splitList(f[1]))
		p.testImps = ownPackages(append(splitList(f[2]), splitList(f[3])...))
		pkgs = append(pkgs, p)
	}
	return pkgs, sc.Err()
}

func splitList(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, ",")
}

// ownPackages keeps only this module's packages: a third-party dependency
// carries its own license and is not what this gate is about.
func ownPackages(in []string) []string {
	var out []string
	for _, p := range in {
		if p == modulePath || strings.HasPrefix(p, modulePath+"/") {
			out = append(out, p)
		}
	}
	return out
}

func relDir(importPath string) string {
	if importPath == modulePath {
		return "."
	}
	return strings.TrimPrefix(importPath, modulePath+"/")
}

// CheckImportBoundary is the gate: no MIT package may reach an AGPL package,
// at any depth, through non-test imports.
//
// This is the property the whole split rests on. The MIT artifacts — the
// `wadjet` library and the `wadjet` binary — are distributable under the MIT
// terms only while nothing they link is AGPL, and "nothing they link" is a
// transitive question that no reviewer can answer by reading a diff.
func CheckImportBoundary(pkgs []pkg) []Violation {
	byPath := make(map[string]pkg, len(pkgs))
	for _, p := range pkgs {
		byPath[p.importPath] = p
	}
	var out []Violation
	for _, p := range pkgs {
		if licenseOf(p.dir) != mit {
			continue
		}
		if path, ok := reachesAGPL(byPath, p.importPath); ok {
			out = append(out, Violation{
				Where: p.dir,
				What: fmt.Sprintf("an MIT package reaches AGPL code: %s\n"+
					"      Either the import is wrong, or this package belongs in the AGPL region "+
					"(declare it in tools/licensecheck/regions.go and give its directory a LICENSE).",
					strings.Join(trimPaths(path), " -> ")),
			})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Where < out[j].Where })
	return out
}

// reachesAGPL walks the import graph depth-first and returns the first path
// from start to an AGPL package. The PATH is the finding: "this package
// reaches the coordinator" is not actionable, "through internal/cli, through
// internal/clid" is.
func reachesAGPL(byPath map[string]pkg, start string) ([]string, bool) {
	seen := map[string]bool{}
	var path []string
	var walk func(string) bool
	walk = func(cur string) bool {
		if seen[cur] {
			return false
		}
		seen[cur] = true
		p, ok := byPath[cur]
		if !ok {
			return false
		}
		path = append(path, cur)
		if cur != start && licenseOf(p.dir) == agpl {
			return true
		}
		for _, imp := range p.imports {
			if walk(imp) {
				return true
			}
		}
		path = path[:len(path)-1]
		return false
	}
	if walk(start) {
		return path, true
	}
	return nil, false
}

func trimPaths(in []string) []string {
	out := make([]string, len(in))
	for i, p := range in {
		out[i] = relDir(p)
	}
	return out
}

// CheckTestCrossings reports an MIT package whose TESTS import AGPL code and
// that is not on the declared list. Test binaries are not distributed, so
// this is not the same violation as the one above — but it is still a
// crossing, and an undeclared one is an accident.
func CheckTestCrossings(pkgs []pkg) []Violation {
	var out []Violation
	used := map[string]bool{}
	for _, p := range pkgs {
		if licenseOf(p.dir) != mit {
			continue
		}
		var agplImports []string
		for _, imp := range p.testImps {
			if licenseOf(relDir(imp)) == agpl {
				agplImports = append(agplImports, relDir(imp))
			}
		}
		if len(agplImports) == 0 {
			continue
		}
		if _, ok := testCrossings[p.dir]; ok {
			used[p.dir] = true
			continue
		}
		sort.Strings(agplImports)
		out = append(out, Violation{
			Where: p.dir,
			What: fmt.Sprintf("the tests of an MIT package import AGPL code (%s).\n"+
				"      Nothing shipped links a test binary, so this is allowed — but it is declared, "+
				"not assumed: add the directory and the reason to testCrossings in "+
				"tools/licensecheck/regions.go, or move the test.", strings.Join(agplImports, ", ")),
		})
	}
	for dir := range testCrossings {
		if !used[dir] {
			out = append(out, Violation{
				Where: dir,
				What: "declared in testCrossings but its tests import no AGPL package any more — " +
					"delete the entry, so the list says what is true.",
			})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Where < out[j].Where })
	return out
}

// CheckSPDX is the second gate: every .go file carries the SPDX identifier of
// its directory's region.
//
// A file is read by a person one file at a time, and a per-directory LICENSE
// is easy to miss. The header is the answer at the point of reading, and
// because it is asserted against the DECLARED region, a file moved from one
// region to the other fails until its header is fixed.
func CheckSPDX(root string) ([]Violation, int, error) {
	var out []Violation
	checked := 0
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := d.Name()
		if d.IsDir() {
			// What the Go toolchain itself skips, plus vendor. A git
			// worktree lives at .claude/worktrees/<name>/ INSIDE the module
			// root and is a second copy of every file (CLAUDE.md).
			if path != root && (strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") || name == "vendor" || name == "testdata") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(name, ".go") {
			return nil
		}
		rel, rerr := filepath.Rel(root, path)
		if rerr != nil {
			return rerr
		}
		src, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		if isGenerated(src) {
			return nil
		}
		checked++
		want := spdxLine(licenseOf(filepath.ToSlash(filepath.Dir(rel))))
		if got, ok := spdxOf(src); !ok {
			out = append(out, Violation{Where: filepath.ToSlash(rel),
				What: "no SPDX header. Add " + want + " as the first line, followed by a blank line."})
		} else if got != want {
			out = append(out, Violation{Where: filepath.ToSlash(rel),
				What: fmt.Sprintf("SPDX header says %q but its directory is %s. Either the header or "+
					"the region in tools/licensecheck/regions.go is wrong.", got, want)})
		}
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Where < out[j].Where })
	return out, checked, err
}

// spdxOf returns the file's SPDX identifier line, if it carries one in the
// header comment block before the first declaration.
func spdxOf(src []byte) (string, bool) {
	sc := bufio.NewScanner(bytes.NewReader(src))
	for sc.Scan() {
		line := strings.TrimRight(sc.Text(), " \t")
		if strings.HasPrefix(line, "// SPDX-License-Identifier:") {
			return line, true
		}
		if strings.HasPrefix(line, "package ") {
			return "", false
		}
	}
	return "", false
}

// isGenerated reports the convention every Go generator writes: a file
// nobody edits cannot be expected to keep a header a regeneration would
// drop. The directory's LICENSE file is what covers those.
func isGenerated(src []byte) bool {
	sc := bufio.NewScanner(bytes.NewReader(src))
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "// Code generated ") && strings.HasSuffix(line, " DO NOT EDIT.") {
			return true
		}
		if strings.HasPrefix(line, "package ") {
			return false
		}
	}
	return false
}

// CheckLicenseFiles: the root carries both license texts, every AGPL
// directory carries a verbatim copy of the AGPL one, and the two MIT
// carve-outs under an AGPL directory carry the MIT one. pkg.go.dev resolves
// a package's license from the nearest LICENSE at or above it, so a missing
// copy publishes the WRONG license rather than none.
func CheckLicenseFiles(root string, pkgs []pkg) []Violation {
	var out []Violation
	mitText, err := os.ReadFile(filepath.Join(root, "LICENSE"))
	if err != nil {
		return []Violation{{Where: "LICENSE", What: "missing: the root license must be the MIT text"}}
	}
	if !bytes.Contains(mitText, []byte("MIT License")) {
		out = append(out, Violation{Where: "LICENSE",
			What: "the root LICENSE is not the MIT text. GitHub's detector reads this file and it is what " +
				"the repository is labelled by."})
	}
	agplText, err := os.ReadFile(filepath.Join(root, "LICENSE-AGPL-3.0"))
	if err != nil {
		return append(out, Violation{Where: "LICENSE-AGPL-3.0", What: "missing: the AGPL text the distributed engine is under"})
	}
	for _, dir := range licenseFileDirs(pkgs) {
		want, name := agplText, "LICENSE-AGPL-3.0"
		if licenseOf(dir) == mit {
			want, name = mitText, "LICENSE"
		}
		got, err := os.ReadFile(filepath.Join(root, dir, "LICENSE"))
		if err != nil {
			out = append(out, Violation{Where: dir + "/LICENSE",
				What: "missing: pkg.go.dev applies the nearest LICENSE at or above a package, and the " +
					"nearest one above this directory is the wrong license. Copy " + name + " here."})
			continue
		}
		if !bytes.Equal(got, want) {
			out = append(out, Violation{Where: dir + "/LICENSE",
				What: "is not a verbatim copy of " + name + " — a license copy that drifts is a second license."})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Where < out[j].Where })
	return out
}

// licenseFileDirs is every directory that needs its own LICENSE: an AGPL
// package whose nearest declared ancestor is not already AGPL, and an MIT
// package that sits under an AGPL one.
func licenseFileDirs(pkgs []pkg) []string {
	seen := map[string]bool{}
	var dirs []string
	for _, p := range pkgs {
		dir := p.dir
		if dir == "." {
			continue
		}
		mine := licenseOf(dir)
		if mine == licenseOf(parent(dir)) {
			continue // the ancestor's LICENSE already answers for this one
		}
		if seen[dir] {
			continue
		}
		seen[dir] = true
		dirs = append(dirs, dir)
	}
	sort.Strings(dirs)
	return dirs
}

func parent(dir string) string {
	if i := strings.LastIndex(dir, "/"); i >= 0 {
		return dir[:i]
	}
	return "."
}

// CheckLicensingDoc: LICENSING.md names exactly the AGPL directories the
// regions declare. The document is how a user learns what they may take
// under which terms, and a document that disagrees with the code is worse
// than none.
func CheckLicensingDoc(root string, pkgs []pkg) []Violation {
	doc, err := os.ReadFile(filepath.Join(root, "LICENSING.md"))
	if err != nil {
		return []Violation{{Where: "LICENSING.md", What: "missing"}}
	}
	text := string(doc)
	var out []Violation
	declared := map[string]bool{}
	for _, r := range regions {
		if r.license == agpl {
			declared[r.prefix] = true
		}
	}
	for dir := range declared {
		if !strings.Contains(text, "`"+dir+"/`") {
			out = append(out, Violation{Where: "LICENSING.md",
				What: "does not list the AGPL directory `" + dir + "/`"})
		}
	}
	// And the other direction: a directory the document claims is AGPL but
	// the regions do not.
	for _, line := range strings.Split(text, "\n") {
		for _, field := range strings.Split(line, "`") {
			if !strings.HasSuffix(field, "/") || strings.Contains(field, " ") {
				continue
			}
			dir := strings.TrimSuffix(field, "/")
			if !strings.HasPrefix(dir, "internal/") && !strings.HasPrefix(dir, "cmd/") && !strings.HasPrefix(dir, "gen/") {
				continue
			}
			if declared[dir] || licenseOf(dir) == mit {
				continue
			}
			out = append(out, Violation{Where: "LICENSING.md",
				What: "names `" + field + "` as a region, which tools/licensecheck/regions.go does not declare"})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].What < out[j].What })
	return out
}
