// SPDX-License-Identifier: MIT

package main

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
)

var (
	reFailStart   = regexp.MustCompile(`^--- FAIL: (\S+) \(.*\)$`)
	reOtherStart  = regexp.MustCompile(`^--- (PASS|SKIP): \S+ \(.*\)$`)
	rePkgSummary  = regexp.MustCompile(`^(ok|FAIL)\s+(\S+)\s+(.+)$`)
	reNoTestFiles = regexp.MustCompile(`^\?\s+(\S+)\s+\[no test files\]$`)
)

// failBlock buffers one "--- FAIL: Name (...)" test's output until the
// package summary line that follows it attributes it to a package. It is
// tracked by pointer identity, not by test name, so two different failing
// packages that happen to share a test name never collide.
type failBlock struct {
	test  string
	lines []string
}

// FilterTextLog reads a plain-text `go test` log — no -v, no -json, the
// shape battery_run.sh's lanes write (`go test ... > log 2>&1`): only
// failing tests print their own "--- FAIL" block, each package prints its
// summary line exactly once, in package-finish order — and writes the same
// quiet shape Summarize does: one line per package, then each failing
// test's name and the last ~tailLines lines of its buffered output.
//
// It also tolerates a verbose (-v) log: "--- PASS"/"--- SKIP" blocks are
// recognized and their content discarded, same as Summarize does for a
// passing or skipped test.
//
// It returns 1 if any package's summary line was FAIL, or a "--- FAIL"
// block was still open at EOF (a truncated log — the process was killed
// or the log was captured mid-run), 0 otherwise: there is no live process
// to ask for the real exit code.
func FilterTextLog(r io.Reader, w io.Writer) int {
	type pkgResult struct{ status, rest string }

	var pkgOrder []string
	pkgSeen := map[string]bool{}
	results := map[string]pkgResult{}

	var pending []*failBlock // finished "--- FAIL" blocks not yet attributed to a package
	var cur *failBlock       // the block currently buffering output, if any

	type attributed struct {
		pkg   string
		block *failBlock
	}
	var failed []attributed

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := scanner.Text()

		switch {
		case reFailStart.MatchString(line):
			m := reFailStart.FindStringSubmatch(line)
			cur = &failBlock{test: m[1]}
			pending = append(pending, cur)
		case reOtherStart.MatchString(line), line == "FAIL", line == "PASS":
			cur = nil
		case reNoTestFiles.MatchString(line):
			cur = nil
			pkg := reNoTestFiles.FindStringSubmatch(line)[1]
			if !pkgSeen[pkg] {
				pkgSeen[pkg] = true
				pkgOrder = append(pkgOrder, pkg)
			}
			results[pkg] = pkgResult{"ok", "[no test files]"}
			pending = nil
		case rePkgSummary.MatchString(line):
			cur = nil
			m := rePkgSummary.FindStringSubmatch(line)
			status, pkg, rest := m[1], m[2], m[3]
			if !pkgSeen[pkg] {
				pkgSeen[pkg] = true
				pkgOrder = append(pkgOrder, pkg)
			}
			results[pkg] = pkgResult{status, rest}
			for _, b := range pending {
				failed = append(failed, attributed{pkg, b})
			}
			pending = nil
		default:
			if cur != nil {
				cur.lines = append(cur.lines, line)
			}
		}
	}

	exit := 0
	if cur != nil {
		exit = 1 // a failing test's block never closed — a truncated log is a failure
	}
	for _, pkg := range pkgOrder {
		res := results[pkg]
		if res.status == "FAIL" {
			exit = 1
		}
		fmt.Fprintf(w, "%-4s %s  %s\n", res.status, pkg, res.rest)
	}
	// Any block still in `pending` (a package whose FAIL block never got a
	// summary line — the log was truncated) is reported too, under the
	// literal test name with no package attribution.
	for _, b := range pending {
		failed = append(failed, attributed{"[unattributed — truncated log]", b})
	}
	for _, a := range failed {
		fmt.Fprintf(w, "--- FAIL: %s  (%s)\n", a.block.test, a.pkg)
		for _, l := range tail(a.block.lines, tailLines) {
			fmt.Fprintln(w, "    "+l)
		}
	}
	return exit
}
