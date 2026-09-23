// SPDX-License-Identifier: MIT

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"strings"
)

// testEvent mirrors the subset of cmd/test2json's TestEvent (`go help
// test`, -json) this tool reads. A build failure carries ImportPath
// instead of Package, and its ImportPath ends in ".test" — the built test
// binary's own import path, not the package's.
type testEvent struct {
	Action     string
	Package    string
	ImportPath string
	Test       string
	Elapsed    float64
	Output     string
}

// testKey identifies one test (or a synthetic "[build error]" entry) inside
// a package, for buffering its output and reporting it once.
type testKey struct {
	Package string
	Test    string
}

const tailLines = 40

// Summarize reads a stream of `go test -json` events from r and writes the
// quiet summary to w: one line per package (its final status + duration),
// then for every failing test its name and the last ~tailLines lines of
// its own buffered output — never a passing test's output, never the
// build/run noise go test interleaves with it.
//
// It returns 0 if every package's own status came back "pass" or "skip",
// 1 otherwise. RunLive prefers the real subprocess exit code over this
// derived one whenever both are available — a killed process or a panic
// outside any test can leave the JSON stream silent about the truth while
// the process's exit code still knows it.
func Summarize(r io.Reader, w io.Writer) int {
	var pkgOrder []string
	pkgSeen := map[string]bool{}
	pkgStatus := map[string]string{} // "pass" | "fail" | "skip"
	pkgElapsed := map[string]float64{}
	var failOrder []testKey
	failSeen := map[testKey]bool{}
	output := map[testKey][]string{}

	touchPkg := func(pkg string) {
		if pkg != "" && !pkgSeen[pkg] {
			pkgSeen[pkg] = true
			pkgOrder = append(pkgOrder, pkg)
		}
	}
	addFail := func(k testKey) {
		if !failSeen[k] {
			failSeen[k] = true
			failOrder = append(failOrder, k)
		}
	}

	dec := json.NewDecoder(bufio.NewReader(r))
	for {
		var ev testEvent
		if err := dec.Decode(&ev); err != nil {
			break // io.EOF, or a non-JSON line merged into the stream — either way, nothing more to parse
		}

		if ev.Action == "build-output" || ev.Action == "build-fail" {
			pkg := strings.TrimSuffix(ev.ImportPath, ".test")
			touchPkg(pkg)
			bk := testKey{pkg, "[build error]"}
			if ev.Action == "build-output" {
				if s := stripNewline(ev.Output); s != "" {
					output[bk] = append(output[bk], s)
				}
			} else {
				pkgStatus[pkg] = "fail"
				addFail(bk)
			}
			continue
		}

		if ev.Package == "" {
			continue
		}
		touchPkg(ev.Package)
		key := testKey{ev.Package, ev.Test}

		switch ev.Action {
		case "output":
			if ev.Test != "" {
				if s := stripNewline(ev.Output); s != "" {
					output[key] = append(output[key], s)
				}
			}
			// Package-level output (the "FAIL\n" / "ok ... " lines go
			// test also emits as text) carries nothing the structured
			// pass/fail/skip events below don't already say.
		case "fail":
			if ev.Test != "" {
				addFail(key)
			} else {
				pkgStatus[ev.Package] = "fail"
				pkgElapsed[ev.Package] = ev.Elapsed
			}
		case "pass":
			if ev.Test != "" {
				delete(output, key) // a passing test's output is never shown
			} else {
				pkgStatus[ev.Package] = "pass"
				pkgElapsed[ev.Package] = ev.Elapsed
			}
		case "skip":
			if ev.Test != "" {
				delete(output, key)
			} else {
				pkgStatus[ev.Package] = "skip"
				pkgElapsed[ev.Package] = ev.Elapsed
			}
		}
	}

	exit := 0
	for _, pkg := range pkgOrder {
		status := pkgStatus[pkg]
		if status == "" {
			status = "fail" // touched (e.g. a build error) but never resolved — don't silently call that a pass
		}
		if status == "fail" {
			exit = 1
		}
		fmt.Fprintf(w, "%-4s %s  (%.3fs)\n", strings.ToUpper(status), pkg, pkgElapsed[pkg])
	}
	for _, k := range failOrder {
		fmt.Fprintf(w, "--- FAIL: %s  (%s)\n", k.Test, k.Package)
		for _, l := range tail(output[k], tailLines) {
			fmt.Fprintln(w, "    "+l)
		}
	}
	return exit
}

// RunLive runs `go test -json [-run pattern] pkgs...` as a subprocess,
// summarizes its JSON stream through Summarize as it streams, and returns
// the subprocess's own exit code.
func RunLive(pkgs []string, runPattern string, stdout, stderr io.Writer) int {
	args := []string{"test", "-json"}
	if runPattern != "" {
		args = append(args, "-run", runPattern)
	}
	args = append(args, pkgs...)

	cmd := exec.Command("go", args...)
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		fmt.Fprintln(stderr, "testquiet:", err)
		return 2
	}
	cmd.Stderr = stderr

	if err := cmd.Start(); err != nil {
		fmt.Fprintln(stderr, "testquiet:", err)
		return 2
	}
	Summarize(stdoutPipe, stdout)
	err = cmd.Wait()
	if err == nil {
		return 0
	}
	if exitErr, ok := err.(*exec.ExitError); ok {
		return exitErr.ExitCode()
	}
	fmt.Fprintln(stderr, "testquiet:", err)
	return 2
}

func stripNewline(s string) string {
	return strings.TrimRight(s, "\n")
}

// tail returns the last n elements of lines (all of them if there are
// fewer than n).
func tail(lines []string, n int) []string {
	if len(lines) <= n {
		return lines
	}
	return lines[len(lines)-n:]
}
