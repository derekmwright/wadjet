// SPDX-License-Identifier: MIT

// Command testquiet runs `go test -json` (or filters an existing go test
// text log) and prints only what an agent or a CI step needs to act on: one
// line per package (ok/FAIL/skip + duration) and, for each failing test,
// its name plus the last ~40 lines of its own output — never the full log.
// See `task test-quiet` / `task test-quiet-log`.
//
// Two modes:
//
//	go run ./tools/testquiet -pkgs "./internal/... ./wadjet/" [-run pattern]
//	go run ./tools/testquiet -log path/to/existing.log
//
// The first spawns `go test -json` itself and exits with go test's own
// exit code (a build failure, a killed process, or a plain test failure
// all show up there — the summary never guesses at one). The second reads
// a plain-text `go test` log — no -v, no -json, the shape the landing
// battery's lanes write (tooling/battery_run.sh: `go test ... > log 2>&1`)
// — and derives an exit code from whether anything in it failed, since
// there is no live process to ask.
//
// stdlib only, deliberately: this is not a gotestsum replacement, just
// enough of one to keep a raw `go test` log out of an agent's context.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

func main() {
	pkgs := flag.String("pkgs", "", "space-separated package patterns to test (live mode)")
	run := flag.String("run", "", "optional -run pattern passed through to go test")
	logPath := flag.String("log", "", "filter an existing go test text log instead of running tests")
	flag.Parse()

	switch {
	case *logPath != "":
		f, err := os.Open(*logPath)
		if err != nil {
			fmt.Fprintln(os.Stderr, "testquiet:", err)
			os.Exit(2)
		}
		defer f.Close()
		os.Exit(FilterTextLog(f, os.Stdout))
	case *pkgs != "":
		list := strings.Fields(*pkgs)
		if len(list) == 0 {
			fmt.Fprintln(os.Stderr, "testquiet: -pkgs is empty")
			os.Exit(2)
		}
		os.Exit(RunLive(list, *run, os.Stdout, os.Stderr))
	default:
		fmt.Fprintln(os.Stderr, "testquiet: one of -pkgs or -log is required")
		os.Exit(2)
	}
}
