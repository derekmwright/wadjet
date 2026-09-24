// SPDX-License-Identifier: MIT

// Command testquiet summarizes test output by package and failing test.
// It reads go test JSON or a saved text log, retaining failure details
// without printing the whole successful run. See docs/testing/GATES.md.
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
