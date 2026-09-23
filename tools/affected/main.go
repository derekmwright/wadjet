// SPDX-License-Identifier: MIT

// Command affected prints the packages a git diff could affect: every
// package whose test binary transitively depends on a package that
// changed between BASE and HEAD, plus every changed package's own tests.
// See `task affected [BASE=sha]`.
//
//	go run ./tools/affected [-base sha] [-C dir]
//
// The output is one import path per line, meant to be fed straight into
// `go test -p 2 $(task affected)` instead of the full `./...` for a fast
// local loop on one change — the landing battery still runs the full
// suite; this does not replace it.
package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	base := flag.String("base", "main", "the git ref to diff against (git diff --name-only BASE...HEAD)")
	dir := flag.String("C", ".", "the module root to run in")
	flag.Parse()

	pkgs, err := Affected(*dir, *base)
	if err != nil {
		fmt.Fprintln(os.Stderr, "affected:", err)
		os.Exit(2)
	}
	for _, p := range pkgs {
		fmt.Println(p)
	}
}
