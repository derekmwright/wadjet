// SPDX-License-Identifier: MIT

// Command licensecheck holds the license boundary of this repository.
//
// One module, two licenses (LICENSING.md): the embedded engine and the
// `wadjet` binary are MIT, the distributed engine and `wadjetd` are
// AGPL-3.0 with a commercial option. That split is only true while two
// properties hold, and neither can be checked by reading a diff:
//
//  1. no MIT package reaches an AGPL package, at any depth, through
//     non-test imports — otherwise the MIT artifacts link AGPL code;
//  2. no MIT package DECLARES the distributed planner's vocabulary — the
//     stage, the exchange, the distribution property — because an import
//     gate cannot see planning written on the wrong side of the line;
//  3. every .go file carries the SPDX identifier of its directory's region,
//     every AGPL directory carries a verbatim copy of the AGPL text, and
//     LICENSING.md names the same directories the code declares.
//
// Run it with no arguments from the module root:
//
//	go run ./tools/licensecheck .
//
// It prints every violation with the import path that produced it, and
// exits non-zero if there is one. `task housekeeping` and CI run it; the
// same checks run as ordinary Go tests in this package, so `go test
// ./tools/...` catches them too.
package main

import (
	"fmt"
	"os"
)

func main() {
	root := "."
	if len(os.Args) > 1 {
		root = os.Args[1]
	}
	pkgs, err := loadPackages(root)
	if err != nil {
		fmt.Fprintln(os.Stderr, "licensecheck:", err)
		os.Exit(2)
	}

	type check struct {
		name string
		run  func() []Violation
	}
	spdxCount := 0
	dagScanned := 0
	checks := []check{
		{"import boundary", func() []Violation { return CheckImportBoundary(pkgs) }},
		{"test crossings", func() []Violation { return CheckTestCrossings(pkgs) }},
		{"SPDX headers", func() []Violation {
			v, n, err := CheckSPDX(root)
			if err != nil {
				return []Violation{{Where: root, What: err.Error()}}
			}
			spdxCount = n
			return v
		}},
		{"DAG vocabulary", func() []Violation {
			v, n, err := CheckNoDAGPlanningInMIT(root)
			if err != nil {
				return []Violation{{Where: root, What: err.Error()}}
			}
			dagScanned = n
			return v
		}},
		{"license files", func() []Violation { return CheckLicenseFiles(root, pkgs) }},
		{"LICENSING.md", func() []Violation { return CheckLicensingDoc(root, pkgs) }},
	}

	failed := false
	for _, c := range checks {
		vs := c.run()
		if len(vs) == 0 {
			fmt.Printf("ok   %s\n", c.name)
			continue
		}
		failed = true
		fmt.Printf("FAIL %s\n", c.name)
		for _, v := range vs {
			fmt.Printf("     %s\n", v)
		}
	}
	fmt.Printf("     (%d packages, %d .go files with a header, %d MIT files read for DAG vocabulary)\n",
		len(pkgs), spdxCount, dagScanned)
	if failed {
		os.Exit(1)
	}
}
