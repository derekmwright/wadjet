// SPDX-License-Identifier: MIT

package main

// The license regions of this repository, declared.
//
// One module, two licenses: the embedded engine is MIT and the distributed
// engine is AGPL-3.0 with a commercial option (LICENSING.md). Which half a
// package is in is DECLARED here rather than derived from what it happens to
// import, because a derived region would relicense a directory the moment
// somebody added an import — silently, in the direction that gives code
// away. Declared, the same import is a gate failure instead.
//
// Longest prefix wins, so internal/server is AGPL while its pgwire and mcp
// subdirectories — the PostgreSQL wire protocol and the MCP server, both of
// which the embedded binary carries — are MIT.
var regions = []region{
	// The distributed engine.
	{"internal/coordinator", agpl},
	// Declared in its own right, though the prefix above already covers it:
	// LICENSING.md names it as a region because it is the stage-DAG PLANNER,
	// the part of the split with the most commercial weight, and the document
	// and this list are held equal by the gate.
	{"internal/coordinator/dagplan", agpl},
	{"internal/worker", agpl},
	{"internal/distributed", agpl},
	{"internal/dataplane", agpl},
	{"internal/wshf", agpl},
	{"internal/server", agpl},
	{"internal/clid", agpl},
	{"internal/harness", agpl},
	{"cmd/wadjetd", agpl},
	{"cmd/tpch-harness", agpl},
	{"cmd/tpch-bench", agpl},
	{"cmd/security-bench", agpl},
	{"gen/dataplane", agpl},

	// Carve-outs inside it: the protocol servers the embedded binary runs.
	{"internal/server/pgwire", mit},
	{"internal/server/mcp", mit},
}

// testCrossings are the MIT directories whose TEST files may import an AGPL
// package, with the reason. A test binary is not a distributed artifact —
// nothing shipped links these — but a crossing is still a deliberate act, so
// each one is named here and a new one fails until it is.
var testCrossings = map[string]string{
	"benchmarks/tpch": "the two-path invariance corpus runs every query through the single-process engine AND the distributed stage DAG, which is the whole point of it",
	"internal/cli":    "two config gates assert that a `serve` config reaches the HTTP server's own planner, which lives with the distributed engine",
}

// licensedDirs are the directories that must carry their own LICENSE file:
// every AGPL directory, because the root LICENSE is MIT and pkg.go.dev
// resolves a license per directory (nearest LICENSE at or above the
// package), and the two MIT carve-outs under internal/server, because the
// nearest LICENSE above them would otherwise be the AGPL one.
const (
	mit  = "MIT"
	agpl = "AGPL-3.0-only"
)

type region struct {
	prefix  string
	license string
}

// licenseOf returns the declared license of a package directory, relative to
// the module root, by longest declared prefix. Everything undeclared is MIT.
func licenseOf(dir string) string {
	best, license := -1, mit
	for _, r := range regions {
		if dir == r.prefix || hasPathPrefix(dir, r.prefix) {
			if len(r.prefix) > best {
				best, license = len(r.prefix), r.license
			}
		}
	}
	return license
}

func hasPathPrefix(dir, prefix string) bool {
	return len(dir) > len(prefix) && dir[:len(prefix)] == prefix && dir[len(prefix)] == '/'
}

// spdxLine is the header every .go file carries, naming its directory's
// region.
func spdxLine(license string) string {
	return "// SPDX-License-Identifier: " + license
}
