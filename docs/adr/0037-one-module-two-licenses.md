# ADR-0037: One module, two licenses, and an embedded server that needs neither

Status: Accepted
Date: 2026-09-15

## Context

Wadjet is two products in one repository. One is an engine you link into a Go
program — a parser, an optimizer, a vectorized executor over Parquet and
Iceberg, a type system, and the PostgreSQL wire protocol in front of them.
The other is a distributed query system — a coordinator that plans and
dispatches, workers that execute fragments, a durable S3 exchange, and the
placement policy that decides who does what.

Those two have different economics. The embedded engine competes with
libraries, where a copyleft license is a reason not to adopt and adoption is
the whole point. The distributed engine competes with services, where the
AGPL is what keeps a hosted competitor from taking the work without
returning any, and where a commercial license is the thing somebody buys.

Until now the whole repository was AGPL-3.0, which priced the library half
out of the use it was built for, and no amount of documentation changes that:
a prospective embedder reads one `LICENSE` file at the root and stops.

Three shapes were considered.

**Two repositories.** The cleanest licensing story, and unworkable here:
`wadjet/` — the public API — is typed in terms of `internal/` packages, which
Go forbids an out-of-tree module from importing (#805). Splitting the repo
would mean promoting a large part of `internal/` to a public API before the
API is ready to be one, and every cross-arm gate in this repository (two-path
invariance, the five-arm census, the door matrix) would have to run across a
module boundary against a pinned version of the other half.

**Two modules in one repository.** Same `internal/` problem, one directory
down: `internal/` visibility is scoped to the module that declares it, so the
distributed module could not see the engine's internals at all.

**Two license regions in one module.** What this ADR records.

## Decision

**1. The repository is one Go module with two declared license regions.** The
embedded engine — everything `wadjet/` transitively imports, plus the
PostgreSQL wire protocol, the MCP server, `cmd/wadjet` and the tools — is
MIT. The distributed engine — `internal/coordinator` (including its
`dagplan` placement policy), `internal/worker`, `internal/distributed`,
`internal/dataplane`, `internal/wshf`, `internal/server`, `internal/clid`,
`internal/harness`, `cmd/wadjetd` and the benchmark commands that stand up
clusters — is AGPL-3.0, with a commercial license available. `LICENSING.md`
is the map; the root `LICENSE` is the MIT text, because that is the file
GitHub's detector and a prospective embedder both read.

**2. The boundary is a property the test suite holds, not a convention.**
`tools/licensecheck` fails if any MIT package reaches an AGPL package through
non-test imports at any depth, and prints the import path that did it. The
MIT artifacts are distributable under the MIT terms only while nothing they
link is AGPL; that is a transitive question, and no diff review answers it.
A second check requires every `.go` file to carry the SPDX identifier of its
directory's region, every AGPL directory to carry a verbatim copy of the AGPL
text (pkg.go.dev resolves a license from the nearest `LICENSE` at or above a
package, and the root one is now MIT), and `LICENSING.md` to name exactly the
directories the code declares.

**3. Regions are declared, not derived.** `tools/licensecheck/regions.go`
holds the list, longest prefix wins. A derived region — "this package is AGPL
because it imports the coordinator" — would relicense a directory the moment
somebody added an import, silently, in the direction that gives code away.
Declared, the same import is a gate failure.

**4. Test binaries may cross the boundary; shipped artifacts may not.** The
two-path invariance corpus runs every query through the single-process engine
AND the stage DAG, which is its entire purpose. Nothing shipped links a test
binary, so the combination is never distributed. Each crossing is declared
with its reason and an undeclared one fails, so the list stays short and
deliberate.

**5. There are two binaries.** `wadjet serve` is the embedded server: the
PostgreSQL wire protocol over the engine in its own process, with no
coordinator, no workers and no task queues. `wadjetd serve
--mode=standalone|coordinator|worker` is the distributed server, unchanged.
The embedded one refuses `--mode=coordinator` and `--mode=worker` by name
rather than pretending to be them.

This is what makes the boundary real rather than notional. Before it, the
only way to serve the PostgreSQL wire protocol was to link the coordinator:
`internal/server/pgwire` held a `*coordinator.Coordinator`, so the wire
protocol every embedded user needs dragged the distributed engine in at
compile time. The routed path is now an interface the wire server owns
(`internal/queryroute`), which the coordinator satisfies and the embedded
server simply does not install.

**6. The CLA is the basis, and is not narrowed.** §3 records that the
maintainer may license the work, including contributions, under multiple
licenses. Relicensing part of the tree from AGPL-3.0 to MIT is that right in
use, in the direction that gives users more. The CLA text now says so
explicitly; the grant itself is unchanged.

## Consequences

- **A new package is MIT by default.** Anything not declared in `regions.go`
  is MIT, which is the safe direction for a mistake: an AGPL-only dependency
  in an undeclared package fails the import gate loudly, while an
  over-declared AGPL package merely refuses an import somebody would then
  reconsider.
- **The MIT half cannot grow a distributed dependency by accident.** Which
  also means: when it legitimately needs one, the fix is an interface the MIT
  side owns. `internal/queryroute` and `cli.ServeOptions` are the two worked
  examples in the landing arc.
- **The distributed engine's own planning belongs on its side of the line.**
  `internal/coordinator/dagplan` is where that migration starts: the shuffle
  candidate, the large-build scan set and the aggregate-shuffle rewrite moved
  there. The rest of the stage planning — stage emission, the
  distribution/exchange assignment and set-op stage planning — is still in
  `internal/planner/physical`, because it is reachable from 136 of that
  package's 139 non-test files through the shared `Planner` type and its
  ~150 unexported helpers. Moving it is a package-scale split, not a file
  move, and it is the next step in this direction rather than a defect.
- **Two binaries to ship, two to document.** Every release publishes both;
  every `serve` in the documentation says which one.
- **Per-directory `LICENSE` copies are duplication on purpose.** Thirteen
  copies of the AGPL text and two of the MIT text exist so that per-directory
  license resolution — pkg.go.dev's and most scanners' — answers correctly.
  The gate requires them to be byte-identical to the root texts, because a
  license copy that drifts is a second license.
