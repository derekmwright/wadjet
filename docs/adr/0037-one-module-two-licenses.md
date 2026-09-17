# ADR-0037: One module, two licenses, and an embedded server that needs neither

Status: Accepted
Date: 2026-09-15 (revised 2026-09-16: the stage-DAG planner moved; decision items 6-7 and their consequences are new)

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

**6. The stage-DAG planner is AGPL, in its own package.** `Plan` — the local
entry the embedded engine, the CLI and every pgwire fallback use — ran the
distributed stage emitter on every query and hung the result on
`PhysicalPlan.Stages`. That single call edge was the whole entanglement: with
it cut, 48 of `internal/planner/physical`'s files have no locally-reachable
declaration left, and 540 declarations — stage emission, the distribution and
exchange assignment, the shuffle fusions, set-op stage planning, the
dynamic-filter and dimension-cascade passes, DAG shape validation and every
distributed refusal — move to `internal/coordinator/dagplan`. `physical` keeps
the local pipeline planner, the declared-output logic, the set-op type
reconciliation both paths need, and the query-limit cost walk. dagplan imports
physical; physical imports nothing back, and an MIT package that names a Stage
no longer compiles.

The planning seam is `physical.PlanContext`, obtained from
`Planner.PlanContext()`. It shares the local planner's statement state and
manifest snapshot, and provides methods for column declarations, emitted
names, join-side schemas, set-operation arm types, window keys, group-key
resolution and the cost walk. `dagplan.StagePlanner` embeds this context;
argument-only walks use its zero value. Value types remain named where a
caller stores or passes them. The [measurement](../design/seam-narrowing-measurement.md)
records those names and their reasons, and `TestAGPLPhysicalReferenceBudget`
limits package-qualified physical names across all AGPL packages, including
tests.

Two user-visible consequences follow, decided rather than discovered:

  - **The cost guard reads the logical plan.** `enforceQueryLimits` estimated
    from the stage list, which is why the local entry emitted one at all. It
    now sums the same catalog numbers over the logical plan
    (`Planner.EstimatePlanScanCost`), on BOTH entries, so a query is refused
    or answered by its own text rather than by which engine planned it. The
    two counts agree exactly on 18 of the 22 TPC-H queries; the four that
    differ are pinned with their mechanism in
    `TestTheLogicalScanCostMatchesTheStageCost` (a shared subplan counted
    twice on one side, a correlated subquery's scan invisible on the other).
  - **`EXPLAIN VERBOSE` on the embedded engine prints the local plan.** It
    printed a stage DAG the embedded engine never executes, emitted only to be
    printed. A server with a coordinator still prints a stage list — but the
    list it prints is now the DAG it would DISPATCH, exchanges included, not
    the local stage generation this door rendered before. Those were never the
    same plan: the dispatched one carries the gather and replicate exchanges
    and drops the stages the local emitter produced for operators a fragment
    executes itself. Base parity is not recoverable on any door, because the
    local generator is what this ADR removed.

    Both of `wadjetd`'s doors print that same list. EXPLAIN routes to the
    coordinator (one predicate in the wire server, behaviour-neutral where no
    router is installed) and both doors render through
    `Coordinator.StagePlanTextForExplain`, so one binary has one answer for one
    statement. No renderer enters the MIT half: EXPLAIN's answer is a
    one-column result, which the router already knows how to return.
    EXPLAIN ANALYZE is not routed — it RUNS the statement — and still answers
    from the embedded database.

**7. A third gate holds where the planning LIVES.** The import gate reads what
a package imports and the SPDX gate reads what a file says; neither can see
distributed planning WRITTEN on the MIT side, which is how 540 such
declarations sat in an MIT directory with both gates green.
`CheckNoDAGPlanningInMIT` reads declarations against a DECLARED vocabulary —
the stage, the exchange, the distribution property, stage emission, the
shuffle and probe-split policy. After the move most of that is
compiler-enforced anyway (naming a stage requires an import, which the
boundary gate refuses); the vocabulary gate is the residual that stops NEW
distributed planning being written on the MIT side before anybody reaches for
the import.

**8. The CLA is the basis, and is not narrowed.** §3 records that the
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
- **The distributed engine's planning is on its side of the line.** All of it:
  `internal/coordinator/dagplan` holds 540 declarations and 85 files, and
  `internal/planner/physical` holds no declaration that names a stage. The
  first cut of this ADR deferred that move on a measurement of the wrong
  thing — the closure of a FILE seed through the shared `Planner` type, which
  reaches nearly everything. The decisive number is narrower: one call edge,
  `plan.Stages = p.generateStages(node)` inside the local `Plan`. Cutting it
  took the reachable-from-local set from 25 files to 44, and the move fell
  out of the compiler.
- **The Planner is two planners.** `physical.Planner` is the local pipeline
  planner; `dagplan.StagePlanner` embeds its planning context and adds the 23 fields of
  per-build scratch stage emission keeps. Every embedded query used to carry
  those fields.
- **physical exposes a planning context.** The shared planner state and local
  planning operations are reached through `PlanContext`; the measurement
  records the separate value types and constructors that callers still name.
- **Two binaries to ship, two to document.** Every release publishes both;
  every `serve` in the documentation says which one.
- **Per-directory `LICENSE` copies are duplication on purpose.** Thirteen
  copies of the AGPL text and two of the MIT text exist so that per-directory
  license resolution — pkg.go.dev's and most scanners' — answers correctly.
  The gate requires them to be byte-identical to the root texts, because a
  license copy that drifts is a second license.
