# Licensing

Wadjet is one repository and one Go module with **two licenses**.

| | License | What it is |
|---|---|---|
| The embedded engine and the `wadjet` binary | [MIT](LICENSE) | The query engine you link into a Go program, and the CLI and single-process server built on it |
| The distributed engine and the `wadjetd` binary | [AGPL-3.0](LICENSE-AGPL-3.0), or a commercial license | The coordinator, the workers, the exchange, and the stage-DAG planning that schedules a query across machines — `internal/coordinator/dagplan` |

The split is the deployment, not the feature set. Everything that answers a
query in one process — the parser, the optimizer, the vectorized executor,
the Parquet and Iceberg readers, the type system, the network functions, the
PostgreSQL wire protocol, the MCP server, the object-store and catalog
layers, and the LOCAL pipeline planner — is MIT. What is AGPL is what makes a
*cluster*: the coordinator that plans and dispatches, the workers that
execute fragments, the shuffle and gather exchange, and the stage-DAG planner
that decides who does what.

That last one is a directory, not a figure of speech:
`internal/coordinator/dagplan` holds the stage emitter, the
distribution/exchange assignment and the shuffle policy, and
`internal/planner/physical` — which the embedded engine links — holds the
local pipeline planner and nothing that names a stage. `wadjet serve`
executes a pipeline and its `EXPLAIN VERBOSE` says so; the stage list is
printed by the servers that dispatch one, on both of their doors — `wadjetd`
routes EXPLAIN to its coordinator so its PostgreSQL wire door and its HTTP
door print the same plan.

Both answer the same SQL with the same answers, and that is gated, not
claimed: `TestTwoPathInvariance` runs every corpus query through both and
requires identical results.

## The two binaries

```bash
wadjet serve     # MIT: the PostgreSQL wire protocol over the engine in this process
wadjetd serve --mode=standalone|coordinator|worker    # AGPL-3.0: the distributed server
```

`wadjet` also carries the rest of the command line — `query`, `shell`,
`tables`, `create-table`, `drop-table`, `compact`, `catalog`, `clusters`,
`mcp` — and `wadjetd` carries all of it too, because a server operator needs
the same tools.

## The AGPL directories

These directories, and everything under them, are AGPL-3.0:

- `internal/coordinator/` — query coordination: planning, dispatch, gather and the local fast path
- `internal/coordinator/dagplan/` — the distributed PLANNER: stage emission, the distribution and exchange assignment, the shuffle fusions, set-op stage planning, the dynamic-filter and dimension-cascade passes, DAG shape validation and every distributed refusal
- `internal/worker/` — the distributed task executor
- `internal/distributed/` — the task, result and DLQ streams, the subjects and the message envelopes
- `internal/dataplane/` — the gRPC data plane for dispatch, results and peer exchange
- `internal/wshf/` — the shuffle file format
- `internal/server/` — the HTTP and gRPC servers over a coordinator (its `pgwire/` and `mcp/` subdirectories are MIT)
- `internal/clid/` — the distributed serve modes of the command line
- `internal/harness/` — the distributed test harness
- `cmd/wadjetd/` — the server daemon
- `cmd/tpch-harness/`, `cmd/tpch-bench/`, `cmd/security-bench/` — benchmark drivers that stand up clusters
- `gen/dataplane/` — the generated data-plane protobuf code

Everything else in the repository is MIT, including `wadjet/` (the public
API), `cmd/wadjet/`, `internal/engine/`, `internal/planner/` (the parser, the
logical optimizer and the LOCAL physical planner), `internal/storage/`,
`internal/auth/`, `internal/server/pgwire/`, `internal/server/mcp/` and
`tools/`.

Each AGPL directory carries a verbatim copy of `LICENSE-AGPL-3.0` as its own
`LICENSE`, because pkg.go.dev and most license scanners resolve a package's
license from the nearest `LICENSE` at or above it, and the one at the root is
MIT. `internal/server/pgwire/` and `internal/server/mcp/` carry a copy of the
MIT text for the same reason in the other direction.

Every `.go` file states its own region in an SPDX header:

```go
// SPDX-License-Identifier: MIT
// SPDX-License-Identifier: AGPL-3.0-only
```

Generated files (`// Code generated ... DO NOT EDIT.`) do not, because a
regeneration would drop it; their directory's `LICENSE` is what covers them.

## What holds the boundary

Two gates, in `tools/licensecheck`, run by CI and by `task housekeeping`:

```bash
go run ./tools/licensecheck .
go test ./tools/licensecheck/
```

1. **The import boundary.** No MIT package may reach an AGPL package through
   non-test imports, at any depth. This is the property the MIT half rests
   on: the `wadjet` library and the `wadjet` binary are distributable under
   the MIT terms only while nothing they link is AGPL, and that is a
   transitive question no reviewer can settle by reading a diff. The gate
   prints the import path that broke it.
2. **The SPDX headers.** Every `.go` file's header must match its directory's
   declared region, every AGPL directory must carry a verbatim copy of the
   AGPL text, and this document must name exactly the directories the code
   declares.
3. **The DAG vocabulary.** No MIT package may DECLARE the distributed
   planner's own words — the stage, the exchange, the distribution property,
   stage emission, the shuffle and probe-split policy. An import gate reads
   what a package imports and an SPDX gate reads what a file says; neither
   can see distributed planning WRITTEN on the MIT side, which is exactly how
   540 such declarations sat in `internal/planner/physical` with both gates
   green. The list is declared in `tools/licensecheck/dagvocabulary.go`.

The regions are **declared**, in `tools/licensecheck/regions.go`, rather than
derived from what a package happens to import. A derived region would
relicense a directory the moment somebody added an import — silently, in the
direction that gives code away. Declared, the same import fails the gate.

**Test binaries may cross the boundary.** Two MIT directories have test files
that import AGPL packages — `benchmarks/tpch` (the two-path invariance
corpus, which runs every query through both engines by definition) and
`internal/cli` (two config gates that assert a `serve` configuration reaches
the HTTP server's planner). Nothing shipped links a test binary, so the
combination is never distributed; each crossing is nevertheless declared in
`regions.go` with its reason, and an undeclared one fails.

## The commercial license

If the AGPL does not fit — you want to run a Wadjet cluster as part of a
service and not publish your modifications — a commercial license for the
distributed engine is available. Contact derekmwright@gmail.com.

The MIT half needs no such arrangement: embed it, ship it, modify it, keep
your changes.

## Why this is possible

Contributions are made under the [CLA](CLA.md), whose §3 records that the
maintainer may license the work, including contributions, under multiple
licenses. Relicensing part of the tree from AGPL-3.0 to MIT is exactly that
right being used, in the direction that gives users more.

Contributions remain accepted under the same CLA, whichever region they land
in; see [CONTRIBUTING.md](CONTRIBUTING.md).
