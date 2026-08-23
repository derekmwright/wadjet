# Contributing to Wadjet

Thank you for your interest in contributing to Wadjet! This document covers the process for contributing to the project.

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/YOUR-USERNAME/wadjet.git`
3. Create a branch: `git checkout -b feat/your-feature`
4. Make your changes
5. Push and open a pull request

## Development Setup

```bash
# Build
go build -o wadjet ./cmd/wadjet

# Run tests
go test ./internal/... -timeout 5m

# Run TPC-H correctness suite
go test -v -run TestTPCHQueries ./benchmarks/tpch/ -timeout 5m
```

## Commit Convention

We use [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <description>
```

**Types:** `feat`, `fix`, `perf`, `refactor`, `test`, `docs`, `build`, `ci`, `chore`

**Scopes:** `planner`, `engine`, `exec`, `expr`, `batch`, `scan`, `storage`, `parquet`, `catalog`, `pgwire`, `auth`, `worker`, `coordinator`, `ingest`, `iceberg`, `tpch`

Examples:
```
feat(expr): add format_bytes and parse_bytes scalar functions
fix(planner): resolve ORDER BY aggregate to correct output column
perf(exec): use typed sort kernels instead of interface comparison
```

## Testing Requirements

- **Bug fixes** must include a regression test that fails without the fix.
- **New features** must include unit tests covering expected behavior and edge cases.
- **Performance changes** must include a TPC-H SF1 benchmark comparison (before/after).

Run the full test suite before submitting:

```bash
go test ./internal/... -timeout 5m
go test -run TestTPCHQueries ./benchmarks/tpch/ -timeout 5m
```

## Code Style

- Follow existing patterns in the codebase
- Wrap errors with context: `fmt.Errorf("building aggregate: %w", err)`
- Use typed kernels — resolve type once per batch, not per row
- Prefer selection vectors over copying for filter operations
- Don't over-engineer — keep changes minimal and focused

## Pull Request Process

1. Ensure all tests pass
2. Use a conventional commit message as the PR title
3. Describe the change, root cause (for fixes), and test plan in the PR body
4. A maintainer will review and may request changes
5. Once approved, a maintainer will merge

## Contributor License Agreement

By submitting a contribution, you agree to the [CLA](CLA.md). Include the following sign-off in your first commit:

```
Signed-off-by: Your Name <your.email@example.com>
```

## AI-Assisted Development

AI coding tools are used in this project as tools, under human direction
and review. The human committer is the sole author of each contribution
and the sole signatory under the [CLA](CLA.md); an AI tool is not an
author, co-author, or contributor and cannot sign the CLA or grant any
license.

Commits made before 2026-07-27 may carry a
`Co-Authored-By: Claude <noreply@anthropic.com>` (or similar) trailer.
That trailer was an automated default of the commit tooling. It asserts
no authorship claim by any non-human entity, and it does not name a
contributor for CLA or licensing purposes. The project is distributed
under the terms in [LICENSE](LICENSE); contributions are licensed to
the maintainer, Derek Wright, under the [CLA](CLA.md).

## Releases

Tags follow `vMAJOR.MINOR.PATCH-<arc-name>`, where the suffix names the
headline work of the release (e.g. `v0.11.0-parallelism-cascade`).

Checklist for every release:

1. **Refresh the README benchmark numbers.** The `## Benchmarks` section
   must reflect the most recent *official-methodology* runs — TPC-H SF100
   distributed (coordinator + 3 workers, per-query table, suite totals)
   and ClickBench c6a.4xlarge (cold/hot per-query table, suite sums) —
   including the hardware specs and the date of each run. If an arc since
   the last release changed performance, re-run before tagging; never
   ship a release whose README describes a previous release's numbers.
2. **Gates green at the tagged commit**: `go build ./...`, unit suites
   (`go test ./internal/... ./wadjet/`), TPC-H SF0.01 correctness
   (`go test -run TestTPCHQueries ./benchmarks/tpch/`), and ClickBench
   correctness vs DuckDB when a hits part is staged.
3. **Release notes** summarize the arc since the previous tag: headline
   perf/feature work first, then correctness fixes, with benchmark
   deltas stated as before → after.
4. **Note any change to the exchange contract in the release notes.** The
   shuffle wire format and the partition ASSIGNMENT that goes with it
   (`hashRowsIntoPartitions`, the partition count derivation, the WSHF field
   order) must be identical across every process in a cluster: a stage whose
   tasks run a mix of binaries across such a change does not fail, it routes
   rows to partitions nobody reads and answers short. A release that touches
   any of them says so, and says that the upgrade is WHOLESALE — coordinator
   and all workers together, never rolling. See ADR-0010's consequences.
5. Tag with `git tag -s` or an annotated tag from the release commit and
   create the GitHub release from it.

## Reporting Issues

Use the [issue templates](https://github.com/derekmwright/wadjet/issues/new/choose) for bug reports and feature requests. Include reproduction steps and expected vs actual behavior.

## Questions?

Open a [discussion](https://github.com/derekmwright/wadjet/discussions) or reach out via the issue tracker.
