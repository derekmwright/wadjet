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
contributor for CLA or licensing purposes. All rights in this repository
vest in Carolina IT Consulting LLC per the [license](LICENSE).

## Reporting Issues

Use the [issue templates](https://github.com/citc-tech/wadjet/issues/new/choose) for bug reports and feature requests. Include reproduction steps and expected vs actual behavior.

## Questions?

Open a [discussion](https://github.com/citc-tech/wadjet/discussions) or reach out via the issue tracker.
