# internal/coordinator

AGPL-3.0 (LICENSING.md) — the query coordinator (plan, dispatch, merge) and
its stage-DAG planner (`internal/coordinator/dagplan/`, its own declared
license region in `tools/licensecheck/regions.go`). Moved out of the root
`CLAUDE.md` (arc TK, token-savings reorg, 2026-09-23) — this was the
Distribution section's "Internals map" bullet, verbatim, unchanged.

- **Internals map**: `docs/internals/native-dag-execution.md` — file-anchored map of the native-DAG path (two coordinator entry paths, `walkStages` per-node stage emission, Stage→fragment conversion, the distribution-property/shuffle system, and inspection recipes). Start here before navigating coordinator/planner/worker distribution code.
