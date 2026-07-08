// Package harness implements the distributed test harness used by
// cmd/tpch-harness. It orchestrates a multi-process wadjet cluster on the
// dev box (local mode) or drives a pre-existing cluster (golden mode),
// runs the TPC-H query suite plus synthetic micro-queries, captures
// structured measurements, and compares them against a calibrated baseline.
//
// The package is intentionally importable (not test-only) so the harness
// binary can use it directly. Helpers for setting up an embedded NATS,
// loading the catalog, and submitting queries are extracted from the
// existing distributed_tpch_test.go in internal/coordinator so there is
// exactly one implementation.
//
// See docs/archive/specs/2026-04-08-distributed-test-harness-design.md
// for the full design.
package harness
