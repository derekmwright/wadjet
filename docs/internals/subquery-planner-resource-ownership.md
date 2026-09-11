# Subquery planner resource ownership

Source: internal/planner/physical/subquery_pipeline.go — forSubquery, moved 2026-09-11 (#1026)

forSubquery returns a child planner for building and running ONE subquery
pipeline.

A correlated subquery is executed per row from the pipeline's parallel
worker goroutines (exec.Pipeline.runParallel), and each execution runs a
full physical build. Those builds must not share the parent's per-build
scratch, for two independent reasons:

  - It is a data race. scanCounter is a plain map written by buildScan, so
    concurrent builds crash the process with "fatal error: concurrent map
    writes" — an unrecoverable throw that takes down every other connection
    in server mode (issue #334).
  - It is wrong even when serialized. scanCounter numbers the scans of one
    build; letting the outer build's count leak in makes a subquery's first
    scan of customer resolve as alias "customer:1", so the ScanFileFilter /
    MaterializedInputs / StreamingSources lookups keyed by that alias miss.
    A mutex would hide the crash and keep the mis-keying.

So the child gets fresh build scratch, and shares everything that genuinely
belongs to the query: the catalog, the CTE definitions and their
materialized cache, the scan cache, the memory/spill resources, and all
configuration. Sharing those is what keeps one budget, one spill directory,
and one materialization of each CTE per query; a per-goroutine copy would
leak spill directories that Plan's Cleanup never sees.

The shared maps (cteCache, scanCache) are populated by Plan before execution
begins and are read-only from here on; scanCached carries its own mutex for
the concurrent-replay case.

The scan-alias injections (MaterializedInputs, StreamingSources,
ScanFileFilter) are dropped. They describe the ENCLOSING fragment's scans —
a worker's probe-split file slice, a scan-split pre-scan — keyed by that
plan's aliases. A subquery is its own query over the catalog and must see
the whole table; binding it to the fragment's file slice would answer it
from one worker's shard. Today they are missed only because the shared
counter happens to push the subquery's aliases past the injected keys, so
dropping them makes the existing behavior explicit rather than incidental.
