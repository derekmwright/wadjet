# Pipeline delete marker ownership

Source: internal/coordinator/coordinator.go — Coordinator.SubmitSQL, moved 2026-09-11 (#1026)

No withQueryDeleteMarkers stamp here, unlike executeStageDAG (#491).
physStages was already overwritten above (:2918/:2927) with a
synthetic single "pipeline" stage that carries no ScanDeletes, so a
stamp built from it would always be empty — but that emptiness is
also the right answer, not just a consequence of the reassignment:
every TaskTypePipeline task (the only kind createPipelineTasks and
preScanBuildTables ever produce) re-parses SQLText and re-plans on
the WORKER against a live catalog (executor.go's executePipeline →
planner.Plan, not PlanDistributed), and that planner's buildScan
goes through the same newScanner/catalogScanSource the embedded
single-process engine uses — which reads the manifest's
DeleteMarkers itself at scan Init (engine/scan/scanner.go). A
probe-split task's ScanFileFilter only narrows which of those
catalog-read files a scan opens; it does not change how deletes are
applied. And the only other file lists a pipeline task carries
(PreScannedInputs, PreComputedAggregates' CacheFiles) are never base
parquet — they are the query-scoped .wshf output of an earlier
pipeline sub-query that already went through this same live-catalog
scan when IT was written (see build_cache.go's preScanOneTable and
aggregate_shuffle.go's preComputeDerivedAggregate, both of which
dispatch a TaskTypePipeline task and inherit the same guarantee).
So this path was already correct for #491 before that fix landed,
through a different mechanism than the DAG's wire-level markers;
TestDistributedScanHonorsDeleteMarkersOnThePipelinePath (this
package) asserts it end-to-end. See #491 discussion for the
evidence trail (worker plumbing that would have consumed a stamp
here — executor.go's PreScannedInputs/PreComputedAggregates
SetDeleteMarkers calls — was removed for being provably dead by the
same argument, not merely unreached).
