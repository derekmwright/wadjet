# Dynamic filter attach on arrival

Source: internal/planner/physical/dynamic_filter_attach.go — applyAttachOnArrival, moved 2026-09-11 (#1026)

```go
// Attach-on-arrival normalization for dynamic-filter consume edges
// (docs/design/attach-on-arrival-dynamic-filters.md).
//
// Every dyn-filter marking pass wires its consume as a hard stat-dep: the
// consumer's dispatch blocks on the emitter stage completing and merging.
// That barrier is coverage-maximizing but not required for correctness —
// the blooms are drop-only and every false positive is re-verified by the
// downstream join. For edges where the emitter chain is scan-only, the
// barrier costs 2-4s of start serialization per marked query (per-stage
// dispatch round-trips + partial merge) while buying a few percent of head
// filtering coverage. This pass converts exactly those edges to
// non-blocking "attach-on-arrival" consumes.
//
// The mode decision is structural — no thresholds, no query names:
//
//  1. The consumer must not itself emit dynamic filters. A re-emitting
//     consumer (cascade mid-scan B0) derives its emitted key set from its
//     post-consume output; a late-attached consume would silently widen
//     the downstream filter (hop-B would ship every supplier key scanned
//     before nation's bloom landed), destroying the cascade transitively.
//  2. The emitter must be a leaf scan stage. Join-fed emitters (semi/anti
//     build filters) complete only after a multi-stage probe chain; the
//     consumer would finish mostly unfiltered and the downstream shuffle/
//     build volume would revert to raw size. Those edges keep the barrier.
//  3. The consumer must dispatch real fragment tasks via the scan-filter
//     path (pushed filters / projections / fused exchange, and NOT fused
//     scan-aggregate — dispatchScanAggregateStage carries no dyn-filter
//     plumbing). Pass-through scans have no runtime pipeline to attach
//     into.
```
