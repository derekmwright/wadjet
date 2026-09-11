# Dimension bloom cascade

Source: internal/planner/physical/dimension_cascade.go — markDimensionCascade, moved 2026-09-11 (#1026)

```go
// markDimensionCascade wires the two-hop dimension bloom cascade
// (docs/design/dimension-cascade.md):
//
//	D (tiny filtered dimension scan, e.g. nation σ(name))
//	  --hop A: emit D's join key--> B0 (mid dimension scan, e.g. supplier)
//	  --hop B: emit J's build key--> F (fact probe scan, e.g. lineitem l1)
//
// for an inner-form join stage J whose PRIMARY build is B0's scan and
// which carries a chained/fused join against D keyed on a column that
// ORIGINATES from B0 (BuildColOrigins / B0 column membership — the
// s_nationkey provenance). Hop A's consume forces B0's scan to dispatch
// (EmitDynamicFilters routes it through dispatchScanFilterStage), where
// the row-level bloom source applies the consume and the AtOutput emit
// then observes POST-consume rows — so hop B ships exactly the surviving
// build keys. Classic transitive semijoin reduction; safe for J's probe
// because an inner-form probe row without a matching build key produces
// nothing.
//
// Join-type eligibility mirrors the legacy scan→scan pass: "inner",
// "semi", and "left" (build feeds the inner side under probe-left
// convention). Outer-build shapes are excluded.
```
