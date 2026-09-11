# Set operation decimal target

Source: internal/planner/physical/set_op_decimal.go — setOpDecimalTarget, moved 2026-09-11 (#1026)

```go
// setOpDecimalTarget is the DECIMAL(p,s) a set operation's arms must all
// carry when their common type is DECIMAL.
//
//	scale     = max over the arms
//	precision = max over the arms of (precision - scale), plus that scale
//
// The scale is the maximum because that is the only choice that moves no
// value: a narrower one would DROP digits the wider arm holds, which is the
// truncating half of #533 (`UNION` emitted 12.75 twice for 12.7500 and
// 12.7501). Precision is then reconstructed from the widest INTEGER part
// rather than taken as max(precision), because max(precision) is not a bound
// on the widened values — DECIMAL(18,2) alongside DECIMAL(9,4) needs 16
// integer digits at scale 4, i.e. 20, where max(precision) would declare 18
// and hand the parquet writer a leaf too small for the value (ADR-0018 §4's
// encoding rule keys off precision). For every pair whose integer parts are
// ordered the same way as their precisions — which is every case the fixtures
// and the corpus reach, (9,2)/(18,4)/(38,10) included — the two rules agree,
// so this stays in step with the single-process path's own widening (#532).
//
// The result is capped at the carrier's full width (ADR-0012 item 9's SUM
// rule: 38 digits is what an Int128 holds). A value that has no Int128 at the
// output scale is then an ERROR at the moment of coercion, never a wrapped
// number — exec.DecimalCoerce raises it.
//
// ok=false means an arm's (p,s) could not be resolved — a computed DECIMAL
// expression carries none, and declaredProjectionDecimal declines on those
// for the same reason. The caller REFUSES the query then, naming the column
// (#551): leaving every arm as written was the pre-#533 behaviour and it is a
// silently wrong answer, because each arm writes its own .wshf at its own
// scale and the stage that reads several of them takes the first header's.
// The shuffle writer's own scale check (internal/worker/shuffle_format.go)
// sees only a SINGLE writer handed two scales and cannot catch that shape.
```
