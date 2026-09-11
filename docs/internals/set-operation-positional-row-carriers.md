# Set operation positional row carriers

Source: internal/planner/physical/set_op_plan.go — setOpSourceAdapter.Next (positional coercion), moved 2026-09-11 (#1026)

```go
		// The boxes are not uniform across types — a DECIMAL is its rendered
		// TEXT, an integer a raw int64, a float a float64 — so a widened
		// column needs each arm's box MOVED into the shape the unified column
		// reads, not merely relabelled. coerceSetOpArmRows does that for every
		// rung of the ladder before the arms meet, so both the dedup key and
		// FromRows read one shape per column — and ERRORS on a value that does
		// not fit the unified DECIMAL, the same overflow the stage DAG raises
		// (exec.coerceDecimalVector), rather than saturating silently. The
		// right arm is coerced against its OWN schema, before alignSetOpRows
		// re-keys it to the result names.
		//
		// POSITIONALLY, from here to the batch. SQL says the arms of a set
		// operation correspond by POSITION and a result may legally carry two
		// output columns of the same NAME — `SELECT n_name AS u, n_comment AS
		// u FROM nation UNION ALL …` is two columns called `u` in PostgreSQL
		// too. A map keyed by name holds ONE of them, so both output columns
		// came back carrying the SECOND source column's value: every row
		// wrong, no error, and only on this path — the stage DAG answers it
		// correctly, which is what isolated the collapse as the cause (#556,
		// and #844's UNION ALL branch, which is the same map).
		//
		// The rows keep their map form — every helper below reads it, and the
		// DECIMAL, dedup and overflow rules those helpers encode are not what
		// is wrong here — but their KEYS become slot positions, which are
		// addresses. The schemas are renamed to match for the duration and
		// the result batch is renamed back at the end, so nothing outside
		// this function sees a slot name.
```
