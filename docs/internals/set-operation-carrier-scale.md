# Set operation carrier scale

Source: internal/planner/physical/output_declared_schema.go — emittedColDecimal (set-operation arm), moved 2026-09-11 (#1026)

```go
		// The (p,s) companion to emittedColTypes' set-operation arm, and
		// bound by the same rule the WIRE's reconciliation uses: a DECIMAL
		// result keeps a typmod only when every arm carries the SAME one
		// (setOpArmDecimalDisagreements, ADR-0012 item 12). An arm that
		// disagrees contributes nothing, which leaves the column DECIMAL with
		// no scale — exactly what the wire declares for it.
		//
		// Without it `SUM(v * 2) + 1` over a `UNION ALL` of DECIMAL columns
		// had a TYPE and no scale, so binOpDecimalType declined and the term
		// fell to float8 with the exact value rendered through a float64
		// (round-3 review P-B).
		//
		// It compared the arms' widths for AGREEMENT and left a disagreeing
		// column out, which is #884's other half: `DECIMAL(18,4) ∪
		// DECIMAL(20,6)` had no scale and fell to float8 for the same reason.
		// The (p,s) comes from setOpDeclaredOutputSchema now — batch.
		// DecimalCommon over the arms that carry a type, the same widening the
		// executed schema and the DAG's arm reconciliation already use — so
		// the CARRIER's scale is one answer computed once. Precision 0 is
		// still "unconstrained" and still contributes nothing, which is what
		// the wire declares for it (ADR-0012 item 12's recorded typmod
		// divergence: PostgreSQL's result is numeric with typmod -1 where a
		// wadjet DECIMAL vector has exactly one scale).
```
