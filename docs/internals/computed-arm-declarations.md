# Computed arm declarations

Source: internal/planner/physical/join_input_projection.go — absorbComputedSubqueryProjection (declaration inference), moved 2026-09-11 (#1026)

```go
		// The computed column exists nowhere in the catalog, so its declared
		// type IS its runtime type — the worker builds the output vector
		// from it (#333), and a DECIMAL's (p,s) rides along or the vector
		// comes out at scale 0 (ADR-0024 item 2).
		//
		// Over a WINDOW SLOT there is no declaration to read: the slot is
		// not a catalog column and WindowColSpec carries a bare TypeID with
		// no (p,s), so inferring here answers the FLOAT fallback and would
		// render a DECIMAL sum at the wrong scale. Leave it unknown and let
		// exec.Project take the type from the vector it computes, the same
		// treatment a passthrough gets.
		// The declaration is read against the schema the expression NOW
		// names. A respelled reference reads the SOURCE column, which the
		// rename's own output schema does not declare — inferring against
		// that answered the FLOAT fallback and the fragment then tried to
		// store a DECIMAL's rendering into a float vector. Same repair as
		// attachScanSelectProjections' #387 branch, and the same helpers,
		// with the FILTER nodes between the Projects stripped: neither emits
		// a stage and the substitution walked through both.
		//
		// Over a WINDOW SLOT there is no catalog column to read at all, and
		// leaving the declaration unknown is not an option either — a
		// projection whose type the plan does not state answers NULL to the
		// AGGREGATE above it (`SUM(c.dv)` came back NULL and its HAVING
		// admitted no row) even where the same column PROJECTS correctly.
		// windowSpecOutputType is the stage's own answer for the slot,
		// DECIMAL (p,s) included, so the slot is declared here exactly as the
		// window stage declares it.
```
