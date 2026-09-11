# Parquet nested leaf validation

Source: internal/storage/parquet/date_parse.go — func validateNestedLeaf(col Column, val any) error {, moved 2026-09-11 (#1026)

validateNestedLeaf is the per-leaf half of ValidateNestedLeaves: one value
against one primitive declaration.

It asks CheckLeafBox, which is the same function the FLAT path asks and the
same sequence of questions decomposeLeaf itself asks — so a leaf inside a
container is held to exactly the rule a top-level column of that type is
held to.

It used to be a four-case switch — DATE, TIMESTAMP, DURATION, DECIMAL — with
`return nil` for everything else, so a container leaf got no range check, no
box check and no VECTOR width check. Measured over 28 column shapes x 52
boxes, that was 19 values the ingest door ADMITTED and the writer refuses,
and every one of them was a container:

	ARRAY(INT32)     <- []any{int64(3000000000)}   writer: 22003 out of range
	ROW{f INT32}     <- {"f": int64(3000000000)}   writer: 22003
	MAP<STRING,INT32><- {"k": int64(3000000000)}   writer: 22003
	ARRAY(VECTOR(2)) <- []any{[]float32{1}}        writer: 1 component, want 2
	ARRAY(INT64)     <- []any{[]float32{1}}        writer: 42804 wrong box

The cost is the one this whole boundary exists to prevent, and it was
measured: 99 good rows plus one such row through ingest.Ingester lost ALL 99
at the flush, with the error naming `column "element", row 99 of this write`
inside a partition flush rather than naming the INSERT that carried it
(round-2 review B1).
