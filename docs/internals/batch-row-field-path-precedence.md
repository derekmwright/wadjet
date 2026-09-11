# Batch row field path precedence

Source: internal/engine/batch/schema.go — func (b *RecordBatch) RowFieldPath(name string) (parent, field int, ok bool) {, moved 2026-09-11 (#1026)

RowFieldPath answers ADR-0022's question for one dotted reference, and it
is the ONE place the question is asked: does the reference's QUALIFIER name
a ROW column of this batch that DECLARES the field? It returns the parent
column's index and the field's position among that container's children.

Every consumer of a column reference asks this BEFORE stripping the
qualifier, because stripping first answers with whatever OTHER relation in
the stream publishes a column of the FIELD's name:

	SELECT n.id, c_row.b FROM typemx_nested n JOIN decpair d ON n.id = d.id
	-- PostgreSQL 17 (spelled `(n.c_row).b`) answers the field: 11, NULL,
	--   NULL, 44, 55, 66, 77, 88, NULL. wadjet answered decpair.b's DECIMALs
	--   on all four arms, in silence (#769).

Four resolvers had to agree about which value `c_row.b` denotes — the
single-process evaluator (expr.ColRef), the stage DAG's projection
(exec.lazyFieldIdx), the DECLARATION half that types it
(exec.fieldPathColumn) and the vectorized filters' ROW delegation — and
each spelled the order for itself. That is the shape ADR-0022 was written
about, one level down: a field path LOOKS like a qualified column
reference, so every site invents the same three-way order and one of them
gets it wrong.

The container must DECLARE the field. Without that test the reorder would
capture an ordinary qualified reference whose qualifier happens to name a
ROW column of the stream, and a field path naming NO field would stop
answering the way it does today (#604).

The parent is looked up the way every other reference is: byte-exact under
the fold, then the ONE column spelled `<qualifier>.<name>` — a join
qualifies a colliding container, so `c_row.b` has to find `x.c_row`. Two
arms spelling it decline HERE, and this function declining is not by itself
the refusal: the caller's later branches would still bind one of them. What
makes the ambiguity loud is the BINDER, which raises PostgreSQL's own
`column reference "c_row" is ambiguous` (42702) at plan time when two of a
block's sources publish the container (physical.colScope.check).
