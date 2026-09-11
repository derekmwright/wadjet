# Nested declared merge key framing

Source: internal/engine/exec/sort.go — appendKeyValueWithMeta, moved 2026-09-11 (#1026)

appendKeyValueWithMeta is appendKeyValue with the value's DECLARED column
type available, so a CIDR value re-keys through kernel.CidrOrderKey even
nested inside an ARRAY, MAP or ROW — where appendKeyValue's plain `any`
switch has no type tag to tell a CIDR string from an ordinary one, the same
gap appendSerializedKey already closes for a bare top-level CIDR column.

Without this arm, GROUP BY arr_cidr agreed with the un-spilled columnar key
(appendColumnValue → appendListKey → appendNestedElem, which walks the real
*batch.Vector tree and re-keys every CIDR leaf already) only until a
cross-batch, cross-worker or spill-boundary MERGE went through this boxed
path instead: '10.0.0.1' and '10.0.0.1/32' inside the array serialized to
two different byte strings, so a k-way merge of otherwise-identical groups
answered two groups where the un-spilled path already answers one.

Every leaf type this does not name keeps appendKeyValue's existing
encoding exactly — this only intercepts CIDR and recurses into a
container's own element/field metadata to find one.

The recursion goes through appendKeyElemWithMeta, NOT back through this
function. A container's elements are framed (a kind tag, then a fixed-width
or length-prefixed payload) precisely because there is no separator down
there; recursing here instead wrote each element in the TOP-LEVEL encoding,
which for an int64 is bare decimal digits, and ARRAY[1,23] and ARRAY[12,3]
became the same key.
