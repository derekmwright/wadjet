# Kernel like type and rendering boundary

Source: internal/engine/exec/kernel/compare.go — func ResolveLikeFilterKernel(typ batch.TypeID, pattern string, negate bool) FilterKernel {, moved 2026-09-11 (#1026)

ResolveLikeFilterKernel creates a FilterKernel for SQL LIKE pattern
matching against a column of the given type. Converts SQL LIKE patterns
(% and _) to optimized matching functions.

The column's underlying storage is not always TEXT in BytesData: TypeIPv4/
TypeMAC/TypePort/TypeProtocol box as Int64Data/Int32Data, and TypeIPv6/
TypeUUID box as BytesData but hold the address's RAW binary form, not the
human-readable text a LIKE pattern is written against. This used to be a
single BytesData.UnsafeStringValue call with no type check at all —
indexing an empty backing store for the Int64Data/Int32Data types (a
process-killing panic, since it is not the one deliberate FatalEvalPanic
shape recover() converts back into a query error) and matching nothing for
IPv6/UUID (their raw bytes never contain the pattern's text) (#497).
likeTextRenderer resolves the row->text function once per column, the same
per-type-dispatch-once discipline ResolveFilterKernel already follows, so
the inner loop has no per-row type switch.

nil for the four container types (#522): PostgreSQL has no `~~` operator
for any composite or array type (verified live: `ARRAY[1,2,3] LIKE 'x'`
raises "operator does not exist: integer[] ~~ unknown", SQLSTATE 42883),
and there is no established text form for a ROW/ARRAY/MAP/VECTOR value
this engine has committed to anywhere else — the old default arm's
`fmt.Sprint(Vector.GetValue(i))` (`[1 2 3]`, `map[k0:0]`) was never a
contract, just what happened to fall out of not refusing. The caller
(exec.LikeFilter) turns a nil kernel into that same 42883, the way
KernelFilter already turns decimalConstError/networkConstError into a
query error for a different type family.
