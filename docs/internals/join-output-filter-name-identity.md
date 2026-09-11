# Join output filter name identity

Source: internal/engine/exec/join.go — outputFilterMatcher, moved 2026-09-11 (#1026)

outputFilterMatcher answers "does the consumer need this join output column"
under the one identity a name has: a column matches when its NAME FOLDS to a
name the filter asks for and the two agree on the RELATION — byte-exact when
both spell one, and either side may leave it off.

The three lists this replaces compared BYTES. That is right until two
relations of one join carry the same column name in different cases, which
they may: an unquoted reference folds to lower case and a delimited one does
not (#731), so `rvya("MixedCol")` joined to `rvyb(mixedcol)` publishes
`[k mixedcol rvya.k rvya.MixedCol]` — the join qualifies the colliding build
column by relation, which is ADR-0026's identity, (relation, folded name).
The consumer asks for the bare `mixedcol` (what the pruning pass records) or
for `rvya.mixedcol` (what the reference itself spells), and NEITHER matched
`rvya.MixedCol` byte for byte. The column was dropped, the reference above
fell back to the bare name, and `SELECT rvya.MixedCol FROM rvyb, rvya`
answered rvyb's 900 where PostgreSQL says 100 — a silent wrong answer, on
every arm, whenever the CamelCase relation was not written first.

The fold belongs here and not only in the reference because this list is the
one that decides whether the column EXISTS downstream: a reference cannot
resolve what the join did not ship. Keeping a column the filter did not name
exactly costs bytes, never an answer, so the qualifier is matched
permissively in both directions — the asymmetry expr.ResolveColumnRef and
exec.columnIndexFallback already resolve.
