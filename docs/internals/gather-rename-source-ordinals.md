# Gather rename source ordinals

Source: internal/coordinator/dag_merge.go — renameSourceIndices, moved 2026-09-11 (#1026)

renameSourceIndices resolves every rename to the source column it reads,
as INDICES, so that two renames can never be handed the same column.

A name is not a key here. The SELECT list may legally carry two output
columns of one name — PostgreSQL answers `SELECT upper(a), upper(b)` with
two columns called `upper`, and #513 made this engine agree — and when the
producing fragment MATERIALIZES that list (attachScanSelectProjections does,
whenever an ORDER BY term forces a projection below the sort) the gather is
handed two columns both named `upper` and two renames both spelled
From:"upper". Resolving each by name gave both column 0, so the second
output carried the first one's values: 25 rows of `ALGERIA | ALGERIA` where
PostgreSQL answers `ALGERIA | NATION ALGERIA COMMENT`.

Duplicates are assigned ORDINALLY — the k-th rename spelled X takes the
k-th column spelled X — and the correspondence is exact because the
matching is NAME-SCOPED. The two lists are not the same list: the rename
list is the VISIBLE select items (extractOutputRenames walks
VisibleProjections) while the producer's projection carries the hidden ones
too (attachScanSelectProjections walks Projections, hidden ORDER BY terms
included). What makes the k-th ↔ k-th correspondence hold anyway is that
only columns SPELLED LIKE THE GROUP are counted, and a hidden term is named
__sortkey_N — a spelling no user alias can collide with — so it can neither
join a group nor shift one. Within one name, both lists are that name's
select items in select order.

A group of N renames over M columns of that name resolves by counting, and
the three cases are different questions rather than degrees of confidence:

	M >= N  ordinal. Each output has its own column, which is what the
	        producer emits when it MATERIALIZED the select list.
	M == 1  every rename reads it. The producer did NOT materialize, so the
	        streams carry SOURCE columns and N select items reading ONE
	        source share ONE column — `SELECT DISTINCT k AS u, k AS u` really
	        is that column twice, and PostgreSQL answers it that way.
	else    NOTHING (-1). 1 < M < N means the producer emitted several
	        columns of the name but fewer than the outputs asking for it, and
	        no counting rule maps them; handing each the same first match is
	        precisely the defect this function exists to end. -1 makes the
	        caller degrade to a rename-only pass, which returns a WIDER
	        result than the select list — visibly wrong rather than silently
	        wrong, and the degradation the projection has always taken when a
	        source does not resolve.
