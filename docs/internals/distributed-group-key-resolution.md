# Distributed group key resolution

Source: internal/distributed/messages.go — GroupByResolve []GroupKeyResolveSpec `json:"group_by_resolve,omitempty"`, moved 2026-09-11 (#1026)

GroupByResolve is index-aligned with GroupByCols: what THIS fragment
resolves each key by, against the columns its own input carries. It is
a different string from the published name whenever the key names a
derived table's alias — a join's stream carries `w` where the query
wrote `x.w`, and `y.w` where the join qualified a duplicate — and it is
the hidden-slot marker (Computed) for a key the fragment must
materialize.

The worker decides NOTHING about a key by parsing text. It used to:
`derivedGroupKeys` re-derived "is this key derived?" from the spelling,
which cannot say (`GROUP BY "g + 1"` names a column and `GROUP BY g + 1`
is arithmetic, and both are recorded as `g + 1`), so a key whose two
names differ collapsed the whole table into one NULL group (ADR-0026
§2/§2c, #736, #781, #794).

Absent — an older coordinator — the worker falls back to that parse,
which is exactly the behaviour this field replaces, for the reason
GroupByTypes tolerates the same absence. A NEWER coordinator talking to
an older worker degrades the same way and no further: GroupByCols is
the published name, which for every shape the old parse got right is
also the spelling it computed from.
