# Merge exposed name collision

Source: wadjet/dml.go — func mergeExposedNames(info *plansql.MergeInfo) (target, source string, err error) {, moved 2026-09-11 (#1026)

mergeExposedNames returns the names a MERGE's ON condition and its SET /
VALUES expressions resolve against — the alias where one is written, the
relation's own name otherwise — and refuses the statement when they are the
same name.

PostgreSQL: 42712, `name "t" specified more than once`, DETAIL "The name is
used both as MERGE target table and data source", raised in
transformMergeStmt BEFORE anything is written. Wadjet answered `MERGE 1` and
WROTE, and the self-merge spelling `MERGE INTO t USING t ON t.id = t.id`
EMPTIED the table where PostgreSQL refuses the statement (#837).

The mechanism is buildMergedRow: it writes both relations' columns into one
map under `exposedName + "." + column`, so when the two exposed names
collide the source's values overwrite the target's at every qualified key.
`ON t.id = t.id` then compares a source column with itself — a tautology
that matches every pair of rows — instead of resolving to the ambiguity
PostgreSQL reports. Same family as #689's `sourceNamed`.

The rule is over EXPOSED names, which is not the same as "the source is not
the target table". `MERGE INTO t AS x USING t AS y` is legal and wadjet
already answers it correctly, and so is `MERGE INTO t AS x USING s AS t` —
PostgreSQL accepts both, measured.
