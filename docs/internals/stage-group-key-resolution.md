# Stage group key resolution

Source: internal/planner/physical/group_key_resolution.go — resolveStageGroupKeys, moved 2026-09-11 (#1026)

```go
// resolveStageGroupKeys settles the RESOLUTION spelling of every GROUP BY key
// that names a derived table's COMPUTED alias, against what the producing
// fragment really emits.
//
// It runs at the END of planning for the reason `resolveFilterAliasSpelling`
// and `resolveDerivedAliasSortKeys` do: the two candidate spellings for such a
// key are the ALIAS and the expression that DEFINES it, and which one a
// fragment carries is decided by `attachScanSelectProjections` and
// `absorbWindowArmProjection`, which run after `walkStages` emits the stage.
// ADR-0026 §4a records three attempts to infer it from NODE KINDS at emission
// time, each wrong in a different direction; the answer is not a property of
// the logical plan at all.
//
// The rules, in order, and each is a statement the stream model can make:
//
// EVERY rule asks WHICH ARM first. The key names a derived table, that table
// is one arm of the join, and a column of the same name on another arm is a
// different value — so a candidate is only a candidate if it came from the arm
// the key names. `keyArmConstraint` answers that from the model's per-column
// arm: the build alias the join declares, or "" for the probe side.
//
// Skipping it is a silent wrong answer and not a missed optimisation. With the
// key naming a build arm whose own inner ORDER BY / LIMIT stopped
// `attachScanSelectProjections` from materialising the alias, the bare column
// of that name in the stream is the PROBE's, and the fragment then groups by a
// different table's value under the key's name (#781's R6/R8 cell — four
// shapes answering `x.a * 3` where the key is `z.a * 3`).
//
//  1. the stream spells the alias EXACTLY (`y.w`), because the join qualified
//     this arm's duplicate column — resolve by that name;
//  2. the stream carries exactly ONE BARE column of the alias's bare name
//     FROM THE KEY'S ARM, some fragment COMPUTED it, and no arm's column of
//     that name was dropped — resolve by the bare name;
//  3. no bare one, and exactly ONE QUALIFIED column of that bare name from
//     that arm, whose qualifier is the alias's own or the key was written
//     bare — resolve by the qualified name. This is the runtime's own
//     qualified↔bare fallback, decided where the key is decided rather than
//     left to a lookup;
//  4. the key's ARM carries every column the DEFINITION reads — resolve by
//     the definition RE-SPELLED into the spellings that arm's columns have in
//     the stream (`a * 3` becomes `z.a * 3` where the join qualified z's
//     copy), which the fragment materializes into a hidden slot;
//  5. none of the above: the arm carries the value nowhere. REFUSED with the
//     arm named, and the coordinator answers the query on its local pipeline,
//     where the derived table's Project is a real operator and the alias is a
//     real column.
//
// Rule 2's MATERIALIZED test is what keeps it off the shape that killed the
// last attempt: `(SELECT id, SUM(id) OVER () + 0 AS g FROM collslot) x GROUP BY
// g` puts a window alias over a table that has its own `g`, and the stream
// carries that base column under the same name. Nothing computed it, so rule 2
// declines and rule 4 answers `__win_0 + 0` — which is what the DAG has always
// evaluated there, correctly.
```
