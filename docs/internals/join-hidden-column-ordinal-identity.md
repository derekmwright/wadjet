# Join hidden column ordinal identity

Source: internal/engine/exec/join.go — HashJoinProbe.OutputExcludeProbe / OutputExcludeBuild, moved 2026-09-11 (#1026)

OutputExcludeProbe and OutputExcludeBuild are the columns this join
materialized FOR ITSELF and must not publish, whatever a consumer asks
for — a decorrelated LATERAL's correlation key. They are keyed by the
column's ORDINAL IN ITS OWN SIDE's batch, which is the identity a NAME
cannot be:

  - reading is not minting, so a table may already STORE a column
    called `__key_0` (ADR-0012), and excluding by name dropped the
    USER's column — `SELECT o.__key_0` read NULL where PostgreSQL reads
    its values;
  - narrowing that to "a name that is also a JOIN KEY of its own side"
    is defeated by the query that CORRELATES ON the stored column,
    which is exactly when it is a key.

The value is the name the PLANNER expects at that ordinal, and it is a
SAFETY CHECK rather than the identity: when the plan's model of a
side's emitted order disagrees with the runtime, the column is KEPT.
An extra column is a divergence a gate sees; a dropped one is a user's
data gone.

Set by the planner from logical.Node.HiddenJoinCols, whose ordinals are
computed against the model of the side that is actually in force — the
logical subtree on the single-process path, the STAGE's stream on the
distributed one, where a Project emits no stage.
