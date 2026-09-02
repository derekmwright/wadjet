package physical

import (
	"errors"
)

// ErrGroupKeyDistributed marks a plan the stage DAG refuses because the value
// a GROUP BY key names reaches no fragment that could compute it.
//
// A key has TWO names on the DAG (ADR-0026 §2): the PUBLISHED name every
// consumer above the aggregate reads (`Stage.GroupByCols`), and the RESOLUTION
// spelling the fragment that computes the key looks up in its own input
// (`Stage.GroupByResolve`). They are the same string for every ordinary
// `GROUP BY c` and every ordinary `GROUP BY c + 1`. They are different strings
// whenever the key names a derived table's alias — a join's stream carries `w`
// where the query wrote `x.w`, and `y.w` where the join qualified a duplicate,
// and the defining expression `a * 3` names a column the join does not carry
// at all.
//
// While the two shared one field, four classes of shape were wrong or refused,
// and this error carried all of them. Each is now answered by construction:
//
//   - a key an aggregate DIRECTLY BELOW already publishes (`SELECT DISTINCT
//     g + 1 AS k … GROUP BY g + 1`) resolves by that published COLUMN, not by
//     re-deriving `g + 1` against a schema that no longer has `g`;
//   - a derived key whose published name an aggregate output also answers to
//     resolves by its hidden `__gb_expr_N` slot and publishes under its
//     canonical text, and the merge above it addresses the aggregates by
//     ORDINAL (`mergeByPosition`) rather than by that shared name;
//   - a key naming a derived table's computed alias — window-wrapped or not —
//     resolves against what the producing fragment really emits, decided after
//     the projection passes by `resolveStageGroupKeys`;
//   - the merge boundary needs no agreement at all: a merge-mode aggregate
//     reads a partial's OUTPUT, where every key is already a column under its
//     published name, so it carries no resolution list (#794).
//
// What remains refused is two classes, both stated by `resolveStageGroupKeys`
// and neither inferred from a node kind:
//
//   - a plan in which NOTHING emits the value. A derived arm whose inner
//     ORDER BY / LIMIT stopped `attachScanSelectProjections` from
//     materializing its alias, read through a join whose exchange manifest
//     ships neither the alias nor the expression's columns — the stream model
//     states it exactly and the error carries the columns the stream does
//     have;
//   - a key whose expression contains an AGGREGATE or a WINDOW call, which a
//     pre-aggregate PROJECTION cannot evaluate at all: the value that call
//     names was computed by the operator below and published under a slot the
//     expression does not spell. `SELECT DISTINCT g, COUNT(*) + 0 AS w …
//     GROUP BY g` is where such a key comes from — the DISTINCT lowering makes
//     every SELECT item a key, so the CALL becomes a key expression. Both of
//     that key's names are that text and they AGREE; there is nothing for the
//     carrier to separate, and the repair belongs to the lowering.
//
// The refusal is a HANDOFF and not the query's outcome: `Coordinator.
// ExecuteSQL` routes it to the coordinator-local single-process pipeline, where
// the derived table's Project is a real operator and the alias is a real
// column, and the query answers PostgreSQL's rows. It shares that route with
// `ErrDistinctDistributed` and `ErrGroupingSetsDistributed`.
//
// Its worst case is NOT "a slow correct answer": `runRefusedLocal` runs under a
// budget of 8× `localFastPathBytes`, so a routed query carrying a JOIN under a
// small `--local-fastpath-bytes` can fail LOUDLY where the DAG would have
// completed. Every shape that reaches it today is base-WRONG → routed-right,
// which is an improvement in kind; the residual risk is a base-RIGHT shape
// routing into that budget, which nothing in the corpus produces.
var ErrGroupKeyDistributed = errors.New(
	"this GROUP BY key needs a published name the stage cannot carry")
