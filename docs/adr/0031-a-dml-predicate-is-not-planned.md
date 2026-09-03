# ADR-0031: A DML predicate is compiled, not planned — and closing that needs a projectable row identity

Status: Accepted (2026-09-03, arc D3). Records the position and the DEFERRAL
of #688, with the structural design measured rather than sketched.

## Context

`DELETE`, `UPDATE` and `MERGE` do not go through the planner. Their `WHERE`
is compiled by `wadjet.BuildDMLPredicate` (`wadjet/dml.go`), which parses the
clause text, resolves its column names against the target's schema, and calls
`expr.Compile` **with a nil subquery runner**. Every planner-resident
guarantee is therefore absent on that door. Measured against PostgreSQL 17.11:

| statement | PostgreSQL 17 | wadjet |
|---|---|---|
| `DELETE FROM pr WHERE id IN (SELECT id FROM src)` | `DELETE 1` | `0A000` |
| `DELETE FROM pr WHERE EXISTS (SELECT 1 FROM src s WHERE s.id = pr.id)` | `DELETE 1` | `0A000` |
| `UPDATE pr SET n = 0 WHERE id IN (SELECT id FROM src)` | `UPDATE 1` | `0A000` |
| `DELETE FROM pr WHERE id NOT IN (SELECT id FROM src)` | `DELETE 2` | `0A000` |
| `DELETE FROM pr WHERE n < (SELECT max(n) FROM src)` | `DELETE 3` | `0A000` |
| `MERGE … WHEN MATCHED AND t.id IN (SELECT …) THEN DELETE` | — | `0A000` |

Every one is LOUD and writes nothing, so no answer is wrong; what is missing
is a legal and common capability. Five of them are pinned in the DML census
(`internal/server/pgwire/dml_census_test.go`, `bug: "#688"`) with
PostgreSQL's answer recorded beside the refusal.

Arc B deferred #688 under rule 11 because the BOUNDED fix — handing
`expr.CompileWithRunner` a `db.Query` closure, three lines — closes
`IN (SELECT …)`, `NOT IN (SELECT …)` and the scalar subquery while leaving
CORRELATED `EXISTS` refused, and correlated `EXISTS` is the shape #688's own
body names first. A fix that leaves the issue's headline shape pinned is not
shipped.

Arc D3 was asked to attempt the STRUCTURAL fix instead. This record is what
that attempt found.

## Decision

**The structural fix is to give the DML door a planner-produced set of
`(file, row-in-file)` identities — `SELECT <row identity> FROM t WHERE <pred>`
planned as an ordinary SELECT, the door consuming the result to mint its
delete markers — and it is BLOCKED on a capability this planner does not
have: a column the planner can PROJECT that is not in the table's schema.**

The design is right and it is not attemptable inside one issue's arc. #688
stays deferred, with the five census pins carrying PostgreSQL's answer, and
the work below is its own arc.

## Why the identity has to come from the scan

A merge-on-read delete marker names `(file path, file-absolute row index)`.
Both halves exist inside the scan and neither leaves it:

- `physical.rgUnit` (`internal/planner/physical/util.go:354`) carries
  `slot.entry.Path` and `rgRowOffset`, a per-row-group prefix sum that
  correctly advances past PRUNED row groups (`util.go:856`). It is read once,
  at `util.go:1015`, to build the selection vector that skips deleted rows.
- The distributed scan source gets the same number from
  `scan.RowGroupIter.RowOffset` (`internal/engine/scan/rowgroup_iter.go:232`),
  consumed once at `internal/worker/stream_source.go:472`.
- `batch.RecordBatch` (`internal/engine/batch/batch.go:14`) carries no file,
  no offset and no provenance of any kind.

`scan.DeleteSet`'s doc (`internal/engine/scan/delete_set.go:11`) states the
contract both implementations keep by hand. MERGE already carries the identity
the only way available today — `mergeTargetRow{row, file, pos}`
(`wadjet/dml.go`) — and its doc records what happens when the two ends
re-derive the position independently instead: 8 of 12 runs deleted the wrong
physical row (#676).

## Why it cannot be projected today

There is exactly one synthetic scan column in the engine and it is the wrong
shape:

`physical.RowLocColumn` = `"__row_loc"`
(`internal/planner/physical/topn_late_mat.go:33`) packs
`rgUnit ordinal << 32 | row-WITHIN-ROW-GROUP`
(`stampRowLoc`, `internal/planner/physical/util.go:1154`). Three things
disqualify it:

1. **It is not a durable identity.** The high bits index
   `scanSourceInner.rgUnits`, a list built fresh per scan-source instance
   after pruning; it resolves only in-process, through that live source. The
   low bits are row-within-row-GROUP, not row-within-file — the
   `rgRowOffset` that would make it file-absolute sits on the same struct and
   is deliberately not used.
2. **It is not plumbed through the planner.** It is switched on by a bare
   bool (`css.emitRowLoc`) and APPENDED after decode. It never appears in
   `RequiredColumns`, `Stage.Columns`, `Stage.OutputColumns` or any declared
   schema — it could not: `buildReadSchema` (`util.go:1282`) and
   `sanitizeScanNeeds` (`internal/planner/logical/optimizer.go:340`) both
   intersect requested names against the TABLE schema and would drop any name
   that is not in it.
3. **It is fenced off from every path a DML predicate must still work on.**
   `topn_late_mat.go:63` bails out when the plan has streaming sources,
   materialized inputs or a scan-file filter, and `plan.go:15556` fails loudly
   on anything but the row-group-parallel scan. A `DELETE` on a table with an
   `ARRAY` column, or under LIMIT pushdown, takes the lazy/nested fallback
   path — which is exactly where the guard refuses.

Every other "synthetic column" in the tree (`__win_N`, `__sortkey_N`,
`__gb_expr_N`, …; `internal/planner/sql/reserved_slots.go`) is materialized by
an operator ABOVE the scan from columns the scan produced. None is minted BY
the scan, and that is the missing mechanism.

`ctid` is not an escape hatch, and the codebase has already taken a position
against making it one: `internal/planner/physical/validate.go:1273` resolves
`ctid` to NULL rather than to a value this server invents, precisely because
"a fabricated ctid is not merely useless but dangerous — a client re-issuing
`DELETE … WHERE ctid = '(0,5)'` could address a row other than the one the
user saw."

## The work the structural fix needs, in dependency order

1. **Redefine the locator so it can name a file and a file-absolute row.**
   `RowLocColumn` / `stampRowLoc`, both refetch decoders. A `string` file path
   cannot ride in an `INT64` vector, so this needs a second column or a
   per-statement file-id side table that survives wherever the locator does.
2. **Establish the file-absolute row invariant on the fallback scan paths.**
   Two of the three scan implementations keep it by hand and a third would
   have to. See the lead recorded below: on the fallback path the invariant
   appears to hold only because nothing prunes there, which is a property no
   assertion states.
3. **Teach the planner to project a column that is not in any table schema.**
   `buildReadSchema`, `sanitizeScanNeeds`, `logical.Node.RequiredColumns`,
   `Stage.Columns` / `OutputColumns` / `ScanSchema`, `pruneScanOutputColumns`
   and the declared-schema passes all assume every column names something.
   This is the cross-cutting item and it is the reason for the deferral.
4. **Rewrite the DML door to consume a set rather than loop over files.**
   `deleteOnce`, `updateOnce`, `scanFileForDeletes`, `readParquetFile`,
   `MatchDMLRows` and `BuildDMLPredicate` all rest on the invariant that
   `readParquetFile` returns one batch per file whose index IS the
   file-absolute row index.
5. **Add a batch-level embedded query entry point.** `DB.Query` returns
   `[]map[string]any`; a `DELETE` matching ten million rows would box ten
   million maps, which is the memory shape `updateOnce`'s per-file streaming
   was restructured to avoid.

## An UNVERIFIED lead on the same invariant, recorded as a lead

Reading the fallback scan path suggests it applies delete markers with **no
row offset**: `plan.go:15850` indexes the delete set by the batch index `i`,
while its producer `readBatchDirect` (`util.go:149`, called at `plan.go:15832`
WITH `inner.scanPreds`) concatenates only NON-PRUNED row groups
(`util.go:186`, `:206`). If a row group is ever pruned on that path against a
table that has delete markers, `i` is a COMPACTED index and the wrong rows
would be skipped. `scan.Scanner` reads the same way (`scanner.go:485` over the
compacted concat at `scanner.go:452`).

**It did not reproduce.** The lazy path is taken when `rowLimit > 0` (LIMIT
pushdown) or the read schema is nested (`plan.go:15568`). Attempted over a
single file of three four-row row groups with `id = 10` (file-absolute row 9)
deleted:

	SELECT id FROM t ORDER BY id                 [1 2 3 4 5 6 7 8 9 11 12]
	SELECT id FROM t WHERE id > 8 ORDER BY id    [9 11 12]
	SELECT id FROM t WHERE id > 8 LIMIT 100      [9 11 12]
	SELECT id FROM t WHERE id > 8 LIMIT 2        [9 11]
	SELECT id FROM t WHERE id > 8 LIMIT 3        [9 11 12]
	SELECT id FROM t WHERE id >= 9 LIMIT 3       [9 11 12]
	SELECT id FROM t WHERE n > 80 LIMIT 3        [9 11 12]
	SELECT id FROM t LIMIT 100                   [1 2 3 4 5 6 7 8 9 11 12]

Every one right, id = 10 absent from all of them. The most likely reason is
that `scanPreds` is empty whenever `rowLimit > 0` — the sibling guard at
`plan.go:15564` refuses `rowPreds` on this path for the same class of reason —
so nothing is pruned and `i` IS file-absolute. On the nested arm
`readBatchViaRows` ignores the predicates entirely and reads every row, so `i`
is file-absolute there too.

This is recorded as a LEAD, not a defect: the two conditions may be
unreachable today, and if they are, the code is right by construction rather
than by an assertion — which is exactly the shape item 2 has to settle before
a projected identity can rest on it.

## Consequences

- #688 stays open with its five census pins. The pins carry PostgreSQL's
  answer, so the day the capability lands the census fails until they are
  deleted — which is the fix's proof.
- The bounded `CompileWithRunner` fix stays forbidden, and this record is why:
  it closes four of the five pinned shapes and leaves correlated `EXISTS`,
  which is the shape the issue was filed for.
- `BuildDMLPredicate`'s doc comment names this record.
