# ADR-0022: A ROW field path is not a column reference

Status: Accepted (2026-08-25)

## Context

`rw.c` — a field of a `ROW` column — parses to the same `plansql.ColRef` that
a table-qualified `t.c` does. Nothing in the AST tells them apart; only the
input's declarations can. Every layer of the engine had quietly assumed the
two were the same thing, and each assumption produced a different silent wrong
answer (#568):

- **The planner's type resolution** looked the field name up among the input's
  own COLUMNS. `c` is not a column of `t(id, c_flat, rw)`, so the answer was
  Undecided and the STRING fallback stood. `SELECT rw.n` over an INT64 field
  returned `string("9")`, `ORDER BY rw.n` sorted `"10"` and `"192"` above
  `"9"`, and pgwire declared OID 25 (text) for both.
- **The aggregate** resolves its inputs and group keys BY NAME, through
  `exec.columnIndexFallback`, which has no ROW arm. `MIN(rw.n)` and
  `GROUP BY rw.n` failed outright — loudly, which is the one mercy in the set.
- **The column pruner** kept the DOTTED spelling as a required column. A
  stage's requested-column list is intersected with the file schema, which has
  no such column, so the PARENT went unread and every field came back NULL —
  on the stage DAG only, while the single-process path answered correctly.
- **`exec.ColumnRef`**, the evaluator a DAG fragment's projection is built
  from, resolved by `b.ColumnIndex` alone and answered NULL.
- **`NullCheckFilter` and `LikeFilter`** resolved a dotted name by stripping
  the qualifier and, finding nothing, matched NO ROWS. `WHERE rw.f IS NULL`
  returned an empty result indistinguishable from real data.
- **The comparison rules** (`expr.classifyOperand`) declined a field path
  outright — "the container's declaration does not type the field here" — so
  every predicate over one fell through to a guess.

The recurring shape is one sentence long: a field path LOOKS like a bare
column reference and behaves like a computed expression.

## Decision

**1. A ROW field path is resolved from its PARENT's declaration, in the same
order the runtime resolves the value.**

`physical.colDecls` carries the input's column types AND, for the ROW columns
among them, their fields (`logical.Node.ScanColFields`, populated by
`AnnotateScanColumns`). Resolution mirrors `expr.ColRef.resolveSlow` step for
step: the full dotted spelling names a column of its own first (a flat Zeek
`"id.orig_h"`), then the bare name, and only then is the qualifier read as a
ROW column and the name as its field. A declaration resolved in a different
order than the value would describe a different column.

**2. Wherever the planner asks "is this a bare column reference?", the answer
for a field path is NO.** `isPlainGroupKey`, `isSimpleColRef` and
`isSimpleColRefForRename`'s callers take the declarations as a parameter for
exactly this reason. A field path is MATERIALIZED — a synthetic pre-projection
for an aggregate input or group key, a computed spec on a DAG fragment — never
passed downstream as a name, because nothing downstream can resolve one.

**3. The scan reads the PARENT column.** Both other answers have shipped and
both were wrong: dropping the reference broke every dotted ROW access (#249),
and keeping the dotted spelling left a name no file carries in the requested
set, where it was intersected away along with the parent.

**4. A field path BOXES exactly as a column of the field's type does — and
every family that UNDOES that boxing must accept one.**
`expr.ColRef.Eval` boxes IPv4, MAC, DATE and FLOAT32 differently from
`Vector.GetValue` — the raw encoded number, not the text — and a field read
out of the container's boxed map produced the other one. `WHERE rw.ip =
ip_col` matched NO rows with both sides holding `"9.0.0.1"`, because one was
an int64 and the other a string.

The field is read through the child vector. `ColRef.fieldTyp` is the field's
declared type and `ColRef.typ` remains the CONTAINER's, because it is what
every typed kernel indexes storage with; the pair `valueType()` /
`valueVector()` is what a caller asks for instead — "what this reference
declares, and where its bytes are" — and it answers for a column and a field
alike.

Half a fix here is worse than none, and the first version of this change was
half. It taught `boxedTextOperand` (LIKE's and CAST's renderer) about field
paths and left the rest of the family keyed on `typ`, so a field path reached
them boxed as a number and they had no reason to unwind it:
`UPPER(rw.ipv4)` rendered `"150994945"`, `LENGTH(rw.ipv4)` counted that
number's digits, `ip_version(rw.ipv4)` answered NULL, and
`DATE_TRUNC('month', rw.date)` read an epoch DAY as epoch SECONDS and
answered 1970-01-01. The sites are `FuncCall.formatNetworkArgs`,
`FuncCall.formatTemporalArgs`, `FuncCall.resolveTemporalArgs`,
`temporalOperand`, `boxedTextOperand`, `temporalColOperand` and
`CmpNetworkLit` — every one of them now keys on `valueType()`, and a new one
must too. This is #484/#500/#319's family one level down, and those were
found the same way: one member fixed, the rest left keyed on the old test.

**5. A parameterized field declares the same way a parameterized column
does.** `colRefDeclaredType` declines DECIMAL, VECTOR and the containers for a
field path for the same reason it declines them for a column: it returns a
bare TypeID, and a DECIMAL without its scale builds an output vector that
reads back wrong. The full `parquet.Column` travels instead — on
`ProjectColumn.SourceCol` for `exec.Project` to resolve, on `aggPreProject`'s
`meta` for the synthetic column, and directly in `declaredOutputSchema`, where
the answer is advisory metadata rather than a vector.

## Consequences

- A new consumer of a column reference must ask whether it can serve a field
  path. If it resolves by NAME against a batch or a stage's column list, it
  cannot, and the planner must materialize the value for it.
- `wadjet.TestRowFieldPathCarriesTheFieldsDeclaredType` is the gate: every
  field type must answer exactly what the same value in a flat column answers,
  compared with `reflect.DeepEqual` so a Go-type divergence is visible. The
  differential gates cannot see this class — `oracle.Compare` and the
  fingerprint both render every cell with `fmt.Sprint`, where `int64(9)` and
  `"9"` are the same cell (ADR-0013).
- `coordinator.TestRowFieldPathDeclaredTypeOnTheDAG` asserts the same on the
  arm where a wrong declaration is also the wire's answer.
- PostgreSQL is the authority for what a field path MEANS (ADR-0012), and can
  be: a composite type is exactly a wadjet ROW. `benchmarks/tpch`'s
  `row_probe` fixture is the seam, and `pgCase.pgSQL` carries the one spelling
  difference — PostgreSQL requires `(rw).b`, since unparenthesised `rw.b` is
  read as table.column.

**6. A field path composes with the whole comparison cluster, or it is not a
reference at all.** `CmpNetworkLit` is built at COMPILE time, where
`structField` is not yet resolved, so a field path always reaches it; keying
its eval on the container's type dropped every one into the generic
text fallback, where `rw.cidr > '10.0.0.0/8'` compared stored text instead of
`kernel.CidrSortKey`'s inet order and `rw.cidr = 'not-a-cidr'` answered ZERO
ROWS where the column raises 22P02. The same rule reaches the vectorized
filters: `KernelFilter`, `NullCheckFilter`, `LikeFilter` and `InFilter` all
delegate a field path to their row-at-a-time fallback, and `KernelFilter`
checks the literal against the FIELD's type before delegating, so the
refusal survives the delegation.

## Not decided here

- **Three-part paths.** `rw.inner.k` and `t.rw.f` do not parse at all
  ("trailing input after the end of the statement"). That is a parser feature,
  and it fails loudly rather than answering wrongly.
- **The differential fuzzer** cannot generate field paths, because it
  qualifies references with the table alias and the parser accepts only a
  two-part reference. See the note in `internal/oracle/shapegen/typematrix.go`.
  The fixed corpora carry the coverage instead: `typematrix.Corpus`'s
  `rowfield_*` entries over a `c_row` widened to IPv4, MAC, DATE, CIDR and
  DECIMAL fields, and `wadjet.TestRowFieldPathCarriesTheFieldsDeclaredType`
  over every flat type.
- **The container lost its children's declarations** (#589) — now CLOSED on
  main, and recorded here because the widened `c_row` fixture is what surfaced
  its full extent. Two faces of one defect, both container-level rather than
  field-path ones (the whole-ROW read showed each identically): IPv6 and UUID
  inside a ROW read back as the empty string, and on the stage DAG a network
  or temporal field inside a ROW came back in raw storage form (`ip:167772167`
  for `ip:10.0.0.7`) with CIDR ordered by that text. fix-589 recursed the
  declared-schema overlay through every container, which fixes both faces on
  both paths; the one residual it did not reach — a windowed temporal
  function over a field path taking the vec kernel with the container vector
  as its argument — is closed here (see the EvalVec guard above). The
  type-matrix `c_row` entries and `wadjet`'s IPv6/UUID field cases are now
  plain gates, no pins.
- **Window functions over a field path are silently wrong** (#603):
  `SUM(rw.f) OVER ()` and `LAG(rw.f)` answer NULL, `ORDER BY rw.f` inside an
  OVER clause is ignored, and `PARTITION BY rw.f` makes one partition.
  `windowSpecOutputType` builds its reference from `cleanExpr`'s output,
  which has already dropped the qualifier, and the window operator resolves
  its input, partition and order keys by NAME. It needs rule 2's
  materialization, which the window path has no seam for yet — a third
  pre-projection site, in a subsystem this change does not touch. Filed
  rather than fixed here, and overlapping #585.
- **CONCAT propagates NULL where PostgreSQL ignores it** (#609). Caught by
  this change's oracle entries and not caused by them: a flat column and a
  bare `NULL` literal diverge identically. `RowFieldTextFunctions` is pinned
  with `knownBug` naming it; its UPPER and LENGTH columns stay gated.
- **A field path naming no field answers NULL** (#604) where an unknown
  COLUMN errors and PostgreSQL raises 42703. `colDecls.field` can already
  answer whether the field exists; the plan-time validation is not wired to
  it.
- **An aggregate over a PARAMETERIZED field on the stage DAG.** The worker
  builds its pre-aggregate projection from `distributed.AggSpec.InputType`,
  which is a bare TypeID, so `MIN(rw.dec_field)` declares the Float64 fallback
  there where the single-process path carries the field's (p,s) through
  `aggPreProject.meta`. This is surface the decision above CREATES — a field
  path only reaches `InputType` because rule 2 routes it through the derived
  path — and it is narrow: a bare DECIMAL column never takes that route, and
  the flat field types the corpus covers (STRING, INT64) are unaffected.
  Closing it means widening the spec to carry a declaration rather than a
  TypeID, which is the same change `ProjectSpec.Type` would need.

## Related

- ADR-0012 (PostgreSQL decides semantics), ADR-0013 (the gates and their
  blind spots)
- `internal/planner/physical/plan.go` (`colDecls`, `colRefDeclaredType`,
  `isPlainGroupKey`, `intArithAllInt`), `internal/planner/logical/optimizer.go`
  (`sanitizeScanNeeds`), `internal/engine/exec/project.go` (`ColumnRef`,
  `fieldPathColumn`), `internal/engine/exec/filter.go` (the four vectorized
  filters' row fallbacks), `internal/engine/expr/expr.go`
  (`ColRef.valueType` / `valueVector` / `fieldValue`, and the boxing-undo
  family that keys on them)
- #603, #604, #609 (residuals filed by this change), #589 (the
  container-level defect the widened fixture surfaced, fixed on main), #585
  (overlaps #603)
