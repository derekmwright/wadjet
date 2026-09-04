# ADR-0022: A ROW field path is not a column reference

Status: Accepted (2026-08-25; amended 2026-09-04 by arc E3 for #769 — rule 1's ORDER is stated the other way round and asked in ONE place)

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
`"id.orig_h"`), **then the qualifier is read as a ROW column that DECLARES the
name as its field**, and only then is the qualifier stripped and the bare name
resolved. A declaration resolved in a different order than the value would
describe a different column.

**The field-path step comes BEFORE the strip, and it is asked in ONE place**
(2026-09-04, #769). The order in the paragraph above was the other way round
until then, and it made the reference mean whatever OTHER relation in the
stream published a column of the FIELD's name:

    SELECT n.id, c_row.b FROM typemx_nested n JOIN decpair d ON n.id = d.id
    -- the field is INT64 (11, NULL, NULL, 44, …); wadjet answered decpair's
    --   DECIMAL(18,4) `b` on all four arms, in silence

Six resolvers had grown their own copy of the three-way order —
`expr.ResolveColumnRef`, `exec.lazyFieldIdx.get`, `exec.fieldPathColumn`,
`exec.Project`'s schema pass, the four vectorized filters' ROW delegation,
`physical.colDecls.colDecl`/`isFieldPath` and `exec.columnIndexFallback` (the
one the group keys, the aggregate inputs, the sort keys and the join keys all
come through). `batch.RowFieldPath` is the one place the question is answered
now, and every one of those sites asks it.

The PREDICATE spelling is the one that shows the class rather than the count:
`WHERE c_row.b IS NULL` beside a join arm publishing `b` counted the ARM's
NULLs — 2 where the field has 3 — on all four arms, and it counts 3 on all four
now. An earlier draft of this paragraph said the two engines disagreed about it
(2 against 3); they did not, and the number is corrected here rather than left
standing (round-2 review, P3).

The step is gated on the container DECLARING the field, which is what keeps
the reorder off an ordinary qualified reference whose qualifier happens to
name a ROW column of the stream, and what leaves the "field path naming no
field" disposition (#604) exactly where it was.

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
  cannot, and the planner must materialize the value for it. When it asks
  whether a dotted reference IS one, it asks `batch.RecordBatch.RowFieldPath`
  and asks it before stripping the qualifier — a private copy of the order is
  how six sites came to disagree (#769).
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

  **Amended 2026-09-04 (arc E3 round 3).** PostgreSQL's own spelling for a
  field path is the PARENTHESISED one — it reads `c_row.b` as `table.column`
  and only `(c_row).b` as the field — and wadjet parses it now:
  `(c_row).b` produces the SAME `plansql.ColRef{Table: "c_row", Column: "b"}`
  the bare spelling produces, so it is one reference with one resolver and one
  `batch.RowFieldPath` question, not a second spelling for the seven sites of
  rule 1 to disagree about. It composes: predicate, GROUP BY, ORDER BY, join
  key, aggregate argument, cast, arithmetic. `a.b.c` is still a syntax error.

  What does NOT work is a container the reference QUALIFIES — `(x.c_row).b`,
  and by the same token the nested `((c_row).rw).k`, whose container is itself
  a path. Both are REFUSED with `0A000` naming the workaround, and the refusal
  is the decision: an earlier form of this change lowered them to a
  three-part flat name and they answered **NULL on every arm, with no join in
  the query at all**. The mechanism is that the identity is two-part —
  `plansql.ColRef` is `{Table, Column}` and every resolver rule 1 binds
  together reads exactly those two fields — while a qualified container needs
  three: relation, container, field. Making it work means a THIRD part
  carried through `inputColFields` (keyed by the bare container name),
  `colScope.rowFields` (the same), `colDecls.field`/`isFieldPath`, the join's
  carried-container set, and `cleanExpr`, which strips the qualifier before
  the field is asked for. That is rule 1's list again, one part wider, and it
  is its own change.

  **The cost of not having it** is recorded here rather than left implicit:
  `(x.c_row).b` is PostgreSQL's disambiguation for a container two relations
  both publish, and that shape is refused 42702 (§the ambiguity, below). So
  the only way to read such a field in wadjet today is a derived table that
  renames the other arm's container away — which works, and is what
  `docs/data-types.md` documents. Gated at
  `internal/coordinator/derived_arm_join_chain_two_path_test.go`:
  `parenthesised/unqualified-is-the-same-reference`,
  `parenthesised/in-a-predicate-and-a-group-key`,
  `parenthesised/unknown-field`,
  `parenthesised/a-qualified-container-is-refused-not-answered`,
  `parenthesised/a-qualified-container-is-refused-without-a-join` and
  `parenthesised/a-nested-path-is-refused-not-answered`, plus the parser's own
  spellings in `internal/planner/sql/paren_field_path_test.go`.
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
- **Window functions over a field path** (#603) — CLOSED by fix-585, which
  materialized a window's expression and field-path keys on both paths, the
  third pre-projection site this change had left to the window subsystem.
  `MIN(rw.f) OVER ()` and `PARTITION BY rw.f` now answer for every scalar
  field type. The `win_*` shapes in
  `wadjet.TestRowFieldPathCarriesTheFieldsDeclaredType` gate it.
  RESIDUAL: a windowed MIN/MAX over a PARAMETERIZED or CONTAINER field path
  still loses the field's (p,s) or drops the container value — the window
  counterpart of the aggregate-over-a-parameterized-field residual below,
  filed as #618 and skipped by those shapes for the affected types.
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
- #604, #609, #618 (residuals filed by this change), #603/#585 (window
  field-path keys, closed on main), #589 (the container-level defect the
  widened fixture surfaced, fixed on main)

