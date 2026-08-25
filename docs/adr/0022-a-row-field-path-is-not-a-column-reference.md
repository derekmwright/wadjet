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

**4. A field path BOXES exactly as a column of the field's type does.**
`expr.ColRef.Eval` boxes IPv4, MAC, DATE and FLOAT32 differently from
`Vector.GetValue` — the raw encoded number, not the text — and a field read
out of the container's boxed map produced the other one. `WHERE rw.ip =
ip_col` matched NO rows with both sides holding `"9.0.0.1"`, because one was
an int64 and the other a string. The field is read through the child vector,
and the renderers that undo that boxing (`likeOperand`, `networkOperand`) key
on the FIELD's type. `ColRef.fieldTyp` is that type; `ColRef.typ` remains the
CONTAINER's, because it is what every typed kernel indexes storage with.

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

## Not decided here

- **Three-part paths.** `rw.inner.k` and `t.rw.f` do not parse at all
  ("trailing input after the end of the statement"). That is a parser feature,
  and it fails loudly rather than answering wrongly.
- **The differential fuzzer** cannot generate field paths, because it
  qualifies references with the table alias and the parser accepts only a
  two-part reference. See the note in `internal/oracle/shapegen/typematrix.go`.
- **IPv6 and UUID stored inside a ROW** read back as the empty string, on the
  whole-ROW read as much as through a field path. That is a container-level
  defect, not a field-path one, and is pinned by
  `wadjet.TestRowFieldContainerLossIsStillReal`.
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
  `isPlainGroupKey`), `internal/planner/logical/optimizer.go`
  (`sanitizeScanNeeds`), `internal/engine/exec/project.go` (`ColumnRef`,
  `fieldPathColumn`), `internal/engine/expr/expr.go` (`ColRef.fieldValue`)
