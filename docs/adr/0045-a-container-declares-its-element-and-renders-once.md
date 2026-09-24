# ADR-0045: A container declares its element at the declared-output seam and renders through one renderer

Status: Accepted (2026-09-24, arc CW: #1250, #1017, #1133, #1303, #1268, #1021)

Related: ADR-0026 (a group key/slot has one identity and one name — the
declared output this extends), ADR-0012 (PostgreSQL decides semantics; the
container divergences listed below), ADR-0044 (the catalog's array rule).

## Context

An ARRAY, ROW or MAP value reached psql, pgJDBC, SQLAlchemy and the CLI as
TEXT in Go's rendering (`[1 2 3]`, `[SYN ACK]`, `[1718454645500]`) where
PostgreSQL sends `{1,2,3}` under an array OID. Measured at 83cd4a93, six
issues were one event: a container value reached an operator whose output
column had been declared STRING, because the declaration walk could not say
"ARRAY of T" or "MAP of (K,V)", and `batch.Vector.SetValue` coerced the `[]any`
/ `map[string]any` into the string vector with `fmt.Sprint`. Downstream of that
one coercion: the wire declared 25 and sent Go text (the constructor, #1250;
`tcp_flags` and the MAP functions, #1017); a zero-row result had no element to
declare (#1133); a derived table, VALUES or UNION re-read the STRING, so `v[1]`
was NULL and `ANY(v)` false (#1303); a TIMESTAMP element had become an int64
before it was stringified (#1268); ORDER BY / MIN / MAX / DISTINCT compared the
Go text (#1021). Where the declaration DID survive — a stored array column,
`ARRAY(subquery)` — the wire was already right. Separately, pgwire and the CLI
each had a renderer, and they disagreed (`map[x:1 y:q]` on the CLI, `(1,q)` on
psql).

## Decision

1. **The seam is the planner's declared type.** `expr.DeclType.Schema` (a whole
   `parquet.Column`: `ElementType` for ARRAY/MAP, `Fields` for ROW), answered by
   `physical.nodeDeclaredType` and read by the consumers that already carried a
   ROW's fields — `exec.ProjectColumn`, `declaredOutputSchema` (zero-row and
   Describe) and, through the executed schema, `wadjet.ColumnMeta` →
   `pgColumnOID`. What was missing was the INPUT: the `ARRAY[…]` constructor and
   an array cast declare ARRAY of their element; the function registry's
   container returns carry their element (fixed, or derived from the argument's
   key/value shape); and `ColDecls` carries `Elems`, filled by the SAME walk
   that fills a ROW's `Fields` (`inputColShapes`, two views of one answer), so
   a column reference to a container declares through renames, derived tables,
   set operations, joins and aggregates. Every materialization boundary —
   projection, aggregate pre-projection, group and window keys, block
   projection, set-op arms, and every DAG stage spec that carries
   (Type, Precision, Scale, Fields) — carries the element beside the fields.
   Set-op arms fold their ELEMENTS on the numeric ladder (`int4[] ∪ bigint[]`
   is `bigint[]`); arms with no common element are 42804.
2. **Loud, not plausible.** A container box written into a STRING or BYTES
   vector is a `*TypeMismatchError` (#361's guard), not `fmt.Sprint` text.
   After (1) no declared path reaches it; a path that does not carry the
   declaration fails with a named error instead of publishing Go text.
3. **One renderer.** PostgreSQL's text output — `array_out` (`{…}`, its
   quoting, bare NULL, a nested dimension bare) and `record_out` (`(…)`, an
   empty slot for NULL), with temporal leaves in their text form — lives in
   `batch.FormatPGText`, keyed on the DECLARED column, in the lowest MIT layer
   every door imports. pgwire's text format, the CLI's table and CSV, and
   `CAST(container AS TEXT)` call it; pgwire's binary format keeps PostgreSQL's
   array wire form under the declared element OID. The CLI's JSON form keeps a
   JSON array/object with typed leaves; the HTTP and async APIs return raw
   values, and the fix there is that the value is a container again.
4. **Ordering is the typed comparator.** Once the value is a declared ARRAY
   vector, ORDER BY, MIN, MAX, DISTINCT and merge keys run the existing
   element-wise `kernel.CompareValuesAt`; measured against PostgreSQL 17.11 for
   ten element types (empty first, prefix before extension, NULL element last).

## Alternatives rejected

- **Per-operator or per-function patches**, or making the STRING coercion print
  `{…}`: the text would be right and the TYPE still text — subscripting,
  `ANY()`, ordering and the OID stay wrong, and a timestamp element is already
  an integer when it is printed.
- **A separate element map beside `ColDecls`**: a fifth parallel walk drifts
  from the one that follows every rename rule a container name obeys
  (ADR-0026: one identity, one declaration).
- **Rendering from the value alone**: a bare `[]any` cannot tell ARRAY from MAP
  nor an int64 from a timestamp element; the renderer takes the column.
- **A renderer per door**: two renderers drifted before this decision.

## Consequences

Recorded divergences (docs/postgres-differences.md, ADR-0012 §5): a nested
array, an array of ROW or MAP, a ROW and a MAP declare OID 25 (MAP renders
`{a: 1, b: 2}`; PostgreSQL has no MAP); a network-type array is `text[]`
(PostgreSQL `inet[]`); `x::int[]` is `bigint[]` (ADR-0012 item 12); a
fractional literal array is `float8[]` (ADR-0024's literal deferral);
`current_schemas` is `text[]` (PostgreSQL `name[]`). `map_keys`, `map_values`
and `map_entries` follow the MAP's stored order (they ranged over a Go map).

Out of scope, recorded as filing candidates: `array_agg(x ORDER BY y)`, the
`ROW(…)` constructor (#985), `string_to_array`.

## Gates

- `pgwire.TestArcCWContainersDeclareAndRenderOnTheWire` — OIDs and text/binary
  rows vs PostgreSQL 17.11 per (container × producer), with
  `TestArcCWPinnedPostgresAnswersStillHold` re-measuring the pins live.
- `pgwire.TestArcCWTypedClientsReadArrays` — pgx typed array scans (the
  getArray cell) and psql `\d` / SQLAlchemy `format_type` reflection.
- `cli.TestArcCWContainersRenderOnTheCLI` — table, CSV and JSON.
- `coordinator.TestArcCWConstructedArraysOrderElementWiseOnEveryArm` — ORDER BY
  / MIN / MAX / DISTINCT / subscript on single, spilled, DAG and shuffled DAG.
- `coordinator.TestArcCWContainerOverARelationDeclaresOnEveryArm` — a
  container over a UNION's column, a derived table under a join or window,
  and MIN/MAX whose partial matched nothing (the aggregate spec carries the
  output element for that identity row), on every arm.
- `server.TestArcCWContainersOnEveryDeploymentDoor` — pgwire local and DAG,
  HTTP local and DAG, async.
- `batch.TestSetValueGuardPanicsOnUnholdableValues` — the container-into-text
  cells of §2.

Each of the first six fails at 83cd4a93.
