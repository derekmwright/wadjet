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
   **Round 2 (every PUBLISHER, from the same walk).** A column that an
   operator PUBLISHES — an aggregate's output (grouped, HAVING, the empty
   identity row), a window function's (MIN/MAX and the value functions over a
   column or a computed argument), a scalar subquery's (the stamp carries the
   element and fields), a bare GROUP BY / DISTINCT key, an aggregate read
   through a renaming projection (a decorrelated LATERAL body) — declares its
   element through `inputColShapes` and `ColDecls.namedDecl`, which answers a
   named column's WHOLE shape; the single-process aggregate reads its
   `AggColumn.OutputElementType` from that walk as the DAG's stage spec does,
   and the DAG gather allocates and publishes a computed column from the whole
   declaration. A zero-row result therefore declares what the same query with
   rows declares, on every arm.
   **Round 3 (the null-padded side, and every READER of a box).** The side of
   an OUTER join or a LATERAL that produced no rows is declared by the plan's
   join-side schema (`declaredJoinSchema` / `declaredBlockSchema`), which built
   a scan, computed or aggregate column from its TypeID alone and declined a
   block holding an aggregate item outright — so the padded column declared
   text (the single path's LATERAL aggregate lost the column entirely and fell
   to the STRING fallback). It now carries the same shape the side with rows
   declares. And the sites that read a container's BOX rather than a vector —
   a CAST that renders or converts one, every comparator of two — take their
   operand's declaration from the same walk (`physical.nodeDeclaredType`,
   registered with the expression layer as `expr.SetShapeResolver` and asked
   of the operand's AST against the input batch's executed columns,
   `expr/operand_decl.go`), not from a narrow operand walk that knew a column,
   a cast and a constructor of those: an element that COALESCE, CASE, a scalar
   subquery or an aggregate built had no declaration there and printed its
   box. A DATE or TIMESTAMP scalar a DAG stage substitutes for a subquery is a
   typed literal (`CAST('…' AS TIMESTAMP)`), so the stage declares it as the
   plan does.
   **Round 4 (a subquery that IS the container).** The expression layer asked
   for a scalar subquery's declaration through `expr.SubqueryDeclFunc`, which
   carried a TypeID, a precision and a scale — so a subquery that RETURNS an
   array reached the cast and every comparator with no element, and the box
   was read instead (`CAST((SELECT ARRAY[ts] …) AS TEXT)` = the epoch; a DATE
   array never equal; `{}` equal to `{""}`). The function now answers the
   subquery's whole declared column, the one the planner's walk computes for
   its projection. The membership constructs over a subquery — `IN`,
   `= ANY`, `<> ALL`, correlated or not — test a container member through the
   one comparator of §4 under the probe's and the set's declarations. On the
   DAG, a subquery's array value is substituted as the typed literal of that
   declaration (`CAST('{2024-01-10}' AS DATE[])`; a multi-dimensional one as
   the constructor of typed leaves), where it had been Go's text of the box
   (unparseable) or `null`. A `T[]` cast whose T is DECIMAL/NUMERIC declares
   its element's (p,s) by the scalar cast's rule (a bare NUMERIC takes the
   operand element's scale); it declared text[], so its elements were
   compared and hashed as text.
   **Round 6 (an element with no column type of its own).** An INTERVAL
   literal (`INTERVAL '…'`, or a CAST to one) declares no column type
   ANYWHERE else in this engine (`physical.binOpTemporalType`'s note: "the
   engine has no interval column") — `nodeDeclaredType` had no case for it, so
   an `ARRAY[…]` of them declined the WHOLE constructor as Undecided, and
   every meeting point guessed: the projection took the STRING fallback and a
   container box into STRING/BYTES is refused (§2 below), and the
   comparator's own last-resort box reader (`expr.boxShape`) had no case for
   `expr.IntervalValue` and defaulted to INT64 — both #361's guard on a shape
   base answered PostgreSQL's value for (`count(*)` over a projection nothing
   reads, `=`, `<`, `GROUP BY`, `DISTINCT`, `UNION`). `arrayLitDeclaredType`
   now declares such an element DURATION — the one element kind this engine
   already carries as a plain int64 with no (p,s) to get wrong
   (`kernel.CompareValuesAt` groups it with INT64) — and
   `batch.DurationNanoser` (an interface `batch` exposes so it need not import
   `expr` to recognize `expr.IntervalValue` by name) is what a value of no
   other column type here writes into it: `expr.IntervalValue.DurationNanos`
   is PostgreSQL 17.11's own `interval_cmp` metric (a month is 30 days here,
   twelve of them a year), so `ARRAY[INTERVAL '2 days'] < ARRAY[INTERVAL '10
   days']` orders by VALUE where the two intervals' RENDERED TEXT does not,
   and `ARRAY[INTERVAL '1 month'] = ARRAY[INTERVAL '30 days']` as
   PostgreSQL's own `array_cmp` does. Rendering an interval element to text
   still refuses 0A000 (§3): DURATION's own renderer prints a plain
   nanosecond count and nothing here asks it to convert one — a bare
   `SELECT ARRAY[INTERVAL '1 hour']` now answers that count (`[3600000000000]`)
   rather than raising 42000, which is not PostgreSQL's value either but is
   this type's existing, documented rendering (`docs/postgres-differences.md`
   #125, "DURATION counts nanoseconds").
2. **Loud, not plausible.** A container box written into a STRING or BYTES
   vector is a `*TypeMismatchError` (#361's guard), not `fmt.Sprint` text;
   a container written into an ARRAY/MAP/ROW vector allocated without its
   element or fields is a `*ContainerShapeError`, not the NULL the old silent
   return left. Round 1 claimed no declared path reached either and the
   review measured two that did (a correlated scalar subquery's MIN/MAX of an
   array, the single path's empty MIN/MAX); round 2 closed both at the walk
   above, and the claim is now the gates' — every publisher in the round-2
   zero-row table and the correlated-subquery cells answer on every arm. A
   path that does not carry the declaration fails with a named error. A CAST of a container is
   decided by ONE table before any scalar arm reads the box
   (`expr/cast_container.go`, round 2): text destinations take the rendering of
   §3, an array type converts element-wise, `VECTOR(n)` CONVERTS (pgvector's
   array_to_vector; round 1 had it hand back the `{…}` text, and every vector
   function over it read NULL), `JSON` takes `to_json`'s text
   (`batch.FormatPGJSON`), and every other destination is 42846 as on
   PostgreSQL — round 1 left `CAST(ARRAY[1,2] AS INT)` answering 0 and
   `AS DATE` NULL. A VECTOR's dimension is part of its declaration and rides
   every projection spec beside the element; a VECTOR vector allocated without
   it refuses (`*ContainerShapeError`) instead of keeping the slot NULL. A function registered with a container return and no shape (no builtin
   is) refuses in every whole-value position. A container whose element has
   no PostgreSQL text form here — an INTERVAL (this engine has no interval
   text form) — refuses a text or JSON cast with 0A000 rather than printing
   Go's struct text (round 3). An array cast to `VECTOR(n)` whose declared
   element is not a number is 42846, pgvector's answer, whatever its box.
   **Multi-dimensional arrays are out of this decision's scope (round 5).**
   This engine holds a nested array as an array of arrays and renders it as
   `array_out` does (`{{1,2},{3,4}}`, OID 25, §3), but it does not have
   PostgreSQL's multi-dimensional SEMANTICS: its leaves, its dimensions,
   `unnest` over the leaves, `cardinality`, `array_length(…, 2)`,
   `array_to_string` of the leaves. Round 4 made the cast into `T[]` convert
   the leaves and keep the dimensions, and the rest of the engine then read
   that shape as the outer array's inner arrays (`unnest` answered `{1,2}`
   rows where PostgreSQL answers the leaves; a constructed argument had
   refused before). Round 5 returns every multi-dimensional spelling to its
   pre-arc behaviour: the cast of a nested value into `T[]` passes it through
   unchanged under its own declaration, a text operand spelling a
   multi-dimensional array (`CAST('{{1,2},{3,4}}' AS INT[])`) passes through
   as its text, and `unnest` / `generate_subscripts` of a constructed or cast
   nested argument refuse 0A000 as they did. The one exception is the
   ordering (§4), where `array_cmp` stays: reverting it moved cells AWAY from
   PostgreSQL that main answered right. The semantics are a follow-up of
   their own (postgres-differences records them).
3. **One renderer.** PostgreSQL's text output — `array_out` (`{…}`, its
   quoting, bare NULL, a nested dimension bare) and `record_out` (`(…)`, an
   empty slot for NULL), with temporal leaves in their text form — lives in
   `batch.FormatPGText`, keyed on the DECLARED column, in the lowest MIT layer
   every door imports. pgwire's text format, the CLI's table and CSV, and
   `CAST(container AS TEXT)` call it — the cast under its operand's DECLARED
   element (§1, round 3): the box is first read through a vector of that
   declaration (`batch.DeclaredValue`), which is the one writer that accepts
   every storage width a type has (a DATE's int32 or int64 day count, an
   address's integer), and an element cast into `T[]` casts each element as a
   COLUMN of its declared type would be cast; pgwire's binary format keeps PostgreSQL's
   array wire form under the declared element OID. The CLI's JSON form keeps a
   JSON array/object with typed leaves; the HTTP and async APIs return raw
   values, and the fix there is that the value is a container again.
4. **Ordering is the typed comparator — for every comparator.** Once the
   value is a declared ARRAY vector, ORDER BY, MIN, MAX, DISTINCT, GROUP BY,
   window ORDER/PARTITION BY and merge keys run the existing element-wise
   `kernel.CompareValuesAt`; measured against PostgreSQL 17.11 for ten element
   types (empty first, prefix before extension, NULL element last). Every
   comparator of two BOXES goes through one function, `expr.containerOrder`,
   which writes both into one-row vectors of the pair's declared shape and
   asks the same kernel: the six operators, IN, BETWEEN, a simple CASE's WHEN
   and IS [NOT] DISTINCT FROM through `boxedPair.order`; GREATEST, LEAST and
   NULLIF through `extremumArms.order` (both with their operands'
   declarations, §1 round 3); and `compare()`, the last resort every other
   caller reaches, with the shape read off the boxes. Round 2 routed the six
   operators alone, and GREATEST/LEAST answered the text-greater array while
   BETWEEN kept the text order (round-2 review B3, P1).
   **Round 4.** Two DECIMAL elements of different scales are ordered by VALUE:
   each side is written under its OWN declaration and the kernel compares two
   DECIMAL vectors at their common scale exactly
   (`kernel.CompareDecimalValues`) — never through a double; an integer beside
   a DECIMAL is a DECIMAL at scale 0. The keys agree with that ordering: the
   columnar key was already scale-normalized (`batch.AppendDecimalKey`), the
   boxed key of a DECIMAL element is its canonical digits, and a set
   operation over two DECIMAL element types meets at their common `DECIMAL(p,s)`
   (ADR-0024's rule for a scalar column) — the DAG casts the arm, where it
   refused 0A000. A comparison of two array COLUMNS in a filter
   (`exec.ColColFilter`) orders through the same kernel (it refused). A
   MULTI-DIMENSIONAL array orders as PostgreSQL's `array_cmp` does — the
   flattened leaves, then their count, then the dimensions (each level's
   lengths; a ragged value's keep two shapes apart) — in the columnar kernel
   and the boxed comparator alike, so `{{1,2},{3,4}}` > `{{1,2,3}}`.
   **Round 5 (the element types of a pair are unified ONCE).** Round 4
   unified two DECIMAL scales and left every other pair to its meeting point:
   `=` widened int ⊕ float8 to a double while the hash-join key, the set
   operations and the FULL join's matched set keyed each side under its own
   element type (`int[] JOIN float8[]` 0 rows for 49, UNION 98), the
   sort-merge key read a FLOAT64 child with the INT64 kernel (XX000), and
   CASE / COALESCE / GREATEST / LEAST / `ARRAY[a, b]` declared the FIRST
   operand's DECIMAL scale and rounded the others' elements into it. Now ONE
   function decides the pair, `batch.CommonContainerColumn` — PostgreSQL's
   numeric promotion over the element (INT32 → INT64 → DECIMAL → FLOAT32 →
   FLOAT64; two DECIMALs at ADR-0024's common `(p,s)`, max scale, so no digit
   is rounded; an integer beside a DECIMAL brings its whole range at scale 0)
   — and every meeting point reads it: the comparator writes both sides under
   it (`expr.commonContainerShapes`); the kernel orders two element vectors
   of different numeric types at it (`kernel.compareMixedLeafAt`, the
   sort-merge key); the join resolves the pair's common LEAF type and both
   sides key every leaf at it (`physical.resolveJoinKeyTypes` →
   `exec.appendListKeyAt`, the same bytes for the build, the probe, the
   bloom filter and the DAG's partition hash); a set operation's element
   target is it and each arm's boxes move into it (the DAG casts the arm);
   and `expr.CommonDeclType` declares it for the choice constructs, whose
   chosen box is moved into it before the declared vector receives it
   (`expr.containerChoice`: an integer leaf as its exact text for a DECIMAL
   element, never an unscaled carrier).
   **Scope (round 6): scalar elements.** `batch.CommonContainerColumn` unifies
   a SCALAR element pair at every meeting point; a ROW or MAP element's OWN
   FIELDS keep their own per-side type instead of folding through the same
   rule. An array of ROW whose fields differ in integer width
   (`list<struct<x int32,…>>` beside `list<struct<x int64,…>>`) compares,
   keys and combines by VALUE here — `=` and a join key say never equal,
   `<` orders by value, `UNION` keeps both rows — where PostgreSQL refuses
   the pair outright (42804, `could not convert type`, the same refusal
   ADR-0012's "Two ROW shapes do not fold" already names for a bare ROW
   column of two shapes): a superset, not a fold, and not this decision's
   "one common element type" rule.

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
- `coordinator.TestArcCW3ContainerCastsRenderTheDeclaredElementOnEveryArm` —
  element {date, timestamp, numeric, bool, ipv4, uuid} × producer {column,
  COALESCE, CASE, scalar subquery, aggregate} × {TEXT, JSON, TEXT[], VARCHAR}
  on four arms, each cast against the one renderer over the same container
  projected, plus PostgreSQL's literal cells and the INTERVAL refusal.
- `coordinator.TestArcCW3NullPaddedSideDeclaresItsContainerOnEveryArm` —
  LEFT / RIGHT / FULL join, a LATERAL aggregate and LEFT JOIN LATERAL, each
  over an empty and a non-empty padded side.
- `coordinator.TestArcCW3EveryComparatorOrdersArraysOneWay` — eleven array
  pairs through twenty-four comparators, one ordering (PostgreSQL's).
- `coordinator.TestArcCW4OneOrderingEveryOperandComparatorElement` — the one
  ordering over OPERAND {constructor, column, expression, subquery, aggregate,
  window, LATERAL} × COMPARATOR {the six operators, GREATEST, LEAST, BETWEEN,
  IN list, IN / `= ANY` / `<> ALL` over a subquery, CASE, NULLIF, IS DISTINCT
  FROM, ORDER BY, MIN, MAX, DISTINCT, GROUP BY, window ORDER / PARTITION, hash
  join, sort-merge join, UNION, INTERSECT, EXCEPT, filter} × ELEMENT {int,
  text, empty text, date, timestamp, two DECIMAL scales, bool, IPv4, nested}
  on six arms (two with the sort-merge join forced).
- `coordinator.TestArcCW4SubqueryOperandCarriesItsDeclaredElement` — a scalar,
  ARRAY(…), correlated and COALESCE'd subquery of six element types cast to
  TEXT, JSON, TEXT[], VARCHAR and VECTOR(1), pinned to PostgreSQL 17.11.
- `coordinator.TestArcCW4DecimalElementsKeyByValueOnEveryKeyPath` — two
  DECIMAL scales of one array through hash join, IN / ANY / ALL, UNION /
  INTERSECT / EXCEPT, DISTINCT, GROUP BY, window PARTITION, a join filter and
  `<`, on six arms.
- `wadjet.TestArcCW4ContainersAnswerAsPostgreSQLOnTheEmbeddedEngine` — the
  embedded engine's cells of the three above.

Each of the first six fails at 83cd4a93.
