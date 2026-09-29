# ADR-0012 divergences: Containers (ARRAY, ROW, MAP, VECTOR)

ROW, ARRAY and MAP containers: field-path spellings, declared OIDs, nested-array semantics, container ordering and array casts. One family of the [ADR-0012 divergence catalog](README.md): the rule that decides each disposition is [ADR-0012 §5](../0012-sql-semantics-authority.md#decision), and the dated history of every entry is the [amendment log](../0012-amendments.md).

Columns: `cell` is the smallest statement that shows the difference; `PostgreSQL 17.11` and `this engine` are the answers; `SQLSTATE` is this engine's (`PG …` when only PostgreSQL raises); `since` names the date and the source entries (`E…` below, `P…` on the [differences page](../../postgres-differences.md)); `gate` is the test that pins the row.

## Mechanisms

**MAP and VECTOR orders are wadjet-defined** (E14)
MAP and VECTOR exist only here, so their total orders (`internal/engine/exec/kernel/container_sort.go`) are not a choice against a PostgreSQL answer. ROW is PostgreSQL's `record`, which has no `min`/`max` (MIN/MAX over ROW is refused since arc BR; its ORDER BY is unchanged). ARRAY maps to `anyarray`, which does have `min`/`max`, so ARRAY is the one container whose ordering is measurable against PostgreSQL, though no fixture gates it there.

**A dotted container reference is a field path only where it can mean nothing else** (E35)
A bare `c_row.b` is a field path only where its qualifier names a ROW column of the stream that declares the field (`batch.RowFieldPath`, the one place the engine asks); an ordinary qualified reference to a relation is untouched, and a container that does not declare the field is refused with PostgreSQL's wording, `could not identify column "nosuch" in record data type` (42703). `(c_row).b` becomes the same reference as the bare form. ADR-0022 carries the mechanism.

**Ambiguity is decided before the qualifier is stripped** (E35)
Two arms spelling the container alike (`SELECT x.id, (c_row).b FROM n x JOIN n y ON x.id = y.id + 1`) are 42702 `column reference "c_row" is ambiguous` at plan time, as on PostgreSQL 17, from `physical.colScope.check`. Asking after stripping answered one arm by plan shape, and beside an arm publishing a column of the field's name answered that column; the join-arm cases are gated in `internal/coordinator/derived_arm_join_chain_two_path_test.go` and `internal/engine/batch/row_field_path_test.go`.

**Container declarations** (E79)
`pgTypeOID` has no ROW arm, so a ROW column declares text (25); its value is PostgreSQL's composite text in declared field order, including the empty slot for a NULL field, and `wadjet.ColumnMeta.Fields` (the result's own declaration, read first by every door) gives the field order for constructed composites. Moving ROW to 2249 is a decision about the whole ROW type on every door. An ARRAY declares the array of its element (int4[] 1007, int8[] 1016, text[] 1009, float8[] 1022, numeric[] 1231, timestamp[] 1115, date[] 1182, uuid[] 2951, bool[] 1000, bytea[] 1001) with PostgreSQL's binary form; the declared-output seam (ADR-0045) carries the element so a zero-row result declares it too. A nested array, and an array of ROW or MAP elements, still declares 25.

## Catalog

| cell | PostgreSQL 17.11 | this engine | SQLSTATE | disposition | since | issue | gate |
|---|---|---|---|---|---|---|---|
| **r1** `SELECT c_row.b FROM n` | ERROR 42P01 missing FROM-clause entry for table "c_row" | answers the field (measured plan); (c_row).b and the qualified (x.c_row).b answer too (measured), as PostgreSQL's spellings | — | kept superset | 2026-09-04 · [E35](#e35), P056 | #769 | `coordinator derived_arm_join_chain_two_path_test.go join-arm-publishes-the-field-name*`, `batch row_field_path_test.go` |
| **r2** `SELECT c_row FROM n` | composite text in declared field order, declared record (OID 2249) | the same composite text, declared text (OID 25) (measured) | — | value divergence | 2026-09-08 · [E79](#e79), P026 | #992, #965 | — |
| **r3** `SELECT ARRAY[ARRAY[1,2],ARRAY[3]]` | ERROR: multidimensional arrays must be rectangular | {{1,2},{3}}, declared text (OID 25) (measured): nested arrays are ragged | — | kept superset | 2026-09-24 · [E79](#e79), P027 | #1133, #1337 | `pgwire.TestAZeroRowArrayResultKeepsItsDeclaration` |
| **r4** `SELECT CARDINALITY(ARRAY[ARRAY[1,2],ARRAY[3,4]])` | 4 | 2 (measured): the outer array's length | — | value divergence | — · P027 | #1337 | — |
| **r5** `SELECT unnest(v) FROM t` | four rows 1, 2, 3, 4 for v = {{1,2},{3,4}} | two rows {1,2}, {3,4}; over a constructed or cast nested array ERROR 0A000 | — | value divergence | — · P027 | #1337 | — |
| **r6** `SELECT array_length(ARRAY[ARRAY[1,2],ARRAY[3,4]], 2)` | 2 | NULL | — | value divergence | — · P027 | #1337 | — |
| **r7** `SELECT array_to_string(ARRAY[ARRAY[1,2],ARRAY[3,4]], ',')` | 1,2,3,4 | [1 2],[3 4] | — | value divergence | — · P027 | #1337 | — |
| **r8** `SELECT 3 = ANY (ARRAY[ARRAY[1,2],ARRAY[3,4]])` | t | ERROR 0A000 | 0A000 | refusal | — · P027 | #1337 | — |
| **r9** `SELECT array_ndims(ARRAY[ARRAY[1,2],ARRAY[3,4]])` | 2 | ERROR 42883 (array_ndims and array_dims do not exist) | 42883 | documented gap | — · P027 | #1337 | — |
| **r10** `SELECT CAST(ARRAY[ARRAY[1.5,2.5]] AS INT[])` | {{2,3}} | {{1.5,2.5}}: the nested value passes through unchanged | — | value divergence | — · P027 | #1337 | — |
| **r11** `SELECT CAST('{{1,2},{3,4}}' AS INT[])` | {{1,2},{3,4}}, declared integer[] | the text {{1,2},{3,4}}, declared text (OID 25) | — | value divergence | — · P027 | #1337 | — |
| **r12** `SELECT (ARRAY[ARRAY[1,2]])[1]` | NULL (one subscript of a 2-D array) | {1,2}: a subscript reads the outer array | — | value divergence | — · P027 | #1337 | — |
| **r13** `SELECT ARRAY[CAST('1.2.3.4' AS IPV4)]` | ARRAY['1.2.3.4'::inet] is inet[] (OID 1041), {1.2.3.4} | {1.2.3.4}, declared text[] (OID 1009) (measured) | — | value divergence | 2026-09-24 · [E79](#e79), P021 | — | — |
| **r14** `SELECT CAST(ARRAY[1] AS INT[])` | declared integer[] (OID 1007) | declared bigint[] (OID 1016) (measured): the element follows CAST(x AS INT), bigint here | — | value divergence | 2026-09-24 · [E79](#e79), P022 | — | — |
| **r15** `SELECT ARRAY[1.5, 2.25]` | {1.5,2.25}, declared numeric[] (OID 1231) | {1.5,2.25}, declared double precision[] (OID 1022) (measured) | — | value divergence | 2026-09-24 · [E79](#e79), P022 | — | — |
| **r16** `SELECT tcp_flags(18)` | ERROR 42883 (no such function) | {SYN,ACK}, declared text[] (OID 1009); map_entries is an array of composites under OID 25 | — | kept superset | — · P019 | — | — |
| **r17** `SELECT m FROM t` | no MAP type | {a: 1, b: 2}, {} for an empty map, declared text (OID 25) | — | kept superset | — · P020 | — | — |
| **r18** `SELECT v FROM t ORDER BY v` | no MAP or VECTOR type in PostgreSQL core | a wadjet-defined total order (internal/engine/exec/kernel/container_sort.go) | — | kept superset | — · [E14](#e14), P082 | — | — |
| **r19** `SELECT min(a) FROM t` | ARRAY min/max ordered by array_cmp | the container_sort.go order; nested arrays order as array_cmp does; no fixture gates ARRAY order against PostgreSQL | — | documented gap | — · [E14](#e14) | — | — |
| **r20** `SELECT ARRAY[1.5] > ARRAY[1]` | ERROR 42883 operator does not exist: numeric[] > integer[] | t (measured): compares by value | — | kept superset | 2026-09-24 · [E79](#e79) | — | — |
| **r21** `SELECT CAST(ARRAY[1,2] AS JSON)` | ERROR 42846 cannot cast type integer[] to json | [1,2] (measured), to_json's text | — | kept superset | 2026-09-24 · [E79](#e79) | — | — |
| **r22** `SELECT ARRAY[INTERVAL '1 day']` | {"1 day"} | ERROR 0A000: an INTERVAL element compares by value through the DURATION carrier but has no text | 0A000 | refusal | 2026-09-24 · [E79](#e79) | — | — |
| **r23** `SELECT ARRAY[1, 'a']` | ERROR 22P02 invalid input syntax for type integer: "a" | ERROR 42000 batch: cannot store string into INT32 vector (measured): no common element type is resolved at plan time; recorded for repair | 42000 | documented gap | 2026-09-24 · [E79](#e79) | — | — |

## Source entries

The ADR-0012 §5 entries this family was built from, verbatim as they stood at 0da8399a (line numbers are that revision's). Dated blocks that recorded a closure, withdrawal or correction moved to the [amendment log](../0012-amendments.md) and are replaced here by a pointer.

### E14

ADR lines 597-606. Catalog rows: r18, r19. Stated in [Mechanisms](#mechanisms).

- **MAP and VECTOR ordering.** Both are wadjet-only types — PostgreSQL has
  neither — so their total orders (`internal/engine/exec/kernel/
  container_sort.go`) are wadjet-defined, not a choice against a
  PostgreSQL answer. This bullet is ONLY MAP and VECTOR: ROW is `record`,
  which PostgreSQL HAS as a type but offers no `min`/`max` over (and
  wadjet's MIN/MAX over it is refused since arc BR, above; its ORDER BY is
  unchanged), and ARRAY maps to PostgreSQL's `anyarray`,
  which DOES have `min`/`max` — so ARRAY is the one container whose
  ordering is a choice measurable against a PostgreSQL answer, even though
  no fixture gates it there today.

### E35

ADR lines 1324-1368. Catalog rows: r1. Stated in [Mechanisms](#mechanisms).

- **An UNPARENTHESISED `container.field` is a ROW FIELD PATH, where
  PostgreSQL requires `(container).field`.** (Added 2026-09-04, #769;
  ADR-0022.) PostgreSQL reads a bare `c_row.b` as *table* `c_row`, *column*
  `b`, and raises 42P01 `missing FROM-clause entry for table "c_row"` — the
  parenthesised `(n.c_row).b` is the only spelling it accepts. Wadjet
  ANSWERS the bare one, resolving the field out of its container, and that
  is a superset in the accepting direction: every query PostgreSQL answers
  answers the same here, and the spelling PostgreSQL refuses is one it
  cannot mean anything else by.

  **PostgreSQL's own spelling parses too** (2026-09-04 round 3):
  `(c_row).b` becomes the SAME reference the bare form does, so the
  superset is two spellings of one meaning rather than two meanings. The
  one PostgreSQL spelling wadjet does NOT accept is a container the
  reference QUALIFIES — `(x.c_row).b` — which needs a three-part identity
  the engine does not carry and is REFUSED with `0A000` naming the
  derived-table workaround. That is a divergence in the REFUSING direction
  and it is listed here for that reason; ADR-0022 carries the mechanism.

  The boundary is what keeps it a superset rather than a second answer. A
  dotted reference is a field path ONLY where its qualifier names a ROW
  column of the stream **that declares the field** (`batch.RowFieldPath`,
  the one place the engine asks); an ordinary qualified reference to a
  relation is untouched, and a qualifier naming a container that does not
  declare the field is refused with PostgreSQL's own wording (`could not
  identify column "nosuch" in record data type`).

  **Two arms spelling the container alike are REFUSED, and there the
  PARENTHESISED spelling is the anchor.** `SELECT x.id, (c_row).b FROM n x
  JOIN n y ON x.id = y.id + 1` is `column reference "c_row" is ambiguous`,
  42702, on PostgreSQL 17 — measured — so the ambiguity has an answer to
  follow even though the unparenthesised spelling does not. Wadjet raises
  the same class with the same wording, at plan time, and the message names
  the QUALIFIER because a container is a column and the ambiguity is the
  container's (`physical.colScope.check`). It answered ONE of the two arms
  until 2026-09-04, and which one depended on the plan shape.

  Asking that question BEFORE stripping the qualifier is the whole of it,
  and asking it after was a wrong VALUE rather than a divergence: beside a
  join arm publishing a column of the FIELD's name, `c_row.b` answered
  THAT arm's column on every arm and declared its type on the wire.
  Gated at `internal/coordinator/derived_arm_join_chain_two_path_test.go`
  (`join-arm-publishes-the-field-name*`) and
  `internal/engine/batch/row_field_path_test.go`.

### E79

ADR lines 3145-3191. Catalog rows: r2, r3, r13, r14, r15, r20, r21, r22, r23. Stated in [Mechanisms](#mechanisms).

- **A ROW column declares OID 25 (text), not `record` 2249.** (Added
  2026-09-08, arc A1; the divergence predates it.) `\gdesc` on
  `ROW(1::int4, 2.5::float8, 'x'::text)` says `record`, OID 2249, measured
  on 17.11. `pgTypeOID` has no ROW arm and falls to its text default, as it
  does for MAP and for the network types this engine renders as text.
  **ARRAY left that list 2026-09-15 (arc ND, #992):** an ARRAY column
  declares the array OF its element now (int4[] 1007, int8[] 1016, text[]
  1009, float8[] 1022, numeric[] 1231, timestamp[] 1115, date[] 1182,
  uuid[] 2951, bool[] 1000, bytea[] 1001), with PostgreSQL's array binary
  form under those OIDs. Three ARRAY shapes still declare 25 and each is a
  fact about PostgreSQL rather than a gap: a NESTED array (PostgreSQL's
  `int4[][]` is RECTANGULAR and this engine's are ragged, so `{{1,2},{3}}`
  is a value here and a syntax error there), a ROW or MAP element (no
  registered composite OID for a constructed row; no MAP at all), and an
  ARRAY the PLANNER could not type — a ZERO-ROW result, where there is no
  vector to read the element from and `colDecls` carries no element map.
  The last is pinned fail-on-agree in
  `pgwire.TestAZeroRowArrayResultKeepsItsDeclaration`.
  **The zero-row shape left the list 2026-09-24 (arc CW, #1133):** the
  declared-output seam carries a container's element (ADR-0045), so a
  zero-row ARRAY result declares its array OID; the pin flipped to 1007
  as the proof. A nested array now renders bare (`{{1,2},{3,4}}`,
  `array_out`'s form) and keeps OID 25 for the raggedness above.
  The other container differences arc CW measured (2026-09-24..26) are
  recorded in ADR-0045's Consequences and on the differences page rather
  than repeated here: a network-type array is `text[]`, `x::int[]` is
  `bigint[]`, a fractional literal array `float8[]`; two supersets kept —
  `ARRAY[1.5] > ARRAY[1]` compares by value (PostgreSQL: 42883) and
  `CAST(ARRAY[1,2] AS JSON)` is `to_json`'s text (PostgreSQL: 42846); an INTERVAL element compares by value through the DURATION
  carrier but has no text (0A000); a nested array's multi-dimensional
  semantics are not PostgreSQL's (#1337); and arrays with NO common
  element type are not refused as PostgreSQL refuses them (a defect,
  recorded for repair, not a kept superset).

  The ROW half of this entry is unchanged. The VALUE is
  PostgreSQL's own composite text in DECLARED field order, byte for byte
  including the empty slot for a NULL field, so a client that parses the
  text gets the server's answer; only the OID a driver binds by differs.
  Moving it to 2249 is a decision about the whole ROW TYPE — every column of
  it, on every door — and is #992's neighbour rather than a per-function
  choice. What arc A1 DID close is the declaration REACHING the renderer:
  until #965 the only source of a composite's field order was the CATALOG,
  which describes no value an aggregate constructs, and such a column
  rendered with SORTED KEYS — a well-formed DataRow carrying the right
  values in the wrong places. `wadjet.ColumnMeta.Fields` is the result's own
  declaration now and every door reads it first.
