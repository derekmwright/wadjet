# Gate coverage audit — the type × consumer matrix

**2026-08-22.** What the correctness gates can and cannot see, per column type
and per consumer of a column value.

**Update 2026-08-23.** #391 (`(*Vector).GetValue`'s `TypeBytes` arm aliasing
the column arena — §"Why" below, §2c row R3, ranked cell #2) is **fixed** on
main (`fa22f72`). The three pins it held — `tmPinPrefixes`/`tmOptPinPrefixes`
in `wadjet/type_matrix_test.go` (`minby_c_bytes`, `maxby_c_bytes`,
`minby_scalar_c_bytes`) — are deleted; `TestTypeMatrixBatchReuse` and
`TestTypeMatrixOptimizationInvariance` now compare those entries with no
exemption and pass. §1b's three inert DuckDB pins are also resolved: all three
(`OuterJoinOnResidual`, `FullJoinOnConjunctBuildSide`,
`OuterJoinExpressionKey`) pass fully gated with no divergence (verified by
running them; #358's join `Residual` field now carries what they exercise), so
their stale `knownBug` prose is deleted rather than converted to
`knownBugArm`. See §6's "Other named gaps" for what is still open, including a
new gap this fix's own mechanism doesn't cover.

**Update 2026-08-23 (second wave).** Four more of the divergences §5 lists
are **fixed**, and every pin they held is deleted — the gates now compare
those entries with no exemption:

| Issue | Fix | Pins deleted |
|---|---|---|
| #394 | `kernel/sort.go` gained a DECIMAL arm in all three resolvers (numeric, by unscaled `Int128` at equal scale), and `compareVectorValues` (`window.go`) one of its own instead of falling through to `compareAny` on the formatted string | 4 in `tmOptPins`, 2 in `tmdPins` |
| #395 | `writePartialKeyToColumn` (`aggregate_partial_spill.go`) no longer writes a DISPLAY-form key into raw storage: only STRING/BYTES/CIDR store that text, the rest re-parse through `Vector.SetValue`. Closes the int-backed half of the same defect too — a DATE/IPv4/MAC key on this path read back as 1970-01-01 / 0.0.0.0 | 4 in `tmOptPins`, which is now empty |
| #396 | the native writer stamps the declared schema into the footer (`wadjet.schema` KeyValueMetadata) and `FileReader` restores each leaf column's type identity from it; `SQLResult.OutputSchema()` + `coordColumnMetas` give the coord path the same pgwire ColumnMetas the embedded path already had | 37 in `tmdPins`, which is now empty |
| #402 | `materializeFlatAccums` resolves the SHARED count array before its bound check — `flatAccumLen` probes an aggregate's own arrays and a count-sharing COUNT owns none, so it measured 0 and was skipped for every group. Not a top-N bug: the heap and the multi-key comparator were correct and were sorting a wrong value. Wider than the reported shape — see below | 1 in `tmFuzzOptPins` + its `tmFuzzIntermittentOptRetries` entry |

`TestTypeMatrixTwoPath` now reports **229 compared, 0 diverged**.

Three consequences worth recording:

- **§2a's "nine types are lossy through a self-describing parquet file" is
  closed for files written from here on**, and with it §6's "a logical
  annotation for them so a self-describing file round-trips". The residual is
  a migration boundary, not a design one: a file written by an older build
  carries no `wadjet.schema` key, so the DAG still reads its IPv4/IPv6/MAC/
  UUID/Bytes/Port/Protocol/Duration columns as INT64/STRING. Closing that for
  existing data needs the catalog's declared schema plumbed to the worker's
  scan (`physical.Stage` → `distributed.OpSpec` → `sourceForAliasWithProjection`
  → `finishParquetState`, plus `worker/executor.go`'s four
  `scan.ReadFileBatches` calls), which is a separate change.

  **The two-path gate is STRUCTURALLY BLIND to that boundary and always will
  be**: `TestTypeMatrixTwoPath` writes its own fixture files with the current
  writer (`tmdCluster` calls `parquet.NewWriter` on every run), so every file
  it ever reads carries a `wadjet.schema` key. The migration case — a table
  whose parquet predates the key — cannot occur in a suite that generates its
  own data, no matter how the corpus grows. On such a table the DAG still
  returns raw storage while the fast path returns the display form, which is
  the ORIGINAL #396 symptom, unfixed, on old data. Catching it needs a fixture
  written WITHOUT the key (the writer would need a switch, or the fixture
  would have to be checked in as bytes) or the catalog→worker plumbing above,
  which removes the question. Until one of those lands, the claim "#396 is
  fixed" means "for files written from here on".
- **#392's four `minby_scalar_*` pins moved from `tmdPins` to
  `tmdUnsupported`.** They were pinned as an ASYMMETRY — the single-process
  arm refusing while the DAG answered — only because the DAG saw those
  columns as INT64/STRING, which `MIN_BY`'s six-case switch happens to
  declare correctly. With the real declared type the DAG reaches the same
  wrong FLOAT64 declaration and refuses the same way. The two paths agree;
  #392 itself is untouched.
- **#402 was a diagnosis correction.** Its pin text (and the ADR-0013
  amendment quoting it) attributed the divergence to an unstable tie-break in
  the top-N merge. The top-N was innocent. `tmFuzzIntermittentOptRetries` is
  now empty but stays, as the settled policy for the next pin with no forcing
  gate.
- **#402's blast radius was wider than "a duplicate COUNT", and the issue text
  understated it.** `planCountArrays` (`aggregate.go:6059`) shares a count
  array by COLUMN CLASS, not by expression: every count-needing aggregate over
  the same input column joins one class, plus one class for `COUNT(*)`, and
  the first of each class owns the array while the rest read it. The
  aggregate that lost its count through this merge is any SHARER that owns no
  arrays of its own — and a COUNT owns none (no sum, no min/max, `count` nil
  once it shares). So `SUM(v), COUNT(v)`, `AVG(v), COUNT(v)`, two `COUNT(*)`
  and the 2nd and 3rd of three `COUNT(v)` were ALL answering 0 through the
  generic-SoA merge, over a float64 group key, whenever a clone merged empty.
  The reverse order (`COUNT(v), SUM(v)`) was always right, because there the
  sharer is the SUM and it owns `sumI64` — which is exactly why
  `agg_count_layout_test.go`'s AVG-sharer merge case passed and the cell
  stayed uncovered. `TestCountSharingGenericSoAMergeShapes` now pins all five
  shapes; four of them fail on the pre-fix guard.

**Update 2026-08-23 (third wave: the adversarial review of the second).** The
second wave's four fixes were reviewed against their own claims. Four changes
came out of it, and two issues were filed for what should not be fixed under
those commits' scope.

| Finding | Change |
|---|---|
| #396's overlay trusted the blob too far | `overlayDeclaredSchema` gated only on `wadjetTypeToPhysical`, so a footer blob could override types the FILE ANNOTATES CORRECTLY — DECIMAL(18,4) relabelled (12,1) is every value 1000× off, TIMESTAMP→IPv4, STRING→IPv6 renders `""` — and an unknown TypeID passed because that function's default returns BYTE_ARRAY, and `Dimension` was copied unvalidated into what the batch allocator sizes from. The overlay now applies ONLY to a leaf with no LogicalType and no ConvertedType (plus one documented exception, below), ONLY for the eight types parquet cannot annotate, and copies type identity ONLY. Permanent `FuzzOverlayDeclaredSchema` plus a 25-case adversarial table |
| #394's comparator was exact only below 2^53 | `CompareDecimalAt` rescaled through float64 at unequal scales, and `SortMergeJoin` uses that comparator for key EQUALITY — so it was a spurious JOIN MATCH, not a sort-order nicety. Now exact at every scale (`Int128.MulPow10` with overflow detection, big.Int beyond it). The suite had no sort-merge join over a DECIMAL key at all; it has one now, at equal and unequal scale |
| the IPv6 write stub | `ipv6StringToBytes` stored the literal's TEXT — 11 bytes where the contract is 16 — which #396 made visible: the column now reads back as IPV6 and `GetValue` renders any other length as `""` |
| a result's schema was read off its batches | `SQLResult.OutputSchema()` fell back to the first batch, and `Stream()` detaches the batches; the correlated-local route never set `Schema` at all. Both sites now record it at construction |

**The one exception to "an annotated leaf is immune", and how the gate found
it.** CIDR has no parquet annotation of its own either, so the writer stamps
it as UTF8 STRING — an annotation that describes the STORAGE truthfully and
loses only the name. The first cut of the hardened rule therefore let CIDR
revert to STRING, on the reasoning that the bytes and the rendering are
identical and only the type NAME differs.

`TestTypeMatrixTwoPath` refuted that in one run. The engine dispatches on the
TYPE, and CIDR and STRING do not behave identically everywhere:

- `minmax_c_cidr` — `SELECT MIN(c_cidr), MAX(c_cidr) FROM typemx` — DIVERGED,
  with the DAG (seeing STRING) answering correctly and the single-process arm
  (seeing the catalog's CIDR) answering NULL for both. The two paths
  disagreeing is exactly what #396 was about — and the NULL is a defect of its
  own, filed as **#417**: `MIN`/`MAX` over CIDR, IPV6 or UUID returns NULL on
  BOTH paths, because the six resolvers in `kernel/agg.go` enumerate only
  `TypeString, TypeBytes` among the bytes-backed types and return nil for the
  rest. Both arms take the same resolvers, so the two-path gate reads three
  matching wrong answers as agreement; only the momentary type disagreement
  above made one arm right and exposed it.
- `minby_scalar_c_cidr` — the DAG started ANSWERING a query the single-process
  arm refuses, re-opening the #392 asymmetry that #396 had just closed.

So the exception is narrow and explicit: a leaf annotated UTF8 STRING may
carry back the name CIDR, and nothing else (`declaredOverlayUTF8Types`, one
entry). Storage and rendering are unchanged by that relabel, which is what
makes it safe where STRING→IPv6 is not — IPv6's contract is exactly 16 bytes
and `GetValue` renders anything else as `""`. The adversarial table pins
STRING→UUID and STRING→BYTES as still refused over the same leaf, and
TIMESTAMP→CIDR as refused over a different one.

The lesson worth keeping: "the storage is identical" was not sufficient
grounds to drop a type name. The behaviour that matters attaches to the NAME,
and only a gate that runs both engines over the same column could say so.

A third defect the gates surfaced about THEMSELVES: **the process-killer gate
blamed the wrong corpus entry** whenever the fatal escaped on a goroutine the
query did not join — which is #392's shape exactly (the panic fires in
`aggregate_parallel_emit.go`'s emit goroutine). One `-count=2` run produced
both halves of the slide at once: `maxby_c_uuid` reported "no longer kills, so
#392 is FIXED" and the adjacent `minby_scalar_c_uuid` reported as a NEW
PROCESS KILLER, both false. Fixed here (#418): the child emits a
`TYPEMATRIX-DONE` marker after a settle window and the parent blames the entry
that started and never finished. 16 consecutive invocations clean afterwards,
still 22 killers against 22 pins. Worth noting because "delete this pin, the
bug is fixed" is an instruction a reader would act on.

Two issues filed rather than fixed in those commits:

- **#415 — the sort comparators report every ARRAY, ROW, MAP and VECTOR value
  EQUAL.** This is #394's residual: DECIMAL was the fifth type in that
  `default` arm and the only one fixed. `ORDER BY` over those types is a
  silent no-op and a sort-merge join on such a key matches every row against
  every row; the `cmp == nil` guard in `sort_merge_join.go:467` is dead code,
  because the resolvers return the always-equal closure rather than nil. The
  two-path gate cannot see it — both arms take the same comparator and agree.
- **#416 — a zero-row result declares no column TYPES on any path**, and none
  of its column NAMES on the correlated-local route. `CollectSink` captures
  its schema from the first batch it CONSUMES and `exec.Project` resolves its
  output types from the first input batch, so an empty result has no schema to
  publish and pgwire declares OID 25 (text) for every column. Typing it needs
  the output schema derived from the PLAN rather than from data flow.

**Update 2026-08-23 (rebased onto main's own #392 fix).** The commits above
landed on a branch that did not yet have main's independent fix for #392
(`MIN_BY`/`MAX_BY`'s declared output type, `minMaxDeclaredType` gaining the
value's own type instead of falling through to FLOAT64). Rebasing the two
together changes the ending this section gave #392: with BOTH fixes present,
`minby_scalar_c_cidr`/`_ipv6`/`_mac`/`_uuid` do not merely agree that they
refuse (the `tmdUnsupported` outcome above) — the single-process arm no
longer refuses at all, so all four fully AGREE on the VALUE, the same as
every other MIN_BY/MAX_BY shape over these types. `TestTypeMatrixTwoPath`
and `TestTypeMatrixMinByTwoPath` confirm this: all four `tmdUnsupported`
entries and `tmdColPin`'s `#396` case are deleted rather than kept, and both
gates pass with no exemption for these columns. #392 itself is closed by
that other fix, not by anything in this document's commits.

**Update 2026-08-23 (container codec closes #397 and #410).** §2a's "ARRAY/
ROW/MAP/VECTOR cannot cross a shuffle" (below) and §5's `#397` row are
**fixed** by `internal/engine/batch/container_codec.go`
(`EncodeContainerColumn`/`DecodeContainerColumn`): `writeColumnData`
(now `internal/worker/shuffle_format.go:289`, not `:415`) gained the
missing arm at `:333`, dispatching to `writeContainerData`, and the read
side — unified into `internal/wshf` since #422 — decodes it back via
`wshf.ReadColumn`'s container arm (`internal/wshf/decode.go:280`) calling
`batch.DecodeContainerColumn`. `#410` (the misattributed "second face" —
a whole-table MAP scan hitting #393's row-fallback mechanism from the DAG)
is also fixed; a gate re-run after #397 landed found no residual
divergence for it. `tmdUnsupported` in
`internal/coordinator/type_matrix_distributed_test.go` is empty for both.
This document's own §2a bullet and §5 table row are left as originally
written — a record of what the audit found at the time — rather than
edited in place, matching how #391/#392/#394-396/#402 are handled above.

## Why

A review found that `(*Vector).GetValue`'s `TypeBytes` arm
(`internal/engine/batch/vector.go:678`) returns a slice **aliasing the column's
byte arena**. `MIN_BY`/`MAX_BY` retain that box across batch boundaries
(`internal/engine/exec/aggregate.go:3888`), so once the pool recycles the batch
the aggregate answers with whatever was written over those bytes. Filed as #391.

No gate could see it. Not TPC-H, not ClickBench, not the DuckDB fingerprint
corpus, not the PostgreSQL oracle, not the two-path invariance suite, not the
shape fuzzer. The reason is the same in every case: **no corpus in this repo has
a top-level BYTES column.**

This audit asks the general question. Rows are the 22 column types; columns are
the consumer classes that can retain, re-key, re-order or re-encode a value.

---

## 1. The headline: three types out of twenty-two

Every differential corpus in the repo is built on **Int32, Float64 and String**.
ClickBench adds Int64 and Date. That is the whole type universe of the gates.

| Corpus | Where | Types present |
|---|---|---|
| TPC-H SF0.01 | `benchmarks/tpch/schema.go:9-120` | Int32, Float64, String. Dates are `TypeString` (`:81`, `:101-103`); money is `TypeFloat64`. **No NULLs at all** (`postgres_compare_test.go:243`). |
| ClickBench hits | `benchmarks/clickbench/hits_exec_test.go:46-75` | Int32, Int64, String, Date. `TypeBytes` is rewritten to `TypeString` at `:62-64`. |
| DuckDB fingerprint (243 entries) | `benchmarks/tpch/duckdb_compare_test.go:255` | the TPC-H three, plus Date/Timestamp as CASTs |
| PostgreSQL oracle | `benchmarks/tpch/postgres_oracle_test.go` | the TPC-H three; the loader `Fatalf`s on the other 14 (`postgresColumnType`, `:454-478`) |
| Two-path invariance (217 entries) | `benchmarks/tpch/two_path_invariance_test.go:670` | the TPC-H three |
| Shape fuzzer | `internal/oracle/shapegen/shapegen.go:31-38` | `Kind` had four members: Int, Float, Text, Date |
| Optimization-invariance | re-runs the above | inherits the same three |

**Nineteen of twenty-two types had no differential coverage of any kind.**

### 1a. CI runs three of the seven gates

`.github/workflows/ci.yml` (before this change) named `TestTPCHQueries`,
`TestTPCHOptimizationInvariance`, `TestStandaloneVsDistributedDifferential` and
the nested/Decimal battery. `TestDuckDBCompare`, `TestTwoPathInvariance`,
`TestFuzz*` and `TestPostgresOracle` are developer-invoked only.

### 1b. Three DuckDB pins are inert

`duckdbCorpus()` has four `knownBug:` strings and one `knownBugArm:`. `checkArm`
(`duckdb_compare_test.go:1717`) only relaxes when `knownBugArm` is set, so
`OuterJoinOnResidual` (`:710`), `FullJoinOnConjunctBuildSide` (`:865`) and
`OuterJoinExpressionKey` (`:1059`) are prose: all three pass fully gated today,
and their comments claiming the engine refuses the query are stale. There is no
`TestDuckDBCorpusPinsAreAccountable` sibling to the PostgreSQL one.

### 1c. Two pin mechanisms are skips, not ratchets

`twoPathQuery.knownBug` → `t.Skipf` (`two_path_invariance_test.go:377`) and the
fuzzer's `fuzzKnownDivergence` → `stats.skip` both remove the case from the run.
Neither can fail when the bug is fixed, which is the property ADR-0013 §Pins
requires. The gates added by this audit use a failing ratchet instead.

---

## 2. The matrix

`GATED` = a CI-run gate exercises it · `UNIT` = a package test only ·
`SMOKE` = a "round trip" test that asserts row count and never looks at values ·
`NONE` = nothing.

### 2a. Storage and transport

| Type | Parquet round trip | Shuffle WSHF | Spill run | Late-mat view |
|---|---|---|---|---|
| Bool | UNIT | NONE | UNIT | UNIT |
| Int32 | GATED | GATED | UNIT | GATED |
| Int64 | GATED | GATED | UNIT | GATED |
| Float32 | UNIT | NONE | **NONE** | UNIT |
| Float64 | GATED | GATED | UNIT | GATED |
| String | GATED | GATED | UNIT | GATED |
| Bytes | **SMOKE** | NONE | NONE | UNIT |
| Timestamp | UNIT | NONE | NONE | UNIT |
| IPv4 | **SMOKE** | NONE | NONE | UNIT |
| IPv6 | **SMOKE** | NONE | NONE | UNIT |
| CIDR | **SMOKE** | NONE | NONE | UNIT |
| MAC | **SMOKE** | NONE | NONE | UNIT |
| Port | **SMOKE** | NONE | NONE | UNIT |
| Protocol | **SMOKE** | NONE | NONE | UNIT |
| Duration | **SMOKE** | NONE | NONE | UNIT |
| UUID | **SMOKE** | NONE | NONE | UNIT |
| Date | **SMOKE** | NONE (bench only) | NONE | UNIT |
| Decimal | UNIT | GATED | **NONE** | UNIT |
| Array | GATED (String elem) | **cannot encode** | UNIT | UNIT |
| Row | SMOKE / GATED via `wadjet/` | **cannot encode** | UNIT | UNIT |
| Map | **SMOKE** | **cannot encode** | UNIT | UNIT |
| Vector | **SMOKE** | **cannot encode** | UNIT | UNIT |

Notes that matter more than the cells:

- The parquet writer emits **PLAIN only** (`file_writer.go:711`). Dictionary,
  delta and byte-stream-split are read-only paths for foreign files, and the
  delta decoders are reached only by fuzz targets that discard the result.
- **Nine types are lossy through a self-describing parquet file.**
  `buildLeafSchemaElement` (`file_writer.go:915`) writes no logical annotation
  for IPv4, IPv6, MAC, Port, Protocol, Duration, Bytes or UUID, so
  `TypeIDFromSchemaNode` (`file_reader.go:502`) cannot recover them. Every
  "round trip test" for those nine asserts row count only.
- `reader.go:342 decodeAllValues` has **no DECIMAL and no VECTOR case** and a
  `default: return nil` at `:404`, so a DECIMAL or VECTOR leaf inside an
  ARRAY/MAP/ROW decodes to **silently all-NULL**.
- `unpackAllPresent`/`unpackWithNulls` have no VECTOR case and no default, so
  `Reader.ReadRows` on a VECTOR column returns all-nil.
- ARRAY/ROW/MAP/VECTOR **cannot cross a shuffle**: `writeColumnData`
  (`internal/worker/shuffle_format.go:415`) has no arm and errors at `:430`.
  Only the ARRAY error had a test.

### 2b. Execution

| Type | Filter kernels | GROUP BY key | Sort comparator | Window PARTITION/ORDER | Join key |
|---|---|---|---|---|---|
| Bool | ✓ (`IN` → drops all rows) | ✓ | ✓ | ✓ | ✓ |
| Int32/Int64/Port/Protocol/Date/Timestamp/IPv4/MAC/Duration | ✓ (`IN` nil for Float32/IPv4/MAC/Duration) | ✓ | ✓ | ✓ | ✓ |
| Float32 | `IN` → **drops all rows** | ✓ | ✓ | ✓ | ✓ |
| Float64 | ✓ | ✓ | ✓ | ✓ | ✓ |
| String/IPv6/CIDR/UUID | ✓ | ✓ | ✓ | display-string order | ✓ |
| **Bytes** | **every kernel `default`s to nil** | in-memory ✓, spill-merge key → `"<unknown>"` | ✓ | **compares equal always** | ✓ |
| **Decimal** | **every kernel `default`s to nil** | float64-lossy | **comparator returns 0** | formatted-string order | float64-lossy |
| **Array/Row/Map/Vector** | **every kernel `default`s to nil** | **key byte `'?'`** | **comparator returns 0** | **compares equal always** | **key byte `'?'`** |

Ranked by blast radius, with anchors:

1. `appendColumnValue` `default: append(buf, '?')` —
   `internal/engine/exec/aggregate.go:5931`. One constant byte. GROUP BY,
   DISTINCT, UNION/INTERSECT/EXCEPT, `COUNT(DISTINCT)`, `APPROX_DISTINCT` and
   hash-join keys over ARRAY/ROW/MAP/VECTOR all collapse to one group / one
   distinct value / a cross join. One default feeds six operators.
2. `consolidateBuild` — `internal/engine/exec/join.go:1553`. No `default:`, no
   arm for Array/Row/Map/Vector. Nulls are copied, values are not, and `:1583`
   discards the originals. Fires only when `len(buildBatches) > 1 && rows ≤ 2M
   && trackerUse < 30%`, i.e. data- and memory-dependent.
3. `vecFloat64` `default: return 0` — `internal/engine/exec/window.go:984`.
   Window `SUM`/`AVG` over DECIMAL, DURATION, PORT, PROTOCOL, DATE, IPv4, MAC
   computes **0** and marks the row valid.
4. `InFilter.Execute` `return nil, nil` — `internal/engine/exec/filter.go:465`.
   `WHERE decimal_col IN (…)` drops **every row**. The function is at 0.0%
   coverage; the planner applies no type guard (`plan.go:9171`).
5. `resolveFloat64Extractor` `default: return nil` —
   `internal/engine/exec/aggregate.go:1049`. STDDEV / VARIANCE / MEDIAN /
   PERCENTILE / MODE / CORR / COVAR / `MIN_BY`'s ordering key over PORT,
   PROTOCOL, DURATION, IPv4, MAC return **NULL**. Same class as #353, which was
   fixed for Date only.
6. **`MIN_BY`/`MAX_BY` `state.bestVal = v1.GetValue(row)`** —
   `internal/engine/exec/aggregate.go:3888`. No deep copy. This is #391.
7. `appendKeyValue` `default: "<unknown>"` — `internal/engine/exec/sort.go:1007`.
   Live as the k-way merge key for drained partial runs
   (`aggregate_partial_drain_cursor.go:411`), so a **BYTES** group key is
   distinct in memory and collapses to one group after a drain: the same query
   answers differently under memory pressure.
8. `ResolveSortCompare` / `…NullsLast` defaults —
   `internal/engine/exec/kernel/sort.go:21`, `:255`. A comparator reporting
   every row equal. `ORDER BY decimal_col` is a stable no-op; a sort-merge join
   on a DECIMAL key matches every row against every row
   (`sort_merge_join.go:466`).
9. `compareVectorValues` default — `internal/engine/exec/window.go:965`.
   DECIMAL compares as its formatted string (`"10.00" < "9.00"`); BYTES/VECTOR/
   ARRAY/ROW/MAP always compare equal. Drives PARTITION BY boundaries and
   RANK/DENSE_RANK/PERCENT_RANK/CUME_DIST peer groups.
10. `ResolveRowSum`'s int32 arm is `{Int32, Port, Date}` while `ResolveRowMin`'s
    is `{Int32, Port, Protocol, Date}` — `internal/engine/exec/kernel/agg.go`.
    `SUM(protocol_col)` is NULL because the two lists disagree by one type.

For contrast, three places get this right and are the model: the columnar spill
codec (`join_spill.go:603`, all 22 types + a loud error), `emitAcc`
(`aggregate_partial_spill.go:349`), and `WindowMinMaxType`
(`window.go:157`) — which whitelists the types whose boxed form round-trips and
**declines** the rest rather than guessing.

### 2c. The aliasing / value-retention class

The producers that hand out a pointer into a vector's storage:

| Producer | Anchor | Aliases for |
|---|---|---|
| `BytesColumn.Value` | `vector.go:220` | BYTES, STRING, IPv6, CIDR, UUID (raw arena slice) |
| `BytesColumn.UnsafeStringValue` | `vector.go:253` | same — **doc warns** |
| `Vector.GetValue` `case TypeBytes` | `vector.go:679` | **BYTES** — no warning |
| `GetValue` ARRAY/MAP → `Child.GetValue` | `vector.go:713` | any container with a BYTES leaf |
| `GetValue` ROW → `child.GetValue` | `vector.go:724` | ROW with a BYTES field |
| `GetValue` view redirect | `vector.go:652` | propagates the above to the view's BASE |
| `RowAt` / `ToRows` | `batch.go:296`, `:305` | inherits all of the above |

Everything else copies: STRING via `string(...)`, IPv4/IPv6/MAC/UUID/DATE/DECIMAL
by formatting, VECTOR by `make`+`copy`. **BYTES is the only leaf type that
aliases** — directly, or as a leaf under a container.

The retaining consumers, ranked:

| # | Consumer | Anchor | Reachable types |
|---|---|---|---|
| R1 | HashAggregate raw-row spill buffer | `aggregate.go:1219-1220` | BYTES + nested-with-BYTES, in **every column** |
| R2 | HashAggregate group-key boxing (`keyValues`, `h.keys`) | `aggregate.go:3002`, `:3330`, `:3523`, `:3594`; read `:4421` | BYTES; DECIMAL in practice (#399) |
| R3 | `MIN_BY`/`MAX_BY` `bestVal` | `aggregate.go:3888` | BYTES + nested-with-BYTES (#391) |
| R4 | Global-window streaming state | `window_global.go:121-147`, `:468-507` | latent — `runMerger.Next` mints fresh batches today |
| R5 | Coordinator row accumulation | `coordinator.go:724`, `:1672`; `plan.go:8402` | safe **only** because `CollectSink.Consume` Detaches |

The batch-reuse paths that make those reachable: the physical scan pool
(`plan.go:10916`), the scanner pool (`scanner.go:189`), Project's output pool
(`project.go:207`), the join emit pools (`join.go:2450`), `probeEmitBuf`
(`join_emit_reuse.go:54`, default ON), `aggPreProject.computedVectors`
(`plan.go:7997`), and the partitioned-agg shared batch
(`partitioned_agg.go:88`). All release through `pipeline.go:237/261/330/418/568`,
where `b.Release()` sits on the line after `Consume` returns.

**Existing defences before this audit:** `Detach`/`DetachPool`, the
`Vector.Claimed()` gate in `probeEmitBuf.reusable()`, and exactly one
adversarial test — `internal/engine/exec/join_emit_reuse_test.go`, a reuse-on
vs reuse-off differential across 8 shapes × 8 consumers. Its schemas contain
Int64, String, Float64 and Bool: **no BYTES**, no containers. Its
`hash-aggregate` consumer groups on a `TypeString` column, which is the one
bytes-backed type that copies. It also toggles only `WADJET_VECTOR_REUSE` and
never exercises `BatchPool` recycling. **No poison/scribble mode existed
anywhere in the tree.**

### 2d. Expression kernels

361 registered scalar functions. **24 appear in any gate corpus — 6.6%.**

| Group | N | Gated | NULL arm asserted |
|---|---|---|---|
| network | 118 | **0** | 92 |
| string | 55 | 12 | 11 |
| math | 44 | 4 | 9 |
| temporal | 30 | 3 | 10 |
| cast/conversion | 20 | **0** | 6 |
| misc/system | 14 | **0** | **0** |
| collection | 13 | **0** | **0** |
| hash | 10 | **0** | 5 |
| conditional | 6 | 5 | **0** |
| json | 4 | **0** | **0** |
| pgcompat + vector + embed | 47 | **0** | **0** |

TPC-H's 22 queries exercise exactly **one** scalar function: `substr`.

Highest-value gaps:

- `Cast.Eval` `default: return v` (`expr.go:4485`) — **17 of 22 type names**
  reach it and the operand is returned untouched: no error, no NULL, no
  coercion. `CAST(x AS BOOLEAN)`, `AS SMALLINT`, `AS UUID`, `AS INET`,
  `AS BYTEA`, `AS JSON` are all silent no-ops. `#340` (CAST AS DATE) was fixed;
  the class it belonged to is still open for everything else.
- `xxhash64` and `murmur3` are hand-rolled and their only tests assert **hex
  length and determinism** (`trino_gap_test.go:196-231`), where md5/sha256/crc32
  are compared against the Go stdlib. The ≥32-byte xxhash branch
  (`expr.go:7951`) is at 0% coverage.
- `stringInputFuncs` / `temporalInputFuncs` (`expr.go:1589`, `:1652`) are
  hand-maintained allow-lists — the mechanism `rettype.go:14-22` says caused
  four production incidents. Live gaps: `char_length`/`octet_length`/
  `bit_length`/`position` are absent from the string list, so
  `CHAR_LENGTH(date_col)` counts the digits of an epoch-day integer;
  `to_iso8601` is absent from the temporal list, so `TO_ISO8601(date_col)`
  reads epoch-days as milliseconds.
- **No scalar kernel produces** IPv4, IPv6, CIDR, MAC, Port, Protocol, Duration,
  UUID, Date, Decimal, Row, Int32, Float32 or Timestamp — `RetTypeOf` is never
  called and `RetTimestamp` never used. Fourteen of the 22 types are read-only
  as far as the expression layer is concerned.

### 2e. pgwire

`pgTypeOID` (`internal/server/pgwire/server.go:1975`) has 8 arms and
`default: return 25 // text`. **14 of 22 types fall through**, which is #305's
exact shape.

| Failure | Types | Class |
|---|---|---|
| `%v` renders `[]byte` as `"[222 173]"` in BOTH formats | Bytes | wrong bytes |
| binary format writes 4/8 raw BE bytes under **OID 25** | Port, Protocol, Duration | wrong bytes |
| text and binary arms produce **different bytes**; ROW/MAP iterate a Go map | Array, Row, Map, Vector | wrong bytes, nondeterministic |
| right value, wrong OID | IPv4, IPv6 (→ 869), CIDR (→ 650), MAC (→ 829), UUID (→ 2950), Decimal (→ 1700) | typed clients fail or downgrade |
| `pg_attribute.atttypid` = 25 while `format_type` = `numeric` | Decimal | the catalog contradicts itself |
| `paraminfer` infers OID 25 → `quoteLiteral` | Port, Protocol, Duration, Decimal | `WHERE port = '80'` — silent wrong row set |

**14 types have no wire test at all**; Float32 has a unit test only. The wire
arm's corpus (`postgres_wire_test.go:521`) puts 7 types on the wire.

---

## 3. Ranked un-gated cells (top 15)

Silent wrong answer > crash > error. Every one of these was invisible to every
gate before this audit.

| # | Cell | Anchor | Failure |
|---|---|---|---|
| 1 | GROUP BY / DISTINCT / set-op / join key over ARRAY, ROW, MAP, VECTOR | `aggregate.go:5931` | one constant key byte → one group, `COUNT(DISTINCT)` = 1, join becomes a cross join |
| 2 | `MIN_BY`/`MAX_BY` value retention, BYTES | `aggregate.go:3888` + `vector.go:679` | **#391** — overwritten bytes |
| 3 | `MIN_BY`/`MAX_BY` output type for 8 types | `plan.go:10005` | **#392** — kills the process on the emit goroutine |
| 4 | any read of a MAP column | `util.go:151`, `plan.go:11092` | **#393** — kills the process on the scan worker |
| 5 | network/UUID values on the stage DAG | result assembly | **#396** — `10.0.0.5` → `167772165`, 37 corpus entries |
| 6 | join build consolidation, nested + VECTOR | `join.go:1553` | payload columns silently emptied, memory-dependent |
| 7 | BYTES group key after a partial drain | `sort.go:1007` | every group merges into one **under memory pressure only** |
| 8 | `ORDER BY` over DECIMAL | `kernel/sort.go:21`, `sort.go:903` | **#394** — lexicographic, numeric or a no-op depending on the path |
| 9 | window `SUM`/`AVG` over DECIMAL/DURATION/PORT/PROTOCOL/DATE/IPv4/MAC | `window.go:984` | computes 0 and marks it valid |
| 10 | scalar-subquery threshold retention | (see #398) | row set changes with allocation order |
| 11 | DECIMAL GROUP BY key retention | `aggregate.go:3523` | **#399** — different keys after recycling |
| 12 | GROUP BY / DISTINCT over IPV6, UUID with partitioned-agg off | key render | **#395** — every key comes back empty |
| 13 | `WHERE <t> IN (…)` over BOOL/FLOAT32/BYTES/DECIMAL/IPv4/MAC/DURATION | `filter.go:465` | drops every row; 0.0% coverage |
| 14 | STDDEV/VARIANCE/MEDIAN/PERCENTILE/MODE/CORR/COVAR over PORT/PROTOCOL/DURATION/IPv4/MAC | `aggregate.go:1049` | returns NULL, no error |
| 15 | pgwire BYTES / Port / Protocol / Duration in binary format | `server.go:3000-3040` | wrong bytes under a wrong OID |

Below the line but worth naming: nested DECIMAL/VECTOR decoding to all-NULL
(`reader.go:404`), `CAST` as a silent no-op for 17 type names
(`expr.go:4485`), and `hashRowsIntoPartitions` mixing a constant for BOOL,
DECIMAL and nested keys (`partitioned_shuffle_sink.go:1158`).

---

## 4. What was built

### `internal/engine/batch/poison.go` — poison on release

A pooled batch's storage is undefined the moment `Release()` hands it back;
anything keeping a value past that call must own it (`Detach`). Nothing enforced
it, so whether a violation produced a wrong answer depended on what the next
batch happened to write. `SetPoisonOnRelease(true)` fills every released,
unclaimed value arena with a recognisable pattern, making the undefined
behaviour defined and loud. It writes exactly where a real recycle is free to
write: only when `b.pool != nil`, never through a view, value arenas only. Off
by default; cost when off is one atomic load per batch.

A per-vector `Claim` deliberately does **not** exempt a column, because nothing
in the pool honours it — `resetVectorForReuse` clears `claimed` and reuses the
arena, on an assumption that `partitioned_agg.go`'s `selView` already violates.

### `internal/oracle/typematrix` — the fixture and corpus

Two tables (18 flat types, 4 container types — separate because
`readBatchDirect` decides row-vs-columnar on the whole table schema) plus a
dimension table. 5000 rows, every column nullable and nulling on its own stride
coprime with the batch and row-group sizes. **270 corpus entries generated from
the column table**, so a 23rd type is covered by adding one row to `Columns()`.

### Four new gates

| Gate | File | What it compares |
|---|---|---|
| `TestTypeMatrixNoProcessKillers` | `wadjet/type_matrix_crash_test.go` | the SET of queries that kill a process, by re-execing a child per killer |
| `TestTypeMatrixBatchReuse` | `wadjet/type_matrix_test.go` | poisoned pool vs clean pool |
| `TestTypeMatrixOptimizationInvariance` | `wadjet/type_matrix_test.go` | #287's kill-switch differential, over all 22 types |
| `TestTypeMatrixTwoPath` | `internal/coordinator/type_matrix_distributed_test.go` | stage DAG vs single process, over all 22 types |
| `TestTypeMatrixFuzz{BatchReuse,OptimizationInvariance}` | `wadjet/type_matrix_fuzz_test.go` | the same two contracts over GENERATED SQL |

All are in CI.

### Fuzzer type universe

`shapegen.Kind` gained `KindOpaque` and `KindDecimal`, and
`shapegen.TypeMatrix()` derives a generation universe from `typematrix.Columns()`.
`genSelfJoin` no longer hardcodes TPC-H table names (it panicked on any other
schema); `Table.SelfJoin` declares it instead.

### Two harness defects fixed

- `oracle.CheckOrder` placed NULLs last in **both** directions, contradicting
  ADR-0012 (PostgreSQL decides semantics) and the `default_null_order` the
  DuckDB arm configures. A correct DESC result with NULLs first was reported as
  unsorted. Invisible because TPC-H has no NULLs.
- `Query.OrderKeys()` now declines the absolute order check when a term reads a
  type whose RENDERED form does not order like its value — IPv4 `"10.0.0.10"`
  sorts before `"10.0.0.9"` as text and after it as an address.

---

## 5. Divergences the new gates surfaced

| Issue | What |
|---|---|
| #392 | `MIN_BY`/`MAX_BY` declares FLOAT64 for every type outside a six-case switch → **kills the process** (16 corpus entries) |
| #393 | any read of a MAP column **kills the process**, `SELECT *` included (6 entries) |
| #394 | `ORDER BY` over DECIMAL is path-dependent: lexicographic, numeric, or a no-op |
| #395 | GROUP BY / DISTINCT over IPV6 or UUID loses the key VALUE with partitioned-agg off |
| #396 | the stage DAG returns IPV4/IPV6/MAC/UUID in raw storage form (37 entries) |
| #397 | ARRAY/ROW/MAP/VECTOR cannot cross a shuffle; container payloads arrive as MAP vectors |
| #398 | a scalar subquery's threshold is read after its batch was released |
| #399 | DECIMAL GROUP BY keys are read after their batch was released |
| #400 | panics on the scan-worker and parallel-emit goroutines are unrecoverable |
| #401 | a qualified column in `WHERE` fails to resolve with `scan-filter` off |
| #402 | GROUP BY + ORDER BY + LIMIT returns a different top-N with partitioned-agg off |

Every one is pinned with a **two-way ratchet**: the comparison still runs, a
pinned divergence is logged rather than failed, and a pin that starts agreeing —
or that names an entry the corpus no longer contains — **fails the gate**
(ADR-0013 §Pins).

---

## 6. Left as plan

### PostgreSQL oracle: extend the fixture to the types PostgreSQL also has

The mapping switch is five minutes per type; the work is in `internal/oracle`.
`postgresColumnType` (`benchmarks/tpch/postgres_oracle_test.go:454`) `Fatalf`s
on 14 types, `copyInto` (`:482`) hands pgx whatever Go value the row map holds,
and `normalizePostgresValue` (`:549`) must render PG's return **byte-identically**
to wadjet's.

| Type | PG type | Cost | Why |
|---|---|---|---|
| Bytes | `bytea` | LOW | pgx returns `[]byte`, already normalized to `string(x)` |
| MAC | `macaddr` | LOW | `net.HardwareAddr`'s `String()` matches |
| Port / Protocol | `integer` / `smallint` | LOW | int32 both ways |
| Duration | `bigint` | LOW | int64 both ways |
| CIDR | `cidr` | LOW-MED | `netip.Prefix` renders with the `/n` wadjet also carries |
| UUID | `uuid` | MED | pgx returns `[16]byte`; needs a canonical-string case |
| IPv4 / IPv6 | `inet` | MED | `netip.Prefix` renders `1.2.3.4/32`, wadjet renders `1.2.3.4` |
| Decimal | `numeric(p,s)` | MED-HIGH | wadjet keeps scale as a string, PG flattens to float64: `"0.10"` vs `"0.1"` |
| Timestamp | `timestamp` | HIGH | wadjet boxes a bare epoch-millis `int64` **by design** (`vector.go:656` — it is also the GROUP BY key and the spill encoding) |
| Array / Row / Map / Vector | `[]`, composite, `jsonb`, `real[]` | HIGH | needs `canonCell`/`fingerprintCell` recursion and a cross-engine spelling |

Recommended shape: a **probe table created inside `runPostgresWireArm`** rather
than a new `AllTables` entry — one `CreateTable` + one `Ingester` on the wadjet
side, one `CREATE TABLE` + `copyInto` on the PG side, ~80 lines, no committed
parquet fixture and no change to the DuckDB arm. Flipping TPC-H's own date
columns to `date` would move the DuckDB fingerprints and every TPC-H query, and
is not worth it.

### DuckDB fingerprint corpus

Adding type-matrix entries needs a regenerated baseline from a live DuckDB
(`WADJET_REGENERATE_DUCKDB_BASELINE=1`, requires `/tmp/duckdb`), which is not
available here — so those entries are gated on the two-path and batch-reuse arms
instead. Two prerequisites before that gate can carry BYTES at all:
`canonCell`/`fingerprintCell` render `[]byte` with `string(b)` **unescaped**
through a `'|'`-joined encoding (`oracle.go:196`), and DECIMAL's
string-vs-float rendering has no cross-engine rule.

### Other named gaps not closed here

- ~~Wire the three inert DuckDB pins (§1b)~~ **RESOLVED 2026-08-23**: all
  three passed fully gated with no divergence once checked, so the stale
  `knownBug` prose was deleted rather than converted to `knownBugArm` (see
  the 2026-08-23 update at the top). `TestDuckDBCorpusPinsAreAccountable`
  itself is still not written — the PostgreSQL oracle's sibling test exists
  and this one doesn't, independent of whether any pin is currently inert.
- Convert `twoPathQuery.knownBug` and `fuzzKnownDivergence` from skips into
  ratchets (§1c).
- ~~A logical annotation for the nine SMOKE types so a self-describing file
  round-trips~~ **RESOLVED 2026-08-23** by the footer's `wadjet.schema` key
  (#396, see the second-wave update at the top) — for files written after
  it. A parquet round-trip battery that asserts VALUES for those nine is
  still not written; `TestDeclaredSchemaRoundTrip` asserts the TYPES only.
- Per-type wire tests for the 14 pgwire types that have none.
- **New 2026-08-23: the scan `BackingPool` is not covered by
  poison-on-release.** `poisonBatch` (`internal/engine/batch/poison.go`)
  fires from `(*RecordBatch).Release` and only when `b.pool != nil` — the
  condition that distinguishes a batch a `BatchPool` owns from one nobody
  does. The row-group output backing pool added on main after this audit
  (`7890eb5`, "pool the row-group output backing behind release+claim")
  recycles through a SEPARATE path, `BackingPool.Recycle`, with its own veto,
  and the batches it mints carry `pool == nil` — so poison-on-release never
  arms for them, and `TestTypeMatrixBatchReuse` cannot see a retention defect
  in that pool no matter how the type matrix corpus grows. Closing this needs
  either a poison hook in `BackingPool.Recycle` itself or widening
  `poisonBatch`'s condition to cover backing recycled outside `BatchPool`;
  left as a follow-up.
