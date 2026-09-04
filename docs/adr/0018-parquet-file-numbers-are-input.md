# ADR-0018: A parquet file's own numbers are input, not fact

Status: Accepted (landed 2026-08-23, `09b19d2`..`94143bc`, on the hardening
arc that began with `20049f9`; ceilings measured against pyarrow 23.0.1,
parquet-go and wadjet's own writer). Amended 2026-08-23 with §4, "the
writer's box for a value is the reader's box for it", after the same arc's
review found read → write was not the identity for DECIMAL (#429) and that
`ReadRowGroup` and `ReadRows` were two readers that disagreed about a nested
column (#428). A second compatibility note follows the pre-#409 one, for the
`DECIMAL(p > 18)` files the writer produced before #429 (#437). Amended
2026-08-25 with §8, "a declared type is restored at every depth, or the paths
do not agree", after the same self-describing-footer channel §3 and §4 rely
on was found to stop at the top level: an IPv6 or a UUID inside a ROW, ARRAY
or MAP read back as the empty string while the flat column read correctly
(#589). Amended 2026-09-03 with §9, "a DECIMAL's declaration is half of every
value, so the CATALOG's is the one that counts", after two files of one table
declaring one column at two scales were found to answer a plain projection
100x wrong on the single-process engine and 100x wrong the OTHER way on the
stage DAG (#707).

## Context

Everything a parquet reader navigates by comes out of the file: the footer's
row counts and chunk offsets, and each page header's value counts and sizes.
All of it is thrift the reader has parsed and nothing more. Several of those
fields become a slice index or an allocation size *before* anything has looked
at the bytes they describe.

The first pass over this (`20049f9`, `ba0ad8e`) refused the negatives — five
of the six whole-file-mutation crashers were a negative `data_page_offset`
used directly as a slice index, and the sixth was a negative `num_rows`
reaching `makeslice`. Refusing negatives was necessary and not sufficient. A
review of the result found four more classes, and each was a different way of
believing the file:

- **Believing a number because another number in the same file agrees.**
  `rg.num_rows` was held to `md.num_rows`, which is a varint out of the same
  footer. `num_rows = 2^40` on a two-row file reached
  `batch.NewRecordBatch` with 128 GiB of bitmap and died as `fatal error:
  runtime: out of memory` — unrecoverable, so in a worker process it is the
  worker. `2^30` was *accepted*, and decoded after allocating gibibytes.

- **Tolerating a claim on the theory that writers are sloppy.** `chunkRange`
  CLAMPED a chunk's end to the file, documented as tolerance for writers that
  round `total_compressed_size` up. No writer does. What the tolerance bought
  was a silent wrong answer: an overstated size reaches into the NEXT column's
  pages, the page loop reads them as more pages of this column, and a 128-row
  chunk comes back holding 64 of its own values and 64 of a neighbour's, with
  `err == nil`.

- **Sizing an allocation from a header with no floor under it.**
  `MaxPageValues` was `2^28` — a gibibyte of `int32` per column, and the scan
  fans out per column, bought with a twenty-byte page header. Separately,
  `findDeltaDataEnd` kept walking blocks after the body ran out (its varint
  reader returns 0 without advancing at the end), allocating one bit-width
  array per block against a value count the header had claimed: a seven-byte
  page body cost 3.6 ms.

- **Two readers disagreeing about the same bytes.** The row path refused a
  zero-length entry in a UUID column while the native columnar path read it;
  two of the three PLAIN byte-array walks stopped at a truncated page and
  returned the remaining rows as empty strings while the third refused the
  page; and both VECTOR copy arms answered a non-positive dimension by
  returning `nil` having written nothing into a POOLED vector — so the column
  came back holding another query's floats.

## Decision

**Three rules, applied to every number a parquet file states about itself.**

### 1. Bound it before it sizes anything, and say which kind of bound it is

Every field that becomes an allocation size or a slice index is checked at the
point it enters the package, and again at the allocation site when the two are
far apart (`CheckRowGroupRowCount` runs at the native batch and at the row
reader's two per-column loops, because the number reaches an allocator through
several doors and only one of them is the validated open).

A bound is one of two things, and the code says which:

- **Exact**, derived from the format's own invariants. `md.num_rows` is
  exactly the sum of its row groups'. For a FLAT leaf, a chunk's page value
  counts sum exactly to the row group's rows. These are the checks that catch
  a single flipped varint wherever it landed.
- **A policy ceiling**, stated as one and justified by measurement, in the
  same class as `footerMaxSize` and `maxPageBodyBytes`. A file past it is
  refused *by name*, never silently truncated or clamped.

| Ceiling | Value | Measured against |
|---|---|---|
| `MaxRowsPerRowGroup` | 2^26 | pyarrow's `max_rows_per_group` default is 2^20; parquet-mr and Spark size row groups in BYTES (128 MiB); wadjet's own default is 128 Ki. pyarrow caps a column chunk at 2^26 values, which is the figure this must not refuse. |
| `maxRowsPerFileByte` | 2^12 rows/byte | The densest files pyarrow will produce — one all-null or one constant column, zstd, dictionary on, maximal pages — run 210–464 rows per byte at 1e6, 1e7 and 1e8 rows. The densest file in this repo's corpus is 0.42. |
| `MaxPageValues` | 2^24 | parquet-cpp caps a data page at 20,000 values (`DEFAULT_MAX_ROWS_PER_PAGE`) and parquet-mr at 20,000 rows whatever the byte target. pyarrow asked for 2 GiB pages over 40 M BOOLEAN rows still wrote pages of exactly 20,000, and the largest page in this repo's whole corpus is the same 20,000. |

Where no sound bound exists, the ceiling is named as policy rather than
dressed up as arithmetic. Two places are like this and both are documented at
the constant: RLE encodes arbitrarily many equal values in a handful of bytes,
so neither a level count nor a DELTA `total_values` can be bounded by the body
that carries it. The answer there is `MaxPageValues` plus, where the caller
knows it, the exact row-group bound — and, for DELTA, growing the result
instead of sizing it from the header.

### 2. A claim the file contradicts elsewhere is refused, not reconciled

Row counts must add up. Column chunks must tile the data region without
overlapping and without crossing into the footer (`ValidateChunkLayout`) —
measured across 44 files from pyarrow (four codecs, format 1.0 and 2.6, with
and without the page index, one and many row groups), parquet-go and wadjet's
own writer: worst inter-chunk gap zero, worst overlap zero. There is no
tolerance band, because there is nothing to tolerate.

The corollary binds the writer: the reader's ceilings are ceilings on what
this package may WRITE. `writeDataPage` refuses to emit a page past
`MaxPageValues` and names the knob, because `pageRowRanges` splits by bytes
and declines to split BOOLEAN, INT96, FIXED_LEN and nested leaves at all — a
large enough `RowGroupSize` would otherwise produce a file wadjet writes and
cannot read back.

### 3. The reader paths must agree

A file is readable through all of the decode paths or through none of them,
and a value means the same thing on each. This is ADR-0013's two-path
invariance applied inside the decoder, where there are four paths, not two:
the full columnar copy, the selection-aware copy, the lengths-only copy, and
the row reader.

Concretely, from this arc: the three PLAIN byte-array walks refuse a truncated
page in the same words; a zero-length entry in a fixed-width column
(IPV6, UUID) is NULL on the row path and on the columnar path, decided by the
DESTINATION's declared type rather than the file's recovered one; and a
non-positive VECTOR dimension is an error rather than a silent no-op over
pooled contents.

The write side of the same rule follows PostgreSQL (ADR-0012): an unparseable
network literal is an **error naming the column and the row**, not a zero
value, and the empty literal is an **absence** written as NULL. "" is the one
input for which "a value" had no stable meaning — as a value it is a
zero-length entry in a column whose entries are all sixteen bytes, which one
reader called an error and the other called a value.

### 4. The writer's box for a value is the reader's box for it

"The paths agree" is not only a statement about two readers. A reader and a
writer that disagree about what a boxed value MEANS turn `read → write` into
a transformation, and the one job in this system that is read → write over a
whole table — compaction — then rewrites the table into something else and
deletes the inputs.

Stated for the one type where the two conventions were inverses:

**The in-memory box for a DECIMAL value is the UNSCALED integer at the
column's declared scale.** 3.25 in a `DECIMAL(9,2)` column is the int64 325.
That is what the format stores, what `Reader.ReadRows` hands back, and what
`batch.Int128`/`DecimalColumn` hold. An INTEGER box (`int`, `int32`, `int64`,
`parquet.Decimal128`) is therefore already scaled and is written verbatim;
only a REAL box (`float64`, `float32`) or a numeric STRING carries a decimal
point, and only those are multiplied by 10^scale on the way in.

The writer used to read every integer box as the WHOLE number and multiply,
so each compaction pass over a `DECIMAL(p, s>0)` column multiplied it by
10^s — ×100 per pass at scale 2, silently, over the inputs (#429). One
generation cannot see it, because the first write is from ingest boxes; the
property to gate is `read(write(read(f))) == read(f)`, run at least twice.

The corollary about ceilings has a twin about ENCODINGS: **a physical
encoding this package chooses must be one the format allows for that logical
type.** DECIMAL's physical type is a function of its PRECISION — INT32 to 9
digits, INT64 to 18, and FIXED_LEN_BYTE_ARRAY beyond — and picking it from
the TypeID alone produced `DECIMAL(38,10)` annotated over an INT64 leaf,
which the Apache implementation refuses to OPEN ("Decimal(precision=38,
scale=10) cannot be applied to primitive type INT64"). Every wadjet table
with a wide DECIMAL column was unreadable outside wadjet, and the widest
values had no encoding here at all.

### 5. A statistics bound can be silent about a value it never recorded

(Added 2026-08-24, from the #459/#474 fold-in review.) The four rules above
are about believing a NUMBER the file states. A statistics bound is a
narrower kind of claim than a row count or an offset: it is not wrong the way
an overstated `total_compressed_size` is wrong, but it can still be
*incomplete* in a way pruning must not treat as complete — and a float
column's MIN/MAX is exactly that shape.

The parquet format excludes NaN from a column's min/max by specification, and
wadjet's writer (`leafBuffer.updateStatsF64`/`updateStatsF32`,
`internal/storage/parquet/file_writer.go`) follows it — but by accident of
implementation, not by a NaN-aware check. Both accumulators seed from the
FIRST value they see (`!lb.hasStats || v < lb.minF64`) and every later
comparison against a NaN accumulator is IEEE-false, so a NaN's actual effect
on the stored bound depends on where it falls in the row group:

- **NaN elsewhere in the row group.** The min/max accumulated from the OTHER
  values is correct and finite — NaN never wins a `<`/`>` against anything,
  itself included, so it is silently skipped exactly as the format asks. The
  footer's bound is a true statement about the non-NaN rows, but it is SILENT
  about whether a NaN row is also present: `[1, 5]` is what a row group
  holding `{1, 3, 5}` writes, and it is also what one holding `{1, 3, 5, NaN}`
  writes.
- **NaN is the FIRST value.** `!lb.hasStats` is true on that first call, so
  `lb.minF64`/`lb.maxF64` are seeded to NaN unconditionally — and every
  subsequent comparison against a NaN accumulator is IEEE-false, so nothing
  after it can ever replace it. The row group's real finite min and max are
  lost, not merely silent: the footer stores NaN as both bounds
  (`buildStats` writes them through with no NaN check of its own).

PostgreSQL's float order (ADR-0012 item 8) makes the silence load-bearing: NaN
sorts ABOVE every value, so `> c`, `>= c` and `<> c` are TRUE for a hidden
NaN row. Pruning a row group on a bound that cannot see that row would delete
rows the filter keeps — a prune reading the predicate differently from the
filter, which this ADR's whole point forbids. `CanPruneRowGroup`
(`internal/engine/scan/pushdown.go`) therefore declines `>`, `>=` and `<>`
for ANY float-typed bound, poisoned or not. `=`, `<` and `<=` need no
exception: NaN satisfies none of them against a finite constant, whether or
not one is hiding, so the bound decides those correctly either way — and in
the poisoned case, `compareValuesOK`'s own `math.IsNaN` check refuses the
comparison outright (`ok=false`), which happens to decline pruning for every
operator there too, not only the three named above. That refusal is a
correct side effect of a NaN-comparison guard written for a different reason
(#442's cross-type coercion), not a deliberate second line of defense — the
`>`/`>=`/`<>` decline in `CanPruneRowGroup` is the one this ADR asserts by
name, and is what a test should gate rather than the accident it currently
also survives on.

The general form: **a statistics bound proves what it recorded, and is
silent — not wrong — about a value the format told the writer to leave out.**
A predicate whose truth depends on that excluded value's presence cannot be
answered from the bound and must decline to prune, the same rule §1's
ceilings apply to a claim the file never made in the first place.

### 6. A bound in the wrong ORDER is not a bound

(Added 2026-08-24, from #492's second pass.) §5 is about a bound that is
*silent* about a value. This is the neighbouring failure: a bound that
records every value faithfully, in an order the engine no longer uses.

A CIDR column is stored as plain address TEXT (`internal/storage/parquet/
schema.go`), so its footer min/max are the text-order extremes of the row
group. That was fine while the engine compared CIDR as text too. It is not
fine now: `kernel.ResolveFilterKernel`'s TypeCIDR arm compares PostgreSQL's
`inet` order (ADR-0012 item 10), where `9.0.0.0/8` is BELOW `10.0.0.0/8` and
the text says the opposite. The bound and the predicate then disagree about
which values a row group can hold, and the prune deletes rows the filter
would have kept — `WHERE c_cidr < '10.0.0.0/16'` answered 0 rows with the
prune on and a non-zero count with it off, on the same data.

Re-keying the BOUNDS at read time does not recover it. The stored min and max
are two particular rows — the text-order extremes — and the inet-order
extremes are, in general, two DIFFERENT rows the footer never named. There is
no conversion from one pair to the other, which is precisely what
`kernel.StatsDomainValue`'s "no conversion exists" answer means. TypeCIDR is
therefore WITHHELD from the prune entirely, through the mechanism that file
already has, and `tmPruneWithheldTypes` (`wadjet/type_matrix_prune_test.go`)
names it in both directions so the withhold cannot be lost by omission or
kept past its usefulness by inertia.

The same audit clears IPv6 and narrows it by one case. IPv6's physical value
IS the engine's comparison key — the raw 16 bytes, a fixed-width big-endian
address whose byte order is its numeric order — so its bounds stay in the
prune. But a v4-SHAPED literal against a v6 column is a FAMILY comparison
(every v4 address is below every v6 one), which no 16-byte bound can express;
converting it to its v4-mapped bytes instead would place it mid-range, so the
prune would read that one predicate differently from the filter. That literal
is withheld and the rest of the type is not.

The general form: **a bound is only usable by a prune that reads it in the
same order the filter reads the column.** When the engine's order for a type
stops being the file's, the answer is to withhold — or to write a bound in
the engine's order at WRITE time, filed as #523 — never to convert harder at
read time.

### 7. Restoring a withheld prune needs a type-safe bound, not a write-time fix alone

(Added 2026-08-25, closing #523.) §6 named the fix and filed it; this is what
shipped, and the reason the write-time fix alone was not enough.

`leafBuffer.updateStatsCIDR` (`internal/storage/parquet/file_writer.go`) now
tracks a CIDR leaf's row-group min/max by comparing each value's inet-order
sort key (a package-local copy of `kernel.CidrSortKey` — this package sits
BELOW `internal/engine/exec/kernel` in the import graph and cannot import it)
rather than the address text's own bytes, while the STORED bound stays the
winning rows' TEXT — the physical column is unchanged. A file's footer
carries `CidrStatsOrderKey=inet` only when EVERY CIDR value in the file
parsed as an address; one that does not suppresses the flag for the whole
file, and `RowGroupStats` treats that file exactly like one written before
#523 — this is a promise about the FILE, not a per-row-group hedge.

That much makes a NEW file's own bounds trustworthy. It does not make
`kernel.StatsDomainValue`'s conversion safe to turn on unconditionally,
because that conversion runs ONCE per query, before any specific file's
footer has been examined — the per-file "is this bound really in inet
order" answer only exists later, inside `RowGroupStats` for one row group
at a time. Making the literal's conversion depend on a fact only a later,
per-file step can know produced the two real regressions the fix's own
gates caught: `TestTypeMatrixPruningNeverChangesTheAnswer/c_cidr_*`, where
the type-matrix table's rows genuinely span the shapes #492 first
diagnosed, wrongly pruned to zero because a stale FOOTER CACHE entry
(`decodeFooter`, a second, independent bug this arc also found and fixed —
see below) still reported the column as `TypeString`; and
`TestTypeMatrixTwoPathWithoutDeclaredSchemaFooter`, a file with no declared-
schema blob at all, where the reader cannot identify the column as CIDR by
construction and so cannot have converted its bound either.

The fix is a distinct Go type, not a per-file check threaded back into
`StatsDomainValue`. `parquet.CidrInetBound` (`internal/storage/parquet/
reader.go`) is what `RowGroupStats` boxes a bound in when — and only when —
it has confirmed both facts: the column is declared CIDR (`DeclaredSchemaKey`
restores that identity per `declaredOverlayUTF8Types`, exactly as it always
has) and the file's `CidrStatsOrderKey` flag is `inet`. `kernel.
StatsDomainValue`'s CIDR literal converts to the SAME boxed type. Every other
case — no declared schema, no flag, a value that failed to re-key — leaves
`MinValue`/`MaxValue` as a plain `string` (raw text) or withholds them
outright. `scan.compareValuesOK` special-cases `CidrInetBound`: two of them
compare as their own bytes, but a `CidrInetBound` against a plain `string`
— or anything else — refuses (`ok=false`), which is `CanPruneRowGroup`'s
existing "not comparable, don't prune" path. The type system is what
guarantees the mismatch can never reach a byte comparison, not a discipline
about which functions are allowed to call which — the same shape as this
ADR's DECIMAL rule (§4): a value's BOX states what it is safe to compare
against, and the comparator trusts the box instead of re-deriving the
per-file fact that produced it.

The stale-footer-cache defect this arc also fixed:
`internal/storage/parquet/footer_cache.go`'s `decodeFooter` — the path every
`OpenFileReader*Cached` constructor shares — built its `Schema` from the bare
parquet inference (`schemaFromTree`) instead of `readerSchema`, skipping the
`DeclaredSchemaKey` overlay every UNCACHED constructor applies. That silently
cost the same nine types §6's compatibility note lists (IPv4, IPv6, MAC,
UUID, Bytes, Port, Protocol, Duration, and now CIDR) their declared identity
for any reader built through the process footer cache — invisible until
something keyed a pruning DECISION on it, which nothing did before #523.
`TestFooterCacheRestoresDeclaredSchemaType` pins it directly; the type-safe
box above is what stopped it from being a silent wrong answer a second time.

### 7a. A comparison box is not a persistable value

(Added 2026-08-25, review follow-up to #523.) `CidrInetBound` as first shipped
carried ONE string: the inet-order sort key. That is the right value for the
comparator and the wrong value for everything else that touches a
`RowGroupStats`, because its other consumers do not compare it at all — they
COPY it:

- `internal/storage/ingest`'s and `internal/storage/compaction`'s
  `extractColumnStats*` copy `RowGroupStats(i)`'s `MinValue`/`MaxValue`
  verbatim into `catalog.FileColumnStats`, which is JSON-tagged and persisted
  in NATS KV. The sort key is a BINARY string (family byte, masked address
  bytes, mask length, full address bytes); `encoding/json` rewrites every byte
  above 0x7F as U+FFFD and nothing puts them back. Compaction REWRITES these
  stats, so the corruption is not confined to new tables — an existing table
  acquires it on its next compaction pass.
- `catalog.EncodeTableRGMeta`'s `writeValue` switches on the value's Go type
  and stores anything it does not recognise as `nil`. A CIDR bound stored as
  nil is a prune the coordinator's rgmeta path silently forgoes for the rest
  of that table's life after ANALYZE.
- `parquet.CompareNative` — the merge those same two extract sites use to fold
  several row groups into one file-level bound — had no arm for the type and
  answered 0 ("equal") for every pair, so a multi-row-group file recorded ROW
  GROUP 0's bound as the whole file's.

None of the three is a wrong ANSWER, which is exactly why every gate stayed
green: two of them lose a prune (never a row) and the third writes metadata
the optimizer only reads approximately. That is §6's failure mode seen from
the other side — a defect whose only symptom is that an optimization stopped
running, plus, here, permanent bad metadata.

The rule: **a statistics bound whose COMPARISON domain differs from its
STORAGE form carries both, and the box names which is which.**
`CidrInetBound` is `{Key, Text}` — `Key` is the inet-order sort key and is the
only half any comparator may read; `Text` is the winning row's address exactly
as the file stores it, and is the half that leaves for the catalog. Both
extract sites unbox to `Text` at that boundary, `CompareNative` and
`scan.compareValuesOK` order on `Key`, and the rgmeta blob carries both under
its own value tag (`rgMetaTagCidrInet`) so an ANALYZEd bound reaches the
planner still boxed — still confirmed, still comparable. `Text` is empty on
the LITERAL side, where there is no row; sound because a literal-side bound is
only ever compared, never persisted.

The blob tag is additive rather than a format version bump. An unknown tag is
a decode error and `Catalog.TableRGMeta` turns a decode error into "no blob"
(scans fall back to per-file footer reads), so an older binary reading a newer
blob degrades in SPEED only — and only for the tables that actually hold the
new type, which a version bump would not have managed.

### 8. A declared type is restored at every depth, or the paths do not agree

§3 says a file is readable through all of the decode paths or through none.
§4 says the writer's box for a value is the reader's box for it. Both rest on
a footer side channel: parquet cannot annotate eight of wadjet's 22 types
(IPv4, IPv6, MAC, UUID, Bytes, Port, Protocol, Duration) and spells the ninth
(CIDR) as plain UTF8, so the writer stamps the declared schema into the footer
under `wadjet.schema` and the reader overlays it back over the bare parquet
inference. That is the only place those nine types survive a round trip.

The overlay restored them for TOP-LEVEL columns and stopped at the first group
node. Its comment reasoned that LIST/MAP/STRUCT "round trip through parquet's
own annotations" — true of the container STRUCTURE, false of the leaf TYPES
beneath it, which are the same nine one level down with the same absent
annotations. So a nested IPv6 recovered as STRING, the row reader boxed its
sixteen intact bytes as a Go string, `batch.Vector.SetValue` handed the string
to `net.ParseIP`, and the slot was set NULL — read back as "". Silently, on
data that was never damaged on disk (#589).

**The overlay recurses the declared schema through every container node — ROW
fields matched to the tree's children by exact name, ARRAY through its
element, MAP through its key and value, to any depth — under the same
conditions the top-level rule applies, per leaf.** Structure, names,
nullability, precision, scale and dimension still come from the parquet tree,
which CAN express them; only a leaf's type identity comes from the blob, which
is the one thing it cannot. The recursion is driven by the TREE, not the blob:
`nodeToColumn` has already walked the same tree to the same depth to build the
inferred column, so a footer cannot reach deeper here than it already does
there, and a subtree where the blob and the tree disagree (a field the blob
does not name, a name it repeats, a count that does not match, a type whose
storage is not the leaf's) keeps the tree's own answer. The blast radius of a
hostile footer stays what §1's bounds make it — relabelling a column as
another type with IDENTICAL storage — now at every depth rather than only the
top.

Restoring the schema is necessary but not sufficient: the row reader's nested
leaf-decode took the leaf's type from `nodeToColumn` (the bare inference), not
from the file's own recovered schema, so it kept taking the STRING arm after
`Schema()` already said IPV6. `FileReader.LeafColumn` is now the single answer
both the schema and the decode come from. This is §3's "the paths agree"
applied to depth: a type the reader can read as a top-level column it can read
three containers deep, and vice versa.

The corollary about consumers that read types from the CATALOG rather than the
file (`retypeFromCatalog`, the `SchemaAs`/`ReadRowsAs` path) was the SAME
top-level-only limitation, left unchanged in #589 on purpose: it only matters
for pre-`wadjet.schema` files, which have no blob, and the no-blob path is
pinned by `TestTypeMatrixTwoPathWithoutDeclaredSchemaFooter`. Moving both
halves at once would have left that gate unable to say which one moved (#608).

**Closed 2026-09-03 (#608): the catalog-side half recurses too, and it takes
three seams rather than one.** The rule for a leaf is the file-side rule
already — substitute only where the file ANNOTATED nothing (or annotated the
UTF8 that CIDR shares with STRING) and the storage matches, decline otherwise
— and the walk is driven by the file's node tree, so the catalog cannot reach
deeper than the file's own schema does. The one difference from the TOP-LEVEL
pass is deliberate: drift there is an ERROR, because the catalog names a column
the query asked for by name; inside a container it is DECLINED, because the
file-side overlay already declines silently on every condition it cannot meet
and making the same disagreement fatal would refuse files that read correctly
today.

The three seams are the finding. Repairing the leaf array that the nested
DECODE reads (`leafColumnsFromCatalog`) fixed the single-process engine and
left the stage DAG answering `167772160` for `10.0.0.0` over the same bytes —
a two-path divergence one level below the one #423's gate was built for. The
DAG resolves its read schema through `Reader.SchemaAs` and carries the result
onward, so `retypeFromCatalog` has to write the catalog's types into the
returned Column TREE as well. And that still was not enough, because the
declaration reaches a worker as `distributed.ColumnSpec`, which carried
`{Name, Type, Precision, Scale, Dimension}` and no container shape at all: a
`ROW`'s TypeID says nothing about its fields, so the nested types could not
cross the wire. `ColumnSpec` now carries `ElementType`/`Fields`, both
`omitempty`, so a flat declaration encodes exactly as before and a worker that
ignores them behaves exactly as before.

`legacyBoundaryColumn` in the coordinator's legacy-file gate now names the
CONTAINER columns beside the nine scalars. Its omission was the reason a gate
built to prove that a pre-v0.18.0 file's inexpressible types survive could not
see the case where they did not.

### 9. A DECIMAL's declaration is half of every value, so the CATALOG's is the one that counts

(Added 2026-09-03, #707.)

§4 settles what a DECIMAL carrier MEANS: the unscaled integer at the column's
declared scale. It does not settle WHOSE declaration, and for every other type
the question does not arise — an INT64 leaf is an int64 whoever wrote it. A
DECIMAL column chunk carries only the integer; the scale lives in a schema. So
when a file's schema and the catalog's disagree about the scale, the same bytes
are two different numbers, and there is no bit anywhere in the data that says
which.

Two files of one table can disagree: a foreign writer (pyarrow's
`decimal128(15,4)` registered against a `DECIMAL(15,2)` table), a pre-#647
write path, an unrepaired §8/#608 file. Before this rule, none of the read
paths asked. The single-process scan allocated the output vector from the
CATALOG's schema and copied the file's carriers into it verbatim, so a
`12.7500` written at scale 4 came back as `1275.00` — a hundredfold error on a
plain `SELECT id, a`, with no aggregate and no error. The stage DAG took the
FILE's `(p, s)` through `retypeFromCatalog` and wrote it into the `.wshf`
header, so the same two files declared one column two ways and the read either
collided in `wshf.SchemaGuard` (ADR-0010) or, where no shuffle stood above it,
answered `0.1275` — wrong in the OTHER direction from the single-process
engine, which is §3's invariant broken as loudly as it can be.

**The catalog's `(p, s)` is the column's type on every path, and a file that
declares another scale has its carriers MOVED to it at read.** Not
reinterpreted, and not refused: the file holds the right number and says so, so
the number survives. The move is PostgreSQL's assignment cast, measured live —
exact when the scale rises, half AWAY FROM ZERO when it falls, `22003` when the
result has no carrier or leaves the declared band. It is one function,
`parquet.DecimalRescale`, routed through `DecimalValueFromText` so it inherits
ADR-0024's already-gated grammar rather than growing a second scaling rule;
`batch.TestDecimalRescaleAgreesWithBatchRescale` holds it to the engine's
`batch.Rescale` so the two cannot drift.

The reconciliation reaches four places, and the fourth is the one that is easy
to miss:

- the native columnar decode (`scan.rescaleDecimalChunk`, the shape
  `rescaleTimestampChunk` already had for the same reason one type over);
- the row reader's decode, in BOTH of its DECIMAL boxes — an int64 to 18
  declared digits and a `Decimal128` beyond, because `decodeDecimalValues`
  chooses between them by precision and reconciling only one would repair a
  narrow column and silently leave a wide one — and in both of its ARMS, the
  flat column and the per-leaf nested one, through one function
  (`rescaleDecimalBoxes`);
- `retypeFromCatalog`, which now adopts the catalog's `(p, s)` where it used to
  `continue` on a matching TypeID, so a stage's `.wshf` header declares the
  relation and not the file;
- **the row-group STATISTICS.** A footer's DECIMAL min/max is the same kind of
  thing as a value — an unscaled integer at the file's scale — and a predicate
  arrives at the catalog's. Reconciling the values and leaving the bounds alone
  moves the defect rather than fixing it: `WHERE a = 12.75` pruned away the
  whole row group of a file declaring `(15,4)`, because the predicate was 1275
  and the footer said 127500. `parquet.ReconcileRowGroupStats` moves the bounds
  by the same function, which is EXACT rather than approximate because
  round-half-away-from-zero is monotone — the rescaled minimum is the minimum
  of the rescaled values. A bound that cannot be moved is DROPPED, never
  guessed at (§5's rule: withholding costs a prune, guessing costs rows).
  ANALYZE records its persisted bounds in the CATALOG's domain for the same
  reason, since a consumer of persisted metadata has no footer left to
  reconcile against.

Compaction needs no rule of its own and gets the property for free: it already
reads through `ReadRowGroupAs` with the table's schema and writes under the
table's schema, so once the reader reconciles, mixed-scale inputs become one
correctly-scaled output. That matters more here than anywhere else, because
compaction DELETES its inputs —
`compaction.TestCompactionReconcilesMixedDeclaredScales` is the gate, run three
passes for §4's idempotence property.

**Depth is not part of the boundary.** A DECIMAL leaf inside a ROW, ARRAY or
MAP is reconciled exactly as a top-level one is, and it has to be: the
declaration is half of every DECIMAL value wherever the value sits. The first
pass at this rule stopped at the top level, and the result was one row of one
table rendering `12.75` for its flat column and `1275.00` for the same number
inside a ROW beside it — with COMPACT then writing the wrong carrier under the
catalog's declaration and deleting the input. `applyNestedLeafRetype` is the
one place a nested leaf's declaration changes, so the Column tree a caller
carries onward and the per-leaf array the nested decode reads cannot disagree,
and `readLeafColumn` moves the carrier through the same `rescaleDecimalBoxes`
the flat arm uses.

The boundary that IS real is a claim, and the corpus attempts it from both
sides: this fires on a DECLARATION disagreement, and a file that agrees with
the catalog about BOTH halves is read exactly as before. The two halves are
treated alike:

- a SCALE disagreement moves the carrier;
- a PRECISION disagreement moves nothing but holds the value to the catalog's
  band, because a file declaring `(38,2)` under a catalog `(15,2)` column can
  carry a value that column promises not to hold. The two read paths used to
  disagree about exactly that — the native scan ANSWERED a twenty-digit value
  in a column whose wire declaration says fifteen, while the row reader refused
  the same bytes with a message of its own that carried NO SQLSTATE — so a
  client saw the blanket 42000 for a value error (#673's shape). PostgreSQL
  cannot reach the state at all (`123456789012345678.00::numeric(15,2)` is
  22003), so both now refuse with that SQLSTATE and PostgreSQL's own wording.
  On the row path the refusal comes from whichever check reaches the value
  first — the box check in `decodeDecimalValues`, for a value with no int64
  under a column the declared precision boxed as one, or `rescaleDecimalBoxes`
  otherwise — and both now raise it through `decimalOverflow`, so the
  DISPOSITION does not depend on which one fired (§3, ADR-0024).

A catalog DECIMAL over a leaf carrying NO decimal annotation states no
declaration to disagree with, so §4's already-unscaled rule stands there; both
read paths refuse that pairing earlier anyway.

## Consequences

- Files that were read before and are refused now: a footer whose row groups
  do not sum to its total; a chunk whose `total_compressed_size` overstates by
  one byte or more; a page claiming more values than its row group has rows; a
  page header past `MaxPageValues`. Each of these was previously either a
  crash, an allocation the machine could not serve, or a wrong answer.
- The false-refusal risk is real and is managed by measurement rather than by
  intuition: every ceiling above cites what it was measured against, and
  `TestRealWritersDoNotOverstateChunkSizes`,
  `TestRowCeilingHasHeadroomOverRealWriters` and
  `TestMaxPageValuesMatchesWhatWritersProduce` keep those measurements in the
  suite so a future loosening needs new evidence, not a new opinion.
- Writing a value wadjet cannot represent now fails the write instead of
  producing a file that reads differently on different paths. Callers that
  fed unparseable network literals get an error where they previously got
  0.0.0.0, `00:00:00:00:00:00`, or ten bytes in a sixteen-byte column.
- A caller that handed an integer to a DECIMAL column meaning the WHOLE
  number now stores a different value — 10^scale smaller. That reading was
  never the reader's, so no round trip through this package changes; what
  changes is a hand-written literal. The type matrix and the compaction gate
  carry the new contract.
- `DECIMAL(p > 18)` columns are written as sixteen-byte FIXED_LEN_BYTE_ARRAY
  leaves. Files wadjet wrote before are still read (the reader takes all four
  physicals), and the new ones are readable by pyarrow. Wide columns lose
  footer min/max statistics, which costs row-group pruning on them and is
  never wrong — absent statistics prune nothing. The files already on disk,
  and the order a cluster must upgrade in, are the compatibility note below.
- The type-pair matrix grew from one shape (PLAIN, single page, three rows,
  one read path) to 735 cells across PLAIN and dictionary pages and all three
  columnar read paths, because "the paths agree" is only a property if the
  paths are all exercised.

## Compatibility note, 2026-08-23: files this writer produced before #409

The rules above are about believing the FILE. One class of file wadjet wrote
itself is now on the wrong side of them.

Before #409, `decomposeArray` and `decomposeMap` derived a continuing entry's
repetition level as `repLevel+1`, conflating the level to STAMP with the
container's own DEPTH. Those differ whenever the inner container sits in the
FIRST entry of an outer repeated group — the level to stamp is then the
OUTER one, because that is where the repetition last happened. So for a
multi-entry LIST or MAP nested inside another LIST or MAP, every element
after the first was written one level too low, and the same arithmetic
mis-levelled an empty inner container. On disk:

    {"k": [1, 2]}   reads back as    {"k": [1]}, with an entry left over
    [[1, 2]]        reads back as    [[1], [2]]

**These files are unrecoverable.** The repetition level is the only record of
where one container ended and the next began; once two elements of an inner
list carry the outer list's level, nothing in the file distinguishes them
from two entries of the outer one. There is no rewrite that recovers the
grouping, and a reader that "corrects" them would have to guess. Tables
holding such columns must be RE-INGESTED from their source. This is #427.

What the reader does with them now: it returns what PyArrow returns —
`{"k": [1]}`, `[[1], [2]]` — because that is what the levels say, and a
second reader disagreeing with the Apache implementation about the same
bytes is the failure §3 exists to prevent. Where the mistake desynchronised
SIBLING leaves (a map's key against its value, a struct's fields against
each other) the read is REFUSED instead, by the assembler's drained-cursor
check: entries left unread after the row group's records mean the levels and
the row count describe different data. That check sees a subset — it cannot
see a shape whose leaves all happen to drain — so the boundary is: a
pre-#409 file with a container inside a container is suspect whether or not
it is refused.

Only wadjet's own writer produced them. Files written by PyArrow or
parquet-go were never affected, and neither was any shape one container
deep. The blast radius is bounded on the read side too: the same release
that fixed the writer is the first one whose reader could read these shapes
at all — before it, a MAP of ARRAY read back absent and an ARRAY of MAP read
back as its keys (#409) — and PyArrow refused such a file outright. So no
correct value was ever recovered from one of these columns by anything.

**Which columns.** A ROW adds no repetition, so what decides it is REPEATED
inside REPEATED — a LIST or MAP holding two or more entries, nested inside
another LIST or MAP. Verified over 16 shapes × 2 row-group layouts by
comparing the old and new writers' level streams byte for byte:

| affected | not affected (old and new writers emit identical levels) |
|---|---|
| `ARRAY<ARRAY<T>>` | `ARRAY<scalar>`, `MAP<K,scalar>`, `ROW{scalars}` |
| `ARRAY<MAP<K,V>>` | `ROW{ ARRAY<scalar> }`, `ROW{ MAP<K,scalar> }`, `ROW{ROW}` |
| `MAP<K, ARRAY<T>>` | `ARRAY<ROW{scalars}>`, `MAP<K, ROW{scalars}>` |
| `MAP<K, MAP<K2,V>>` | |

and any deeper mix that CONTAINS one of the four, the ROWs in between being
transparent: `ARRAY<ROW{ m MAP }>`, `ROW{ l ARRAY<MAP> }`,
`MAP<K, ARRAY<ROW{ l ARRAY<T> }>>`.

**A second, smaller loss on the same arithmetic.** `decomposeArray` now
forces a LIST's element OPTIONAL on the value side, which `flattenColumn`
and `buildArraySchemaElements` had always done on the schema side. Before
the fix, a LIST whose declared `ElementType` was a CONTAINER with
`Nullable: false` stamped an EMPTY inner container one level too low — the
level that means "this element is NULL". So `ARRAY<ARRAY>` / `ARRAY<MAP>` /
`ARRAY<ROW>` declared with a non-nullable element read `[[]]` back as
`[null]` and `[{}]` as `[null]`, in PyArrow as in wadjet. The footer's
schema did not change, so no column's declared shape or max levels are
affected — only the level stamped on an absent or empty element.

**Finding the affected tables is not something the file can answer.** The
footer records `created_by` as `wadjet (native writer)`, with no version, so
no reader can tell a pre-#409 file from a post-#409 one (#456): the audit is
by INGEST DATE against the release that carried #409, over the tables whose
schema holds one of the shapes above. Both halves of that are why the note
lists the shapes at all. Reading the file back and comparing is not an audit
either — the bytes are self-consistent, which is the whole difficulty.

**What re-ingest means here.** Rewrite the affected tables from their
SOURCE, not from wadjet: compaction and the rewrite mode both read the file
and write what they read, so they carry the wrong grouping into the new file
and delete the old one. Nothing that starts from these bytes can undo them.

## Compatibility note, 2026-08-23: `DECIMAL(p > 18)` files written before #429

Unlike the pre-#409 nesting damage below, nothing here is lost. It still needs
doing, and it needs doing in an order.

**The files already on disk are unreadable outside wadjet.** Before #429 the
writer chose a DECIMAL's physical type from the TypeID alone, so EVERY DECIMAL
was an INT64 leaf. The format allows INT64 only to precision 18, and the
Apache implementation refuses to OPEN a file that annotates a wider one:

    OSError: Could not open Parquet input source '...':
    Decimal(precision=38, scale=10) cannot be applied to primitive type INT64

(pyarrow 23.0.1, on a file written by v0.18.0.) So every wadjet table carrying
a `DECIMAL(p > 18)` column is readable only by wadjet until it is rewritten.

**The remedy is one rewrite pass of the new code, and it has a command.** The
unscaled values in the old files are intact and §4 makes read → write the
identity, so a single rewrite produces a correct FLBA(16) file with
byte-identical unscaled values, which pyarrow opens. `DECIMAL(p <= 18)` files
are byte-identical before and after and need nothing, so the pass is safe to
run over a whole table rather than a hand-picked set of partitions:

    wadjet compact --rewrite <table>

It is `--rewrite` and not ordinary compaction, deliberately. Compaction's
trigger asks whether a partition is worth MERGING — two files or more, at
least `MinFiles` of them, an average size under `MaxFileSizeBytes` — and a
healthy table answers no to all three. The partitions holding one large
well-compacted file, which is most of a table that has been running a while,
are exactly the ones a compaction sweep will never touch. So an earlier draft
of this note, which said a compaction pass over the table was the whole
migration, was true of the DATA and false of the CODE: the files that most
needed rewriting were the ones no pass would have been triggered on.

`Compactor.RewriteTable` (`internal/storage/compaction/compactor.go`) is that
mode: exempt from the trigger's floors, admitting a 1 → 1 rewrite, and
terminating structurally rather than by compaction's per-pass progress rule —
it reads the partition's file list from the manifest ONCE, splits it into
memory-bounded groups, and writes each group once, so no output of the call
can become an input to it. "1 removed, 1 created" is progress here, which is
precisely why the progress rule cannot apply. Failures are per-partition: a
partition that cannot be read is reported and skipped, and the rest of the
table is still migrated.

**The reverse direction is NOT safe: upgrade readers before writers.** A
v0.18.0 reader opens the new FLBA(16) files without error and silently
TRUNCATES to the low 64 bits any value that needs more than 64:

    row 5, true value 93468288258671214869 (scale 10 -> 9346828825.8671214869)
      v0.18.0 row path:     int64 1234567890123456789
      v0.18.0 native path:  123456789.0123456789
      new reader:           Decimal128{Hi:5, Lo:0x112210f47de98115}   correct

Values that fit 64 bits read correctly on the old reader, and those are the
only values a pre-#429 wadjet could write at all — so the exposure is confined
to values NEWLY writable. It is still a wrong number with no error, returned
by an old worker in a mixed-version cluster, which is the failure this ADR
exists to refuse. This is #437.

(The old reader's rendering of the truncated value was further mangled by
#434, which dropped the high word from every DECIMAL text form.)

**What the compaction gate does and does not check.** `TestCompactionIsIdem-
potentOverTheTypeMatrix` asserts the VALUES across generations, on both read
paths, with a PyArrow cross-check, and it asserts the SCHEMA each generation
declares. It does not assert the footer's per-column STATISTICS. That is a
deliberate scope line and not an oversight: a wide DECIMAL has no statistics
to compare (see the consequence above), and statistics are an optimization
input whose absence prunes nothing. A statistics defect shows up as a wrong
ANSWER through the prune, which is where #442's sweep gates it, not here.

## Alternatives considered

- **Keep clamping an overstated chunk.** Rejected on evidence: no writer
  overstates, and the clamp's only observable effect was reading a
  neighbour's bytes as this column's values.
- **Bound the RLE and DELTA counts by the page body.** Rejected as unsound: a
  run header of a few bytes legitimately encodes 2^31 values, so any
  bytes-to-values ratio would refuse valid files. Named as policy instead.
- **Treat a zero-length fixed-width value as the empty value.** Rejected: it
  answers false to `IS NULL` and equal to the empty string, which is not what
  "there is no address here" means, and it was the shape the two readers
  disagreed about.
- **Refuse a non-16-byte UUID at the reader only.** Rejected as
  insufficient: the file had already been written. The refusal belongs at the
  writer, where the column and the row are still known.
- **Make the WRITER's convention the one the reader follows** — hand back
  DECIMAL as the whole number so the existing writer arm is right. Rejected:
  the unscaled integer is what the FORMAT stores and what the engine's own
  `Int128` column holds, so this would have moved the conversion into every
  reader and every consumer instead of removing it.
- **Keep writing wide DECIMAL as INT64 and refuse the values that overflow.**
  Rejected: the refusal was already there and the file was still malformed —
  a reader outside wadjet cannot open it whatever the values are.
- **Overlay a container's declared type from the blob's own nested shape.**
  Rejected: it would let the footer choose how deep to reach and what a group
  node contains, which is exactly the power §1 and §2 deny it at the top. The
  recursion walks the parquet TREE and consults the blob only for a leaf's
  type identity, so it can reach no node `nodeToColumn` did not already build.
- **Restore the schema and leave the row reader taking `nodeToColumn`.**
  Rejected as half a fix: `Schema()` would say IPV6 while the nested decode
  still boxed the value as a STRING — §3's paths disagreeing by construction.
