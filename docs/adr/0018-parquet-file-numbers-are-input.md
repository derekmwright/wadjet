# ADR-0018: A parquet file's own numbers are input, not fact

Status: Accepted (landed 2026-08-23, `09b19d2`..`94143bc`, on the hardening
arc that began with `20049f9`; ceilings measured against pyarrow 23.0.1,
parquet-go and wadjet's own writer). Amended 2026-08-23 with §4, "the
writer's box for a value is the reader's box for it", after the same arc's
review found read → write was not the identity for DECIMAL (#429) and that
`ReadRowGroup` and `ReadRows` were two readers that disagreed about a nested
column (#428).

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
  never wrong — absent statistics prune nothing.
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
