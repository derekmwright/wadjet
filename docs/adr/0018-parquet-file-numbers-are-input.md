# ADR-0018: A parquet file's own numbers are input, not fact

Status: Accepted (landed 2026-08-23, `09b19d2`..`94143bc`, on the hardening
arc that began with `20049f9`; ceilings measured against pyarrow 23.0.1,
parquet-go and wadjet's own writer)

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
- The type-pair matrix grew from one shape (PLAIN, single page, three rows,
  one read path) to 735 cells across PLAIN and dictionary pages and all three
  columnar read paths, because "the paths agree" is only a property if the
  paths are all exercised.

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
