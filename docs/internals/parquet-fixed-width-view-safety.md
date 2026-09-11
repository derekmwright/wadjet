# Parquet fixed width view safety

Source: internal/storage/parquet/values.go — func typedValues (the generic fixed-width view over Values), moved 2026-09-11 (#1026)
Superseded: INT32 file to INT64 catalog is now admitted by CoercibleTo as a post-decode widening; it still must never read the INT32 buffer directly as INT64.

typedValues reinterprets v.data as a []T of width-byte elements — the
zero-copy cast every fixed-width accessor is built on — and is the ONE
place that decides whether the cast is safe to make.

Two conditions are checked, and neither is theoretical:

  - the physical type must be the one the caller asked for. The row
    reader decodes a column as the type the CATALOG names, not the type
    the file stores, so a catalog INT64 over a file INT32 asked for
    v.count int64s from a buffer holding v.count int32s — an unsafe.Slice
    twice as long as its backing array, i.e. adjacent heap read straight
    into query results. retypeFromCatalog now rejects that pairing before
    it gets here; this is the backstop at the unsafe site itself.
  - the bytes must be able to back v.count elements. Every constructor in
    this file establishes that (the DecodePlain* decoders now REFUSE a
    page body shorter than its declared value count instead of slicing
    into it), so this is a second backstop, not the enforcement point.

A short buffer yields nil, not a truncated slice. Truncating would answer
a read of N values with fewer, which every caller silently accepts as "the
rest were absent" — the same silent-wrong-answer class the physical-type
refusal exists to prevent. nil is refusal: the read sites check it and
report an error naming the column.
