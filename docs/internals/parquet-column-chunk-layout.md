# Parquet column chunk layout

Source: internal/storage/parquet/footer.go — func ValidateChunkLayout(md *FileMetaData, dataEnd int64) error {, moved 2026-09-11 (#1026)
Superseded: Validation forbids overlap and footer crossing but allows gaps; the contiguous layout observed in the corpus is not a required invariant.

ValidateChunkLayout holds the file's column chunks to the one thing a
single chunk's metadata cannot check about itself: where its neighbours
are.

A chunk's extent is [offset, offset+total_compressed_size). chunkRange
refuses one that runs past the end of the FILE, but the far more damaging
overstatement is the small one — a chunk that reaches a few hundred bytes
into the NEXT column's pages. The page loop reads those bytes as more pages
of this column and returns them as values, so a 128-row chunk comes back
holding 64 of its own values and 64 of a neighbour's, with err == nil. That
is the silent-wrong-answer shape, and nothing downstream can see it.

The rule is that the chunks tile the data region without overlapping and
without crossing into the footer. Measured, not assumed: across 44 files
from pyarrow (four codecs, format 1.0 and 2.6, with and without the page
index, one and many row groups), parquet-go and wadjet's own writer, every
adjacent pair of chunks is exactly contiguous — worst gap zero, worst
overlap zero — and the last chunk ends at or before the footer. No writer
overstates, so an overstatement is a corrupt file and is named as one.

Empty chunks are skipped: a zero-byte extent has no bytes to collide over,
and its offset is whatever the writer happened to leave behind.
