# Parquet writer closed lifecycle

Source: internal/storage/parquet/file_writer.go — var ErrWriterClosed = errors.New("parquet: writer is closed (the file was already finalized)"), moved 2026-09-11 (#1026)
Superseded: Concurrent Close calls now serialize on closeMu and wait for the first finalization before returning its latched failure or ErrWriterClosed.

ErrWriterClosed is returned by every call on a writer whose file has
already been finalized.

A parquet file ends with its footer, a four-byte footer length and the
magic trailer, so the last byte Close writes is the end of the artifact.
Nothing can be appended to it and nothing can be taken back. Before this,
neither Writer nor NativeWriter recorded that Close had run, and a later
WriteRows/WriteMapRows returned nil in both of the two shapes the row-group
size selects (#972, measured at f415faba on a one-INT64-column file):

  - the row fitted the open row group, so it was buffered, nothing reached
    the output, and the accepted row was silently LOST;
  - the row crossed RowGroupSize, so a whole column chunk was appended
    AFTER the trailer — 292 bytes became 347 and both wadjet's reader and
    pyarrow then refused the file ("invalid magic", "Parquet magic bytes
    not found in footer"). A finalized, readable file became unreadable
    because of a call that returned success.

Close was not idempotent either: a second Close wrote a second footer and
trailer over the first, which is what a `defer w.Close()` beside an
explicit one would have done.

The rule is therefore the simplest one that has no such shapes: a closed
writer is closed. The first Close latches it — whether it succeeded or
failed — and every later WriteRows, WriteMapRows and Close returns a loud
error having touched neither the leaf buffers nor the output. A writer
whose Close FAILED keeps returning that failure instead, because it is the
more specific answer and it is what #888's latch already promised.

A writer is NOT safe for concurrent use: its leaf buffers, its error latch
and its byte count are all unsynchronized, and two goroutines writing rows
to one writer will corrupt the file. The single exception is this latch,
which is claimed atomically, so two goroutines racing to Close cannot both
finalize — exactly one writes the footer and the other is told the file is
already finalized.
