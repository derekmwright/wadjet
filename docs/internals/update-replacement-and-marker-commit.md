# Update replacement and marker commit

Source: wadjet/dml.go — var totalUpdated int64, moved 2026-09-11 (#1026)
Superseded: The opening per-file marker-commit description predates the statement-wide FlushAll and single CommitDML below; files and markers publish in one CAS.

Per-file streaming: box only the matched rows, hand them to the
ingester, then commit that file's delete markers. The previous shape
boxed every row of every file (even at zero WHERE selectivity) and
accumulated all updated rows table-wide before one Ingest — a broad
UPDATE held the whole table as boxed maps.

EVERY REPLACEMENT ROW IS DURABLE BEFORE ANY MARKER IS COMMITTED. The
markers accumulate across the whole statement, one FlushAll follows the
loop, and only then does a single CommitDML commit them.

Committing a file's marker inside the loop is what made this per-FILE
rather than per-STATEMENT. Ingest only BUFFERS, so with the marker for
file 1 already durable and its replacement rows still in RAM, a failure
on file 2 — a legacy value past the column's precision, or an
object-store error inside the auto-flush that bounds memory — returned
without ever flushing, and file 1's matched rows were simply gone
(#647 re-review). Marker-first, the shape before that, lost them on the
FIRST file.

The remaining duplication is closed by DeferManifestCommit: the
ingester holds its flushed files OUT of the manifest and they land in
the SAME CAS as the markers, so a refused commit has published nothing
and the statement is simply redone (#691). What an interrupted attempt
leaves behind is unreferenced objects in the store, never a row.
