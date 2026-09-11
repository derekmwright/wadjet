# Row group object stream loading

Source: internal/planner/physical/scan_rowgroup_load.go — row-group file loading, moved 2026-09-11 (#1026)

```go
// One object GET, many buffers.
//
// The whole-file read charges a scan's entire parquet file to the query's
// memory tracker and releases it only when the file's LAST row group has been
// decoded (ADR-0006 producer 1). While that charge is resident every other
// operator's admission is measured against a floor that has nothing to do with
// what the query is holding, and how far the scan ran ahead of its consumer
// decides whether the query answers or refuses (#789).
//
// The bytes are read the same way — ONE Get per file, so the request count the
// object store sees is unchanged, which is the recorded decision
// (docs/design/scan-pread-reads.md: "one object GET beats per-chunk ranged
// GETs") — but the body is landed into one buffer per ROW GROUP instead of one
// per file, using the byte ranges the footer already carries. Each buffer is
// charged when it lands and released when its row group has been decoded, so
// the scan's resident charge is the row groups actually in flight rather than
// the file.
//
// The read is DEMAND-DRIVEN and ADMITTED: the stream advances only when a
// decode worker asks for a row group the loader has not reached, and each row
// group's bytes are reserved before they are read — memory.ReserveOrForce,
// the same bounded-wait-then-force every other non-discretionary charge uses,
// so admission can neither fail a load nor deadlock. With room (or no budget)
// every reservation is clean and the read runs at full speed; without room the
// loader waits for the row group ahead of it to decode instead of piling the
// whole file onto the ledger. One row group is always admitted without waiting
// — the floor that stops a scan holding nothing from waiting on itself.
//
// It requires the file's footer to be decoded ALREADY (the process footer
// cache, populated by buildRGUnits' pruning pass): the row-group byte ranges
// live in it, and reading it from the object separately would be the second
// request this design exists to avoid. A file whose row-group metadata came
// from the catalog's persisted blob has no footer in that cache and keeps the
// whole-file path.
//
// KNOWN BOUNDARY — the object body is held open across the file's decode.
// The whole-file read did Get, read, Close in one span; this one opens the
// body in `advance` and closes it in `close`, which runs when the file's last
// row group has decoded. Between them the read is paced by the decode and, at
// a tight budget, by admission — `fileLoadReserveWait` is 2 s per row group —
// so a many-row-group file can hold one HTTP body open across a mostly idle
// socket, times up to the load gate's lanes. MemStore and FileStore cannot
// observe this and no S3 or MinIO run was made for it, so the cost is stated
// rather than measured: a server-side idle reap becomes a mid-file read error
// where it was previously impossible, and that error fails the query loudly
// (`read <path> row group N: ...`) rather than answering short. Re-issuing the
// Get from `s.pos` on a mid-stream read error is the fix if it is ever seen;
// it is not written on speculation.
```
