# Scan selected byte materialization

Source: internal/engine/scan/sel_decode.go — var selDecodeToggle = optswitch.Register("sel-decode", "WADJET_SEL_DECODE",, moved 2026-09-11 (#1026)

Sel-aware materialization (#299).

When the scan-level filter returns a partial selection, every column in
the read schema still materialized in full — for a 0.1%-selective
predicate over a wide row, >99% of the byte-array copy work built values
no operator would ever read (Sel semantics: unselected slots are dead,
exactly as they are downstream of any exec filter). readColumnNativeSel
decodes a byte-array column against the selection instead: offsets are
written for every row (BytesColumn offsets must stay monotonic, so dead
rows become zero-length slots), but dictionary gathers and value copies
run only for selected rows. Dictionary pages additionally skip the
resolve-to-scratch + BulkSet double copy — selected entries gather
straight from the dictionary into the vector arena.

Scope: flat (non-nested) String/Bytes leaves, which is where the copy
cost lives; fixed-width columns keep the straight memmove that already
beats any per-row selection test. Sel-decoded columns are never offered
to the decoded-chunk cache (the cache key has no selection component; a
partial column entering it would corrupt later full reads).

Failure contract matches the full decoder: every dictionary index
actually dereferenced is bounds-checked; corrupt or hostile files error,
never panic or mis-slice.
