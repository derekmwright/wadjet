# Catalog compaction conflict boundary

Source: internal/storage/catalog/compaction_commit.go — var ErrCompactionConflict = errors.New("this compaction output was cut from a snapshot the table no longer has"), moved 2026-09-11 (#1026)

ErrCompactionConflict reports that a compaction output cannot be published
because the snapshot it was cut from is no longer the table's state.

It is the compaction half of the rule `ErrDMLTargetMoved` states for DML
(ADR-0030), and it exists for the same reason: a writer that reads a
manifest, spends time producing a replacement, and then commits, is running
a transaction whose read set has to be validated at commit time or not at
all. Compaction's read set is two things — WHICH FILES it consumed and
WHICH ROWS OF THEM were already deleted — and until #893/#894/#895 neither
was checked:

  - `RemoveFiles` treated an input that was already gone as success, so two
    compactors could each publish a replacement for the same originals and
    the table ended up holding both copies of every row (#895).
  - a DELETE that committed after the output was written but before it was
    published was undone by the publication, because the output still
    carried the row and `RemoveFiles` stripped the marker that named it
    (#894).

A conflict is not a failure: nothing was written, the previous snapshot is
intact, and the losing writer replans from the manifest that replaced the
one it read. It is detected BEFORE the CAS write is attempted, which is why
the caller may safely delete the output object it uploaded — a publication
ERROR (the KV refused, timed out, or is unreachable) says nothing about
whether the write landed, and the bytes are kept in that case.
