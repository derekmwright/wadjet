# Parquet nested assembly drain check

Source: internal/storage/parquet/nested_assembly.go — func (a *recordAssembler) checkDrained(leaves []*SchemaNode) error {, moved 2026-09-11 (#1026)

checkDrained reports a leaf whose level stream still holds entries after
every record has been assembled.

The count is an exact bound, not a policy one (ADR-0018 §1): assembly
consumes one entry from a leaf per value the record holds at that leaf,
and a NULL or empty container consumes exactly one placeholder from each
leaf beneath it, so after the row group's numRows records EVERY leaf that
was paged in must sit exactly at the end of its own stream. A residual
means the levels and the row count describe different data — the file's
own numbers contradicting each other, which is the one thing a reader can
see without a second opinion.

It is the assembler's only cross-check. The level walk itself cannot
notice a wrong repetition level: it reads what the levels say, and levels
that close a container early simply produce a shorter value. Where the
mistake desynchronises SIBLING leaves — a map's key against its value, a
struct's fields against each other — the leftover entries are what is left
to see, and this is what sees them. Wadjet's own writer emitted exactly
that shape for a multi-entry LIST or MAP nested inside another one, before
#409 (see docs/adr/0018-parquet-file-numbers-are-input.md); such files are
refused here rather than answered from.
