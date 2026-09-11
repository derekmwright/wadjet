# Wshf stage input schema guard

Source: internal/wshf/schema_guard.go — type SchemaGuard struct {, moved 2026-09-11 (#1026)
Superseded: The historical six-reader count is inconsistent with the listed consumers; the contract covers every reader, not a fixed count.

SchemaGuard holds the several .wshf files of ONE stage input to ONE
description of the relation they carry.

ADR-0010: a `.wshf` header declares its schema once and every chunk in the
file is read under it, so for a DECIMAL the header holds half of every value
— the chunk carries the unscaled integer and the header carries the scale.
`shuffleWriter.writeChunk` already refuses a CHUNK that disagrees with its
own header, and the ADR says in as many words that this covers the
SINGLE-WRITER shape and only that shape: it fires where one task is handed
batches at two scales, and cannot fire where each producer writes its own
internally-consistent file and a downstream reader concatenates several of
them. There is no writer at the point of reinterpretation — the consumer
resolves against the first batch it sees and reads every later file under
that.

This is the missing half, and it lives HERE rather than in any one reader
because there are six of them: the worker's stage source and its inline
result decode, and the coordinator's inline-result, stage-result, gather
receiver, gather replay and scalar-extract reads. #685 was found through one
of them; a guard in that one would have left the other five open, which is
the shape ADR-0010 already refuses for the DECODER itself ("one reader,
fuzzed" — the coordinator and the worker each having their own copy is how
they drifted).

It cannot repair anything: by the time a batch is in hand the integers are
already ambiguous. It does the one thing that is better than a silent wrong
answer — fails the read by name.

The zero value is ready to use. Not safe for concurrent use; each guard
belongs to one reader.
