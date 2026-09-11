# Temporal text cast shared parser

Source: internal/engine/expr/cast_refusal.go — func castTemporalText(src any, kind castTemporalKindT) (any, bool) {, moved 2026-09-11 (#1026)

castTemporalText converts TEXT to a DATE's epoch days or a TIMESTAMP's epoch
milliseconds through the ENGINE'S ONE temporal accept-set, raising its
classified refusal when the text names no instant.

It is the VALUE and the CODE from the same function, and that pairing is the
point. `parquet.ParseDateDays` / `ParseTimestampMillis` is what the ingest
boundary, the parquet writers, the row→batch builder and the filter kernel
all read, so a literal that STORES is a literal a predicate over the same
column reads the same way — and now a literal a CAST reads the same way too.

#836's first pass took only the CODE from here and left `parseDateArg` as
the value source, which made the two accept-sets visible as a DIVERGENCE
rather than as one rule: `CAST('20240101' AS DATE)` is 2024-01-01 on the
live server and in this classifier, and the mismatch turned it into a
refusal. One function answers both halves now, so a spelling the engine
accepts anywhere is accepted here, and a spelling it refuses anywhere is
refused here with that refusal's own SQLSTATE.

The BOUNDARY is deliberate and the corpus attempts it from both sides: only
a TEXT operand comes here. Every other Go box that fails to parse — a
boolean, a container, a value type with no temporal reading at all — is a
TYPE-PAIR failure, which PostgreSQL answers at PARSE time with 42846
(`cannot cast type boolean to date`) and not with a data exception. Minting
22007 for those would put a data-exception code on a type error, so they
keep the NULL they have and are recorded in ADR-0012's divergence list.

A NULL operand never reaches here: Cast.Eval returns before the conversion.
