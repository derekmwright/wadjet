# Pgwire result type modifiers

Source: internal/server/pgwire/server.go — func TypeMod(m wadjet.ColumnMeta) int32 {, moved 2026-09-11 (#1026)

TypeMod returns the PostgreSQL type modifier (atttypmod) for a result
column, or -1 for a type that has none. It is exported so the differential
oracle asserts the typmod a client is HANDED rather than a copy of this
rule (benchmarks/tpch, ADR-0024 item 5).

The modifier is where PostgreSQL keeps the part of a declaration the OID
does not carry: numeric's (precision, scale), varchar/bpchar's length,
time/timestamp/interval's second precision. -1 means "unconstrained", which
is protocol-legal and is what every unparameterised type sends — so writing
the constant -1 for everything was less information rather than wrong
information, and it went unnoticed until DECIMAL started declaring OID 1700
(#454). What a client loses is ResultSetMetaData.getPrecision()/getScale():
a column declared DECIMAL(9,2) reports 0 or "unlimited", and a tool that
sizes a display column or round-trips DDL from a result set gets it wrong.

numeric packs the pair as ((precision << 16) | scale) + VARHDRSZ, exactly as
PostgreSQL's numerictypmodin does (utils/adt/numeric.c). Precision 0 means
the declaration did not reach us — a plan-declared schema for a zero-row
result, an inferred type — and an unconstrained numeric is the honest
answer there, not a fabricated (0,0).

The switch is keyed on the wadjet TypeID rather than the OID so that a type
which later gains a parameter (a VARCHAR(n), a TIME(n)) is added here and
not in the wire writer.
