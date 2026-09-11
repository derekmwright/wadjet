# Distributed aggregate output type

Source: internal/distributed/messages.go — OutputType *int `json:"output_type,omitempty"`, moved 2026-09-11 (#1026)
Superseded: SUM and AVG are now input-dependent (including exact integer/DECIMAL accumulation); the old input-independent statement is stale.

OutputType is the plan-time parquet.TypeID of this aggregate's
output column, carried as a plain int so the wire package stays free
of the storage dependency.

Nil means "the planner did not declare one" — either an older
coordinator that predates the field, or a MIN/MAX whose input
column the planner could not resolve to a catalog type. Workers
fall back to deriving the type from Func alone in that case, and
any decision that needs a TRUSTWORTHY type (emitting an ungrouped
aggregate's identity row over zero input, where there is no input
schema to read it from) must decline rather than guess.

COUNT-family, SUM and AVG are input-independent in this engine, so
the planner always declares them. MIN/MAX follow their input
column, and are declared only when it resolves to exactly one
catalog column type.

A POINTER since #354, for WindowColSpec.OutputType's reason:
parquet.TypeID's zero value is BOOL, so the plain int this used to be
could not tell a declared BOOL_AND/BOOL_OR output from an absent
declaration — the DAG read it as undeclared and fell back to a guess,
reinstating #345's silent-drop shape for exactly one type.
