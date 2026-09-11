# Network function argument rendering

Source: internal/engine/expr/expr_scalar_fns.go — FuncCall.formatNetworkArgs, moved 2026-09-11 (#1026)

formatNetworkArgs rewrites boxed TypeIPv4/TypeMAC ColRef argument values
to their canonical text form (dotted-quad / colon-hex) for
networkTextFuncs AND stringInputFuncs. Reads through the column directly —
like resolveTemporalArgs's columnInstant, and unlike formatTemporalArgs
above — because formatIPv4/formatMAC are batch-package-internal;
Vector.GetValue is the exported boundary that already renders them
correctly (it is what a bare `SELECT ip_col` reads through, via
exec.ColumnRef). Only direct column references are covered, matching
formatTemporalArgs: a nested expression's output type isn't known here.

stringInputFuncs (length/concat/upper/starts_with/...) went unfixed by
#484, which only taught networkTextFuncs (ip_to_string, cidr_contains, ...)
this rewrite: a SEPARATE registry, so `length(ipv4_col)` kept reading the
raw encoded int64 and answering the DIGIT COUNT of the address's number
instead of its text (#500). TypeIPv6/TypeCIDR/TypeUUID need no entry here:
ColRef.Eval already falls through to Vector.GetValue's default case for
those three (see the type switch there), which is the correct rendering
for a function argument same as it is for CAST — only TypeIPv4/TypeMAC
take the raw-int64 fast path that needs unwinding. TypePort/TypeProtocol
need none either: their canonical text IS their raw number, so the box
ColRef.Eval already returns is already the right string.
