# Column instant storage units

Source: internal/engine/expr/expr_string_entropy.go — columnInstant, moved 2026-09-11 (#1026)

columnInstant resolves row i of a vector to the UTC instant it denotes, and
reports whether it could. It is THE definition of "what time is stored in
this column", and both evaluation paths go through it: the vectorized
date-part kernels below call it directly, and the scalar path reaches it
through (*FuncCall).resolveTemporalArgs. Divergence between the two paths is
what let this defect survive — one shared resolver makes agreement
structural rather than something a test has to keep rediscovering.

A raw stored number carries no unit, and each type stores a different one:

	TypeDate      Int32Data, days since the epoch
	TypeTimestamp Int64Data, MILLISECONDS since the epoch (what the parquet
	              writer emits — file_writer.go encodes TimestampMillis — and
	              what the comparison path assumes, parseTemporalInt64OK)
	String/Bytes  text, parsed
	anything else Int64Data read as seconds, the only defensible reading of
	              an untyped integer and what parseTime(int64) has always done

Reading Int64Data unconditionally was right only for a timestamp-in-seconds
column: a DATE column has nothing in Int64Data at all, so every row came
back as 1970 — silently, with no error and no null, collapsing a decade of
`GROUP BY EXTRACT(YEAR FROM d)` into one bogus bucket (issue #319).
