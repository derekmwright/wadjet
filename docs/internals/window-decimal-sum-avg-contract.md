# Window decimal sum avg contract

Source: internal/engine/exec/window_decimal_agg.go — WindowDecimalAggMeta, moved 2026-09-11 (#1026)

Windowed SUM/AVG over a DECIMAL answer what the GROUPED SUM/AVG answer
(#586, #475, ADR-0024 item 2).

`SUM(d) GROUP BY g` and `SUM(d) OVER (PARTITION BY g)` are the same question
written twice, and a BI tool flips between the two spellings freely. Until
this file existed they disagreed about the TYPE of the answer and about its
DIGITS: the grouped form kept an exact Int128 accumulator and declared
DECIMAL(38,s) (#455, ADR-0012 item 9), while the window accumulated in
float64 through vecFloat64 and declared FLOAT64, so everything past ~16
significant digits was gone before any consumer saw it.

The rules here are ADR-0012 item 9's, unchanged:

	SUM(DECIMAL(p,s)) -> DECIMAL(38, s)
	AVG(DECIMAL(p,s)) -> DECIMAL(38, min(s+4, 38)), exact Int128 division
	                     rounded half away from zero
	overflow          -> SQLSTATE 22003, never a wrapped total

The declared precision is the carrier's full width rather than the input's,
because a sum genuinely exceeds its column's precision and a narrower
declaration would hand the parquet writer a leaf too small for the value.
