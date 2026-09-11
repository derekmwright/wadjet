# Distributed ohlcv state decomposition

Source: internal/coordinator/ohlcv_decompose.go — decomposeOhlcv, moved 2026-09-11 (#1026)

decomposeOhlcv is ADR-0035's rewrite for the bar, and it is decomposeVar's
shape exactly:

	OHLCV(ts, price, volume) AS out  →  OHLCV_STATE(...) AS __ohlcv_state#out

A FINISHED bar cannot be re-aggregated — the partial-then-merge shape would
run OHLCV over per-task ROWs, which is not a bar at all — so what ships is
the STATE each partial accumulated. Intermediate merge stages fold states
into states (the worker rewrites OHLCV_STATE into OHLCV_STATE_MERGE in merge
mode, as it does VAR_STATE), and the final stage's fold turns the last one
into the ROW the query asked for (worker.applyOhlcvFold).

The state's DOMAIN — exact or float, and the scales — travels inside the
encoded state, so a merge stage needs nothing but the string. The declared
FIELDS travel on the spec (AggSpec.OutputFields), because only the final
fold needs them and only the planner can derive them.

Unlike the variance family there is no <kind>: one state, one way to finish
it. The synthetic is `__ohlcv_state#<out>` and `#` is illegal in an
identifier, delimited or not, so it cannot collide with a user's column.

Returns the original slice unchanged when no bar is present.
