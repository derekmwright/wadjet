# Window function result type dispatch

Source: internal/planner/physical/window_declared_output.go — windowOutputType, moved 2026-09-11 (#1026)

windowOutputType declares the output type of an INPUT-INDEPENDENT window
function — the rank family, whose answer is a position or a ratio computed
from the frame, plus COUNT, which finalizes to int64 whatever it consumed.

SUM and AVG reach this list only as a FALLBACK. Over a DECIMAL they answer
DECIMAL, exactly as the grouped forms do (#586, ADR-0012 item 9), and
windowSpecOutputType resolves that from the input column; the float64 here
is what every other numeric input still gets, and what an input the planner
could not type at all falls back to.

The value functions — lag, lead, first_value, last_value, nth_value — are
NOT here: they return a value taken from their input column rather than
computing one, so their output type IS that column's type and no name list
can know it. Declaring them float64 typed the window's output vector
numeric while the value path wrote strings, and exec.Window (unlike
exec.Project) had no runtime correction, so every string write was dropped
for the integer 0 (#345). windowSpecOutputType resolves them instead.

MIN/MAX over a window were the last input-dependent family answered from
this list, and landed on the float64 default: MIN(a_string) OVER (...)
and MIN(int32_col) OVER (...) had #345's symptom for the same reason
(#361). They resolve from the input column like the value functions, and
since #569 for EVERY type the engine has — exec.WindowMinMaxType names
them all, so what still reaches this list from a MIN/MAX is only an input
type the planner could not resolve at all.
