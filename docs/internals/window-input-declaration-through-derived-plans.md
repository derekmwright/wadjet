# Window input declaration through derived plans

Source: internal/planner/physical/window_declared_output.go — windowSpecOutputType, moved 2026-09-11 (#1026)

colRefDeclaredType declines every PARAMETERIZED type (DECIMAL without
its scale, VECTOR without its dimension, the nested types), so those
keep the float64 fallback here and are corrected at runtime instead:
exec.Window.retypeValueColumns re-declares from the input vector and
exec.windowOutputColumn carries the (p,s)/element/field metadata with
it. A ZERO-ROW result has no such vector and is described from this
declaration alone, which is why `MIN(dec_col) OVER (...)` matching no
row still describes itself float8 while the same query matching rows
describes itself numeric — tracked in #587, not fixable by widening
colRefDeclaredType, whose decline exists for projections that have no
runtime correction at all.

inputColDecls, not inputColTypes: it carries the ROW columns' FIELDS
too, so a windowed value function or MIN/MAX over a field path
(`MIN(rw.f_i64) OVER ()`) resolves the field's type here instead of
defaulting to float64 (#568). A field path of a parameterized type
still declines and rides the same runtime correction as a column.
emittedColDecls, not inputColDecls: the walk that CROSSES a derived
table's Project instead of stopping at it (#529's walk, ADR-0026 §5).
A window one nesting level above its scan —
`SUM(a) OVER () FROM (SELECT id, a FROM t) u` — resolved NOTHING here
and fell to the float64 fallback, so the same window that declares
numeric directly over the table declared float8 through a derived
table, and an aggregate reading it inherited the float box on every
arm where PostgreSQL answers numeric (#796). It is the same walk the
aggregate's own argument (aggInputDecls) and declaredOutputSchema
already use, which is what makes the window's declaration, the
aggregate's above it and the wire's agree through a nesting level.
