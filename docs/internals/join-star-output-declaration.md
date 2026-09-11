# Join star output declaration

Source: internal/planner/physical/star_declared_schema.go — starJoinDeclaredOutputSchema, moved 2026-09-11 (#1026)

starJoinDeclaredOutputSchema is the declaration for `SELECT *` over a JOIN —
the one zero-row shape that reached a client with NO COLUMNS AT ALL, on
every arm (#978, #846's twin).

`SELECT * FROM a JOIN b ON …` produces no Project node for the walk above to
read and no single scan for it to describe, so a result WITH rows was
described from the first batch and a result without rows was described by
nothing: psql printed no header, pgJDBC's executeQuery had no column
metadata, and the pgwire door sent an EMPTY RowDescription because that was
the most honest thing it could say. `SELECT * FROM a WHERE false` has
declared its columns since #416.

THE NAMES ARE THE OPERATOR'S OWN. The join executor emits the probe's
columns and then the build's, with every DUPLICATE bare name qualified by
its owning alias, and that rule lives in `exec.joinOutputSchemaWithMapping`.
This function does not reimplement it — it assembles the arguments from the
plan and calls it (`exec.JoinOutputSchema`), which is why the declaration
and the executed answer cannot disagree. A second copy of that rule is
exactly what this file declined to write before, and it was right to.

THE BOUNDARY IS ONE JOIN, and it is a claim rather than a convenience:

  - Neither side may contain a join of its own. `declaredJoinSchema` walks a
    nested join by CONCATENATING its sides and dropping duplicate names,
    which is not the operator's rule, so a bushy shape would be described by
    a list the engine never produces.
  - `QualifyAllBuildCols` is a STAGE property set only where TWO joins in one
    chain build from one table (markCoPathingSelfJoinBuilds), so with one
    join in the plan it is false on every path — which is what lets this be
    answered from the logical tree at all.
  - A side whose columns the plan cannot type declines the whole schema, the
    same rule the scan arm above applies: no declaration beats a wrong one.
