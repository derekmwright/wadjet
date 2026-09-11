# Window key binding and materialization

Source: internal/planner/physical/window_keys.go — resolveWindowKeys, moved 2026-09-11 (#1026)

A window's PARTITION BY / ORDER BY terms, and the two ways they stopped
naming a column (#585).

exec.Window reads its keys by NAME off the input batch, so a term the batch
does not carry used to drop out of the key list — and a window whose keys
all drop out is a window over ONE partition spanning the whole input. Two
spellings a BI client produces routinely did exactly that:

	PARTITION BY p.g       the batch carries `g`; the qualifier misses
	PARTITION BY id % 3    nothing computed it, so no batch carries it

Both answered silently and wrongly: ROW_NUMBER() numbered straight through
every group, SUM OVER returned the whole-table sum.

The two halves need different repairs, and this file is where the choice is
made once for BOTH execution paths — buildWindow's operator and walkStages'
stage spec go through windowExecColumn, which calls resolveWindowKeys, so
the single-process pipeline and the DAG cannot come to different
conclusions about what a key names.

  - A QUALIFIED reference is BOUND to the input column, which is the rule
    #488 settled for a qualified ORDER BY at the query root: `p.g` is `g`
    in the FROM scope, whatever the SELECT list calls it. The binding is
    done here, at plan time, and not left to exec.Window's runtime fallback
    alone, because the key name is ALSO the DAG's clustering key — the
    exchange ahead of a window stage hash-partitions on it (distribution.go,
    windowPartitionKeys), and it cannot partition on a name no upstream
    stage emits.

  - An EXPRESSION is MATERIALIZED as a computed column, exactly as a GROUP
    BY expression is pre-projected for the hash aggregate
    (`SUBSTR(c_phone, 1, 2)` — plan.go's preProjectCols, and the worker's
    buildAggInputProjection). It is named __winkey_N, the same synthetic
    convention __gb_expr_N uses and for the same reason: the expression's
    own TEXT is already a key elsewhere — exec.Project resolves a
    projection's output type by looking its source spelling up in the input
    batch — so a column literally named `id % 7` made the SELECT list's own
    `id % 7 AS k` type itself from the window's scratch column and write an
    INT64 value through a FLOAT64 evaluator.

    Nothing ABOVE the window reads __winkey_N, which is what keeps it clear
    of #558: the SELECT-list projection drops it on the single-process path
    and the gather projects to the visible output on the DAG. It is never a
    sort key, so the materialize-above-a-window problem that issue names
    does not arise.

Anything this pass cannot resolve is left exactly as written and reaches
exec.Window.bindKeyNames, which refuses it. Degrading to one partition is
never an outcome either half can produce.
