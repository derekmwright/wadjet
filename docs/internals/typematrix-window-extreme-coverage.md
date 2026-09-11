# Typematrix window extreme coverage

Source: internal/oracle/typematrix/typematrix.go — add("windowminmax_"+n,, moved 2026-09-11 (#1026)

Windowed MIN/MAX. Unlike the value functions above, the answer is
CHOSEN by a comparison rather than lifted by position, so the
entry gates two things at once: the output column's declared type
(the input's own, with DECIMAL's scale and a container's shape
riding along) and the ORDER the choice is made in.

Until #569 twelve of the twenty-two types declined to declare and
kept the planner's FLOAT64, which did not degrade the answer — it
FAILED the query, "cannot store string into FLOAT64 vector", for
CIDR/UUID/IPV6/IPV4/MAC/DECIMAL/BYTES/BOOL and all four
containers, while the plain aggregate over the identical column
answered correctly. A corpus entry could not have shown that
before, because none of the entries above asks a window for a
value it has to CHOOSE.

Two shapes, because they are two evaluators and the comparison
reaches the value through a different door in each:

  whole-partition  the partition-at-a-time columnar path, which
                   compares kernel.CompareValuesAt on the vector —
                   and, on the DAG, a hash-partitioned window
                   stage (#349) with its own declared schema.
  OVER ()          the streaming empty-PARTITION-BY evaluator
                   (window_global.go), which carries its running
                   extreme as a BOX across batches and so compares
                   through newBoxedCompare — the site where a
                   network type's DISPLAY text is not its address
                   order (#520/#565's shape, one consumer over).

A SLIDING frame is deliberately not here. It exercises the deque's
EVICTION, which is worth gating, but it does so identically on
every arm this corpus feeds — the DAG's window stage runs the same
operator — and each entry here costs a distributed query per type
in coordinator.TestTypeMatrixTwoPath. It is gated instead in
exec.TestWindowExternalMinMaxEveryType, which compares the
in-memory and SPILLED evaluators against each other over all 22
types and asserts the spill actually happened.
