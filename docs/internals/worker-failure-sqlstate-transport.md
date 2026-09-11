# Worker failure sqlstate transport

Source: internal/coordinator/task_retry.go — stageTaskFailure, moved 2026-09-11 (#1026)

stageTaskFailure carries a failed stage task's SQLSTATE across the process
boundary. err is the caller's framing of the worker's raw error text, whose
wording each stage type owns; f carries the worker's own classification of
that same failure.

A worker-side error is converted at the worker's boundary and reaches the
coordinator as a bare STRING in a ResultNotification — the typed error and
its class do not survive that trip, and framing the text with %s produced an
error with no code at all. So the class travels as a field. Two of them:

  - Panicked, for a query-scoped panic (ADR-0019). It wins, because XX000
    is what a panic is regardless of any class the panicking error carried.
  - SQLState, for every ordinary runtime failure (#649). A DECIMAL overflow
    is 22003, a division by zero is 22012 and an invalid network literal is
    22P02 on the single-process path, and all three reached the client with
    no class at all through the DAG until this field existed. An empty
    SQLState leaves the error as it was: no class is the honest answer for
    a failure whose producer never assigned one, and inventing one here
    would be guessing.

Neither is ever derived from the text. Error is free-form and can
legitimately contain any substring — including one that used to double as
the panic marker, which a CAST reporting back an invalid input of "internal
error in x" reproduced exactly.
