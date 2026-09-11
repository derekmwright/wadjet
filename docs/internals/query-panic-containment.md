# Query panic containment

Source: internal/engine/exec/panic_boundary.go — RecoverQueryPanic / CatchQueryPanic, moved 2026-09-11 (#1026)

The query-scoped panic boundary.

recoverFatalEval converts exactly one class of panic — the deliberately
raised fatalEval / TypeMismatchError family that expression evaluation uses
as its error channel — and re-panics everything else. That is the right
contract for THAT conversion: a runtime panic is a bug, and turning it into
a generic error at the point it happens would bury it.

It is the wrong contract for PROCESS SURVIVAL. An unrecovered panic on any
goroutine terminates the whole Go program, so "re-panic the rest" means an
index-out-of-range in one connection's query kills every other connection's
query too — and a client can reach one with ordinary SQL (#509 needed no
join and no error condition). The soak found three independent instances in
under two minutes, which says the interesting number is not three, it is
"however many are left".

So the class gets a boundary rather than a patch per instance. Every
goroutine a query spawns, and every entry point a query is driven through,
converts ANY panic into an error that carries the panic value and a
truncated stack, logs it at error level with the query id, and lets the
caller cancel and drain normally. The client gets SQLSTATE XX000. This is
ADDITIVE to the FatalEvalPanic contract, not a replacement: a FatalEvalPanic
still becomes its own precise error with its own SQLSTATE, and only what
recoverFatalEval declines lands here.

Nothing is swallowed. The error reaches the client, the stack reaches the
log, and QueryPanicsRecovered counts the event so the process-killer gate
can fail CI on a query that reaches one of these — a recovered panic is
still a defect, it is just no longer an outage.
