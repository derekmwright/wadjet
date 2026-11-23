# Gather receiver budget and scratch

Source: internal/coordinator/gather_receiver.go — subscribeGather, moved 2026-09-11 (#1026)

subscribeGather installs the NATS subscription. Must be called BEFORE
the Gather task is published so the subscriber is present when the
worker emits batches and the terminal marker — raw-subject publishes
are not buffered for late subscribers.

workers may be nil (test code that doesn't care about liveness); when
non-nil, every received gather batch updates LastSeen for the emitting
worker via WorkerRegistry.MarkWorkerSeen.

budget caps the decoded bytes the receiver holds in coordinator heap
(<=0 = uncapped). The receiver was the last big uncharged coordinator
accumulator: a distributed SELECT * or no-LIMIT high-cardinality GROUP
BY landed its entire result in coordinator heap and OOM-killed the
process — taking every in-flight query with it. Past the budget the
receiver degrades gracefully: the remaining payload frames are appended
raw (still WSHF-encoded, not decoded) to a local scratch file, and the
result is replayed lazily disk→wire by gatherReplayStream. Only if the
scratch write itself fails does the query fail cleanly (the process
still must not die for one query's result size).

The caller that successfully subscribes MUST `defer recv.discard()` so
scratch is removed on every path where wait() never claims the result.

## Identity-aware acceptance (#1709)

A task that streams to the gather receiver can execute more than once: a
JetStream AckWait redelivery (a delivery buffered or stalled past AckWait, or
a task whose AckWait extensions stopped), a restart replaying unacked work,
the worker's own Nak after three failed result publishes, a duplicate
publish. Each execution streams its rows here. Before #1709 the receiver
appended every execution's batches and counted every terminal, so the
statement succeeded with duplicated rows — or, when a duplicate terminal was
counted in place of a task still running, with the right row count, one
group duplicated and another missing.

What every gather message carries, on both transports (`GatherBatchMsg` on
NATS, `dpv1.ResultBatch` on the gRPC data plane): the task id, an execution
id (`Task.ExecutionID`, 64 random bits drawn once per actual execution in
`Executor.Execute` — a redelivery, a replay, a duplicate, or a second copy on
the same worker each draw their own; neither the worker id nor the
coordinator's `Attempt` tells executions apart), a per-execution batch
sequence (1, 2, 3, …) and, on the terminal, the number of batches sent.

What the coordinator guarantees:

- It registers the exact task set before dispatch (`expectTasks`: the fused
  stage's tasks, the fan-out's final task, the standalone gather's task).
- Under the receiver's lock, for both entrances (the NATS decoder and the
  gRPC adapter feed the same `handleParsed`): a message missing its task or
  execution id fails the statement; so does a message for a task that was
  not registered. A task binds to the first execution observed; a message
  from any other execution of a bound task fails the statement before its
  payload is accepted. Within the bound execution, a gap, a reorder or a
  terminal count that does not match the batches received fails the
  statement: neither transport promises that a batch sent before a
  reconnect arrives, or arrives before a later one (gRPC `SendMsg` returns
  once the message is queued, and the sink picks the current stream per
  send; a NATS publish in flight at a disconnect can be lost).
- Success is declared only when every registered task's bound execution has
  sent a clean terminal. A result notification never stands in for a gather
  terminal.
- Deciding success or failure closes the receiver: every later message is
  discarded without changing the returned result or recreating scratch. A
  duplicate that arrives after valid completion does not fail the statement
  retroactively; one that arrives while any task of the stage is still
  running does.
- The failure is an `XX000` error naming the task and both execution ids,
  and it is delivered to the statement's cancellation path (it cancels the
  stage DAG), so the fused path's wait for the DAG is interrupted rather
  than read afterwards.
- A first execution that streamed some rows and then died, with no
  replacement, ends in the bounded gather/stage timeout — never in success
  over its partial rows. One that died before sending anything is simply
  never observed, and its replacement binds.
- A worker's own failure (an error terminal) keeps its existing path: it is
  reported when every task has finished, with the worker's error text, and
  the task's coded result error still decides what the client sees.

What it does NOT guarantee. The rule fences what reaches the gather
receiver, nothing upstream of it. These remain unfenced by execution and are
a separate decision (recorded on #1709):

- **Stage outputs.** `writeStageOutput` writes task-id-derived keys with no
  execution fencing; two executions of one producer overwrite each other's
  partitions, and they need not write identical bytes (scheduling, spill
  layout, volatile expressions). A consumer can read a mixture.
- **Dynamic filters.** Their completeness is counted per task id; nothing
  couples a filter artifact to the execution whose output was selected (a
  source-derived risk recorded on #1709, not reproduced).
- **Same-worker scratch paths.** `scratch_dir.go` distinguishes
  executor/query/task, not execution; two copies on one worker can share
  files and cleanup.
- **Result notifications.** `taskRetrier` and the single-task build and
  aggregate-cache subscribers count a task id once, but carry no execution
  fencing.
- Surviving a redelivery: the statement fails loudly; it does not keep one
  execution's rows and drop the other's (buffering per execution was not
  built).
