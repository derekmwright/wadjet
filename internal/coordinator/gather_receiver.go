// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/derekmwright/wadjet/internal/dataplane"
	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/wshf"
)

// gatherResult is the terminal output of a gather receiver: the assembled
// batches from all worker gather tasks, the column schema (copied from the
// first batch), and any worker-reported error surfaced in a terminal message.
//
// When the result exceeded the gather budget, batches holds only the
// in-memory prefix (≈budget bytes) and the remainder sits as raw payload
// frames in the scratch file at spillPath, to be replayed lazily by
// gatherReplayStream. The renamer, when set, must be applied to each
// replayed batch (the prefix already has it applied).
type gatherResult struct {
	batches    []*batch.RecordBatch
	columns    []string
	totalRows  int64
	workerErr  string
	spillPath  string // "" = fully in memory
	spillBytes int64
	renamer    *batchRenamer
}

// gatherReceiver is a two-phase receiver: subscribeGather installs the NATS
// subscription synchronously (guaranteeing no messages are lost to a race
// with the worker publishing before the subscriber exists), and wait() blocks
// until every registered task has completed or ctx/timeout fires.
//
// Acceptance is identity-aware (#1709). A task can execute more than once —
// a JetStream redelivery, a replay after a restart, a Nak after a failed
// result publish, a duplicate publish — and each copy streams its rows here.
// expectTasks registers the exact task set BEFORE dispatch; handleParsed
// then, under mu, rejects a message missing its task or execution id or
// naming an unregistered task, binds each task to the FIRST execution it
// observes, and fails the statement on a message from any other execution
// before accepting its payload. Within the bound execution, batches must
// arrive numbered 1, 2, 3, … and the terminal's count must match. Success
// is declared only when every registered task's bound execution has sent a
// clean terminal; deciding success or failure closes the receiver, and
// every later message is discarded without touching the result.
type gatherReceiver struct {
	sub       *nats.Subscription
	workers   *WorkerRegistry // nil-safe; used to mark worker liveness from gather batches
	budget    int64           // max decoded bytes held in memory; <=0 = uncapped
	mu        sync.Mutex
	batches   []*batch.RecordBatch
	accBytes  int64 // decoded bytes accumulated in memory (MemBytes sum)
	totalRows int64
	columns   []string
	workerErr string
	// guard holds every worker's gather payload to ONE description of the
	// relation — see wshf.SchemaGuard. The receiver resolves r.columns from
	// the first batch it decodes and every later one is read under that, so a
	// worker whose payload declares a column differently is reinterpreted
	// rather than refused (#685).
	guard wshf.SchemaGuard
	// tasks is the registered task set (expectTasks), nil until then; a
	// task's state records the execution it is bound to and how far that
	// execution's stream has got. completed counts tasks whose bound
	// execution sent its terminal.
	tasks     map[string]*gatherTaskState
	completed int
	// closed is set the moment success or failure is decided; failErr is the
	// identity/stream anomaly that decided failure, if one did. onFail, when
	// set, delivers that failure to the statement's cancellation path so it
	// interrupts whatever the statement is waiting on (the fused path waits
	// for the whole DAG before it calls wait()).
	closed   bool
	failErr  error
	onFail   func(error)
	done     chan struct{}
	msgCount atomic.Int64 // diagnostic: incremented on every message received

	// Spill state: once accBytes reaches budget, raw payload frames are
	// appended to a local scratch file instead of being decoded.
	spillFile   *os.File
	spillW      *bufio.Writer
	spillPath   string
	spillBytes  int64
	spillFailed bool // scratch write failed → clean per-query fail (old behavior)
	claimed     bool // wait() handed spill ownership to the gatherResult
}

// subscribeGather must run BEFORE publishing the Gather task: raw NATS subjects
// do not buffer batches or terminal markers for late subscribers.
// Non-nil workers receives MarkWorkerSeen updates for each emitting worker;
// nil is allowed for tests that do not need liveness.
// budget caps decoded coordinator-heap bytes (<=0 uncapped); beyond it, append
// remaining frames still WSHF-encoded to scratch for lazy gatherReplayStream replay.
// Scratch-write failure fails the query cleanly; one result must not kill the process.
// After successful subscription, the caller MUST defer recv.discard() to remove
// scratch on every path where wait() does not claim the result.
// See docs/internals/gather-receiver-budget-and-scratch.md for the design.
func subscribeGather(nc *nats.Conn, subject string, workers *WorkerRegistry, budget int64) (*gatherReceiver, error) {
	r := &gatherReceiver{
		workers: workers,
		budget:  budget,
		done:    make(chan struct{}, 1),
	}
	sub, err := nc.Subscribe(subject, r.handle)
	if err != nil {
		return nil, fmt.Errorf("subscribing gather subject %q: %w", subject, err)
	}
	// Flush so the subscription is registered on the server before any
	// publish can race past us. (Subscribe returns after the client-side
	// record exists, but the interest isn't propagated to the NATS
	// server until the next flush.)
	if err := nc.Flush(); err != nil {
		sub.Unsubscribe()
		return nil, fmt.Errorf("flushing gather subscription %q: %w", subject, err)
	}
	r.sub = sub
	return r, nil
}

func (r *gatherReceiver) handle(m *nats.Msg) {
	var msg distributed.GatherBatchMsg
	if err := distributed.Unmarshal(m.Data, &msg); err != nil {
		return
	}
	r.handleParsed(&msg)
}

// handleParsed is the transport-neutral entry point. NATS path calls it
// after Unmarshal; gRPC data-plane path calls it after translating a
// ResultBatch proto into the equivalent GatherBatchMsg. The behavior
// must be identical for both transports.
func (r *gatherReceiver) handleParsed(msg *distributed.GatherBatchMsg) {
	if h := gatherParsedTestHook.Load(); h != nil {
		(*h)(msg)
	}
	if r.workers != nil {
		r.workers.MarkWorkerSeen(msg.WorkerID)
	}
	r.msgCount.Add(1)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		// Success or failure is already decided: a late frame — a duplicate
		// execution after valid completion, or anything after a failure —
		// changes nothing and must not recreate scratch.
		return
	}
	st, ok := r.admitLocked(msg)
	if !ok {
		return
	}
	if msg.Terminal {
		st.done = true
		r.completed++
		if msg.Err != "" && r.workerErr == "" {
			r.workerErr = msg.Err
		}
		if r.completed == len(r.tasks) {
			r.closeLocked()
		}
		return
	}
	if len(msg.Payload) == 0 {
		return
	}
	// Once the query is failing (scratch write error) or the result has
	// been claimed/discarded, skip the work for the remaining stream.
	if r.spillFailed || r.claimed {
		return
	}
	// In-memory prefix is full — spill the raw frame. The first payload
	// always decodes (accBytes starts at 0 < budget), so columns are
	// already resolved by the time spilling starts.
	if r.budget > 0 && r.accBytes >= r.budget {
		r.spillFrameLocked(msg)
		return
	}
	decoded, err := wshf.DecodeBatches(msg.Payload)
	if err != nil {
		if r.workerErr == "" {
			r.workerErr = fmt.Sprintf("decoding gather batch: %v", err)
		}
		return
	}
	if err := r.guard.CheckBatches("a gather batch from worker "+msg.WorkerID, decoded); err != nil {
		if r.workerErr == "" {
			r.workerErr = err.Error()
		}
		return
	}
	for _, b := range decoded {
		if r.columns == nil && len(b.Schema) > 0 {
			cols := make([]string, len(b.Schema))
			for i, c := range b.Schema {
				cols[i] = c.Name
			}
			r.columns = cols
		}
		r.totalRows += int64(b.ActiveLen())
		r.accBytes += b.MemBytes()
		r.batches = append(r.batches, b)
	}
}

// gatherParsedTestHook, when set, sees every gather message before
// acceptance; the #1709 redelivery regressions use it to observe execution
// ids and order deliveries. Nil outside tests: one atomic load per message.
var gatherParsedTestHook atomic.Pointer[func(msg *distributed.GatherBatchMsg)]

// gatherTaskState is one registered task's acceptance state.
type gatherTaskState struct {
	execID   string // the bound execution; "" until the task's first message
	accepted uint64 // batch messages accepted from execID (the last Seq seen)
	done     bool   // execID's terminal arrived
}

// gatherIdentitySQLState is the SQLSTATE of a gather-acceptance failure: an
// internal execution failure, the class the coordinator already reports a
// failed task under (task_retry.go stageTaskFailure).
const gatherIdentitySQLState = exec.SQLStateInternalError

// admitLocked applies the identity and stream rules to one message and
// returns the task state it belongs to; ok=false means the message was
// rejected and the statement failed. Caller holds r.mu and has checked that
// the receiver is still open.
func (r *gatherReceiver) admitLocked(msg *distributed.GatherBatchMsg) (*gatherTaskState, bool) {
	kind := "batch"
	if msg.Terminal {
		kind = "terminal"
	}
	if msg.TaskID == "" || msg.ExecutionID == "" {
		r.failLocked(sqlerr.New(gatherIdentitySQLState,
			"gather: a result %s from worker %q carries no task or execution identity (task %q, execution %q); a worker that predates execution-identified gather streaming cannot serve this coordinator",
			kind, msg.WorkerID, msg.TaskID, msg.ExecutionID))
		return nil, false
	}
	st := r.tasks[msg.TaskID]
	if st == nil {
		r.failLocked(sqlerr.New(gatherIdentitySQLState,
			"gather: a result %s arrived for task %s (execution %s, worker %q), which this statement did not dispatch",
			kind, msg.TaskID, msg.ExecutionID, msg.WorkerID))
		return nil, false
	}
	if st.execID == "" {
		st.execID = msg.ExecutionID
	} else if st.execID != msg.ExecutionID {
		r.failLocked(sqlerr.New(gatherIdentitySQLState,
			"gather: task %s executed more than once: execution %s is streaming its result, and a %s arrived from execution %s (worker %q); refusing to combine two executions' rows",
			msg.TaskID, st.execID, kind, msg.ExecutionID, msg.WorkerID))
		return nil, false
	}
	if st.done {
		r.failLocked(sqlerr.New(gatherIdentitySQLState,
			"gather: task %s execution %s sent a %s after its terminal",
			msg.TaskID, st.execID, kind))
		return nil, false
	}
	if msg.Terminal {
		// A failed execution's terminal fails the statement with the
		// worker's own error; its count is not checked, because a send that
		// failed (the usual reason for the failure) consumed a number
		// without delivering a batch.
		if msg.Err == "" && msg.BatchCount != st.accepted {
			r.failLocked(sqlerr.New(gatherIdentitySQLState,
				"gather: task %s execution %s finished after sending %d result batches, but %d arrived; refusing an incomplete result",
				msg.TaskID, st.execID, msg.BatchCount, st.accepted))
			return nil, false
		}
		return st, true
	}
	if msg.Seq != st.accepted+1 {
		r.failLocked(sqlerr.New(gatherIdentitySQLState,
			"gather: task %s execution %s: result batch %d arrived where batch %d was expected; refusing a stream with a gap or out of order",
			msg.TaskID, st.execID, msg.Seq, st.accepted+1))
		return nil, false
	}
	st.accepted++
	return st, true
}

// failLocked decides failure: the receiver closes, drops what it holds,
// wakes wait(), and delivers the error to the statement's cancellation path.
// Caller holds r.mu; a receiver already closed keeps its first decision.
func (r *gatherReceiver) failLocked(err error) {
	if r.closed {
		return
	}
	r.failErr = err
	r.batches = nil
	r.spillFailed = true // no frame may recreate scratch
	r.dropSpillLocked()
	r.closeLocked()
	if r.onFail != nil {
		r.onFail(err)
	}
}

// closeLocked marks the decision taken and wakes wait(). Caller holds r.mu.
func (r *gatherReceiver) closeLocked() {
	r.closed = true
	select {
	case r.done <- struct{}{}:
	default:
	}
}

// failure returns the identity/stream anomaly that failed the statement, or
// nil. The fused path reads it after the DAG wait returns: when the anomaly
// cancelled the DAG, the stage error is only the cancellation's echo.
func (r *gatherReceiver) failure() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.failErr
}

// spillFrameLocked appends one raw payload frame ([uint32 LE length][WSHF
// payload]) to the scratch file. Decode cost is paid once, at replay.
// Row accounting uses the message's RowCount (stamped by the worker sink);
// the actual sent-row count downstream comes from the replayed batches, so
// a zero RowCount from an old worker only skews the TotalRows statistic.
// Caller holds r.mu.
func (r *gatherReceiver) spillFrameLocked(msg *distributed.GatherBatchMsg) {
	if r.spillFile == nil {
		f, err := os.CreateTemp("", "wadjet-gather-*.frames")
		if err != nil {
			r.failSpillLocked(fmt.Errorf("creating gather scratch: %w", err))
			return
		}
		r.spillFile = f
		r.spillPath = f.Name()
		r.spillW = bufio.NewWriterSize(f, 1<<16)
	}
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(msg.Payload)))
	if _, err := r.spillW.Write(hdr[:]); err != nil {
		r.failSpillLocked(fmt.Errorf("writing gather scratch: %w", err))
		return
	}
	if _, err := r.spillW.Write(msg.Payload); err != nil {
		r.failSpillLocked(fmt.Errorf("writing gather scratch: %w", err))
		return
	}
	r.spillBytes += int64(len(msg.Payload))
	r.totalRows += int64(msg.RowCount)
}

// failSpillLocked records a scratch failure: the query fails cleanly (the
// accumulated result is dropped immediately to relieve pressure; terminals
// keep counting so wait() returns promptly). Caller holds r.mu.
func (r *gatherReceiver) failSpillLocked(err error) {
	r.spillFailed = true
	if r.workerErr == "" {
		r.workerErr = fmt.Sprintf(
			"result exceeded the coordinator gather budget (%d MB in memory) and spilling to scratch failed: %v — add a LIMIT, raise the budget (Config.GatherResultBudget / GOMEMLIMIT), or free local disk",
			r.accBytes>>20, err)
	}
	r.batches = nil
	r.dropSpillLocked()
}

// dropSpillLocked closes and removes the scratch file. Caller holds r.mu.
func (r *gatherReceiver) dropSpillLocked() {
	if r.spillFile != nil {
		r.spillFile.Close()
		r.spillFile = nil
		r.spillW = nil
	}
	if r.spillPath != "" {
		os.Remove(r.spillPath)
		r.spillPath = ""
	}
}

// discard releases everything the receiver still owns — buffered batches
// and unclaimed spill scratch. No-op once wait() has handed the result
// off. Idempotent; `defer recv.discard()` at every subscribe site is the
// contract that keeps scratch from leaking when wait() is never reached.
func (r *gatherReceiver) discard() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.claimed {
		return
	}
	r.spillFailed = true // block any late frame from recreating scratch
	r.closed = true      // and any late message from being admitted
	r.batches = nil
	r.dropSpillLocked()
}

// expectTasks registers the exact set of task ids whose streams this
// receiver completes on. It must run BEFORE those tasks are published: a
// message for a task not yet registered is rejected as unknown. Used by the
// standalone gather (its one task) and by gather fusion, where the receiver
// is subscribed early in executeStageDAG but the task set is only known
// once the upstream stage's dispatcher has built it. An empty set completes
// at once.
func (r *gatherReceiver) expectTasks(ids []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.tasks == nil {
		r.tasks = make(map[string]*gatherTaskState, len(ids))
	}
	for _, id := range ids {
		if _, ok := r.tasks[id]; !ok {
			r.tasks[id] = &gatherTaskState{}
		}
	}
	if !r.closed && r.completed == len(r.tasks) {
		r.closeLocked()
	}
}

// registerWithDataPlane installs a ResultHandler on the data-plane
// gRPC server so workers that publish results via the gRPC stream are
// routed to the same handleParsed entry point as the NATS path. Both
// transports flow into the same accumulator; receive ordering between
// them is unspecified but each batch is independent. Returns a cleanup
// function that removes the handler; cheap no-op if srv is nil.
func (r *gatherReceiver) registerWithDataPlane(srv *dataplane.Server, queryID string) func() {
	if srv == nil {
		return func() {}
	}
	srv.RegisterResultHandler(queryID, func(rb *dataplane.ResultBatch) {
		r.handleParsed(&distributed.GatherBatchMsg{
			Terminal: rb.Terminal,
			RowCount: rb.RowCount,
			Payload:  rb.Payload,
			Err:      rb.Err,
			WorkerID: rb.WorkerID,

			TaskID:      rb.TaskID,
			ExecutionID: rb.ExecutionID,
			Seq:         rb.Seq,
			BatchCount:  rb.BatchCount,
		})
	})
	return func() { srv.UnregisterResultHandler(queryID) }
}

// wait blocks until all expected terminal messages arrive, ctx is done,
// or the timeout fires. Always unsubscribes before returning. On success
// the returned gatherResult takes ownership of the spill scratch (if any);
// on every error path the receiver keeps ownership and the caller's
// deferred discard() removes it.
func (r *gatherReceiver) wait(ctx context.Context, timeout time.Duration) (*gatherResult, error) {
	if r.sub != nil {
		defer r.sub.Unsubscribe()
	}
	select {
	case <-r.done:
	case <-time.After(timeout):
		r.mu.Lock()
		got, want := r.completed, len(r.tasks)
		r.mu.Unlock()
		return nil, fmt.Errorf("gather receiver timed out after %s (got %d/%d terminals)",
			timeout, got, want)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failErr != nil {
		return nil, r.failErr
	}
	if r.workerErr != "" {
		return nil, fmt.Errorf("gather worker error: %s", r.workerErr)
	}
	gr := &gatherResult{
		batches:   r.batches,
		columns:   r.columns,
		totalRows: r.totalRows,
	}
	if r.spillFile != nil {
		if err := r.spillW.Flush(); err != nil {
			r.dropSpillLocked()
			return nil, fmt.Errorf("flushing gather scratch: %w", err)
		}
		if err := r.spillFile.Close(); err != nil {
			r.spillFile = nil
			r.spillW = nil
			r.dropSpillLocked()
			return nil, fmt.Errorf("closing gather scratch: %w", err)
		}
		r.spillFile = nil
		r.spillW = nil
		gr.spillPath = r.spillPath
		gr.spillBytes = r.spillBytes
		r.spillPath = ""
	}
	r.claimed = true
	r.batches = nil
	return gr, nil
}
