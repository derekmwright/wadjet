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

	"github.com/citc-tech/wadjet/internal/dataplane"
	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/engine/batch"
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
// until `expectedTerminals` terminal messages arrive or ctx/timeout fires.
type gatherReceiver struct {
	sub               *nats.Subscription
	workers           *WorkerRegistry // nil-safe; used to mark worker liveness from gather batches
	budget            int64           // max decoded bytes held in memory; <=0 = uncapped
	mu                sync.Mutex
	batches           []*batch.RecordBatch
	accBytes          int64 // decoded bytes accumulated in memory (MemBytes sum)
	totalRows         int64
	columns           []string
	workerErr         string
	terminals         int
	expectedTerminals int
	done              chan struct{}
	msgCount          atomic.Int64 // diagnostic: incremented on every message received

	// Spill state: once accBytes reaches budget, raw payload frames are
	// appended to a local scratch file instead of being decoded.
	spillFile   *os.File
	spillW      *bufio.Writer
	spillPath   string
	spillBytes  int64
	spillFailed bool // scratch write failed → clean per-query fail (old behavior)
	claimed     bool // wait() handed spill ownership to the gatherResult
}

// subscribeGather installs the NATS subscription. Must be called BEFORE
// the Gather task is published so the subscriber is present when the
// worker emits batches and the terminal marker — raw-subject publishes
// are not buffered for late subscribers.
//
// workers may be nil (test code that doesn't care about liveness); when
// non-nil, every received gather batch updates LastSeen for the emitting
// worker via WorkerRegistry.MarkWorkerSeen.
//
// budget caps the decoded bytes the receiver holds in coordinator heap
// (<=0 = uncapped). The receiver was the last big uncharged coordinator
// accumulator: a distributed SELECT * or no-LIMIT high-cardinality GROUP
// BY landed its entire result in coordinator heap and OOM-killed the
// process — taking every in-flight query with it. Past the budget the
// receiver degrades gracefully: the remaining payload frames are appended
// raw (still WSHF-encoded, not decoded) to a local scratch file, and the
// result is replayed lazily disk→wire by gatherReplayStream. Only if the
// scratch write itself fails does the query fail cleanly (the process
// still must not die for one query's result size).
//
// The caller that successfully subscribes MUST `defer recv.discard()` so
// scratch is removed on every path where wait() never claims the result.
func subscribeGather(nc *nats.Conn, subject string, expectedTerminals int, workers *WorkerRegistry, budget int64) (*gatherReceiver, error) {
	r := &gatherReceiver{
		expectedTerminals: expectedTerminals,
		workers:           workers,
		budget:            budget,
		done:              make(chan struct{}, 1),
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
	if r.workers != nil {
		r.workers.MarkWorkerSeen(msg.WorkerID)
	}
	r.msgCount.Add(1)
	r.mu.Lock()
	defer r.mu.Unlock()
	if msg.Terminal {
		if msg.Err != "" && r.workerErr == "" {
			r.workerErr = msg.Err
		}
		r.terminals++
		if r.terminals >= r.expectedTerminals {
			select {
			case r.done <- struct{}{}:
			default:
			}
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
	decoded, err := readShuffleBatches(msg.Payload)
	if err != nil {
		if r.workerErr == "" {
			r.workerErr = fmt.Sprintf("decoding gather batch: %v", err)
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
	r.batches = nil
	r.dropSpillLocked()
}

// SetExpectedTerminals updates the terminal-count threshold after the
// subscription is already installed. Used by gather fusion: the receiver
// is created early in executeStageDAG (so no early publishes are lost),
// but the actual fragment task count is not known until the upstream
// stage's dispatcher computes it. The dispatcher calls this before
// publishing tasks. Re-arms the done signal if already-arrived terminals
// meet or exceed the new threshold (handles the race where every fragment
// task finishes before the dispatcher returns from PublishTasks).
func (r *gatherReceiver) SetExpectedTerminals(n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.expectedTerminals = n
	if r.terminals >= r.expectedTerminals {
		select {
		case r.done <- struct{}{}:
		default:
		}
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
		got := r.terminals
		r.mu.Unlock()
		return nil, fmt.Errorf("gather receiver timed out after %s (got %d/%d terminals)",
			timeout, got, r.expectedTerminals)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
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
