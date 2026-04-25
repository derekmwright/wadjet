package coordinator

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/engine/batch"
)

// gatherResult is the terminal output of a gather receiver: the assembled
// batches from all worker gather tasks, the column schema (copied from the
// first batch), and any worker-reported error surfaced in a terminal message.
type gatherResult struct {
	batches   []*batch.RecordBatch
	columns   []string
	totalRows int64
	workerErr string
}

// gatherReceiver is a two-phase receiver: subscribeGather installs the NATS
// subscription synchronously (guaranteeing no messages are lost to a race
// with the worker publishing before the subscriber exists), and wait() blocks
// until `expectedTerminals` terminal messages arrive or ctx/timeout fires.
type gatherReceiver struct {
	sub               *nats.Subscription
	mu                sync.Mutex
	batches           []*batch.RecordBatch
	totalRows         int64
	columns           []string
	workerErr         string
	terminals         int
	expectedTerminals int
	done              chan struct{}
	msgCount          atomic.Int64 // diagnostic: incremented on every message received
}

// subscribeGather installs the NATS subscription. Must be called BEFORE
// the Gather task is published so the subscriber is present when the
// worker emits batches and the terminal marker — raw-subject publishes
// are not buffered for late subscribers.
func subscribeGather(nc *nats.Conn, subject string, expectedTerminals int) (*gatherReceiver, error) {
	r := &gatherReceiver{
		expectedTerminals: expectedTerminals,
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
		r.batches = append(r.batches, b)
	}
}

// wait blocks until all expected terminal messages arrive, ctx is done,
// or the timeout fires. Always unsubscribes before returning.
func (r *gatherReceiver) wait(ctx context.Context, timeout time.Duration) (*gatherResult, error) {
	defer r.sub.Unsubscribe()
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
	return &gatherResult{
		batches:   r.batches,
		columns:   r.columns,
		totalRows: r.totalRows,
	}, nil
}

// runGatherReceiver is a back-compat convenience: subscribe and wait in one
// call. New callers should use subscribeGather + wait() so they can publish
// the Gather task BETWEEN the subscribe and the wait, eliminating the race
// where the worker publishes results before the subscription is installed.
func runGatherReceiver(
	ctx context.Context,
	nc *nats.Conn,
	subject string,
	expectedTerminals int,
	timeout time.Duration,
) (*gatherResult, error) {
	r, err := subscribeGather(nc, subject, expectedTerminals)
	if err != nil {
		return nil, err
	}
	return r.wait(ctx, timeout)
}
