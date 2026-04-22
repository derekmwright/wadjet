package coordinator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/engine/batch"
)

// gatherResult is the terminal output of a gather receiver: the assembled
// batches from all worker gather tasks, the column schema (copied from the
// first batch), and any worker-reported error surfaced in a terminal message.
type gatherResult struct {
	batches  []*batch.RecordBatch
	columns  []string
	totalRows int64
	workerErr string
}

// runGatherReceiver subscribes to subject, collects GatherBatchMsg messages
// from `expectedTerminals` worker tasks, decodes each payload's WSHF batch,
// and returns the assembled result. Waits up to timeout for all terminals.
//
// Ordering: this MVP concatenates batches in the arrival order of their
// source messages (NATS preserves per-subject order within a single
// publisher, but inter-publisher order is not guaranteed). For ordered
// gather (GatherOrdering set), the caller applies merge-sort afterward.
func runGatherReceiver(
	ctx context.Context,
	nc *nats.Conn,
	subject string,
	expectedTerminals int,
	timeout time.Duration,
) (*gatherResult, error) {
	var (
		mu        sync.Mutex
		batches   []*batch.RecordBatch
		totalRows int64
		columns   []string
		workerErr string
		terminals int
	)
	done := make(chan struct{}, 1)

	sub, err := nc.Subscribe(subject, func(m *nats.Msg) {
		var msg distributed.GatherBatchMsg
		if err := distributed.Unmarshal(m.Data, &msg); err != nil {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if msg.Terminal {
			if msg.Err != "" && workerErr == "" {
				workerErr = msg.Err
			}
			terminals++
			if terminals >= expectedTerminals {
				select {
				case done <- struct{}{}:
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
			if workerErr == "" {
				workerErr = fmt.Sprintf("decoding gather batch: %v", err)
			}
			return
		}
		for _, b := range decoded {
			if columns == nil && len(b.Schema) > 0 {
				cols := make([]string, len(b.Schema))
				for i, c := range b.Schema {
					cols[i] = c.Name
				}
				columns = cols
			}
			totalRows += int64(b.ActiveLen())
			batches = append(batches, b)
		}
	})
	if err != nil {
		return nil, fmt.Errorf("subscribing gather subject %q: %w", subject, err)
	}
	defer sub.Unsubscribe()

	select {
	case <-done:
	case <-time.After(timeout):
		mu.Lock()
		got := terminals
		mu.Unlock()
		return nil, fmt.Errorf("gather receiver timed out after %s (got %d/%d terminals)",
			timeout, got, expectedTerminals)
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	mu.Lock()
	defer mu.Unlock()
	if workerErr != "" {
		return nil, fmt.Errorf("gather worker error: %s", workerErr)
	}
	return &gatherResult{
		batches:  batches,
		columns:  columns,
		totalRows: totalRows,
	}, nil
}
