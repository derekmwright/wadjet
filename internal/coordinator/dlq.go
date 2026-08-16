package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/nats-io/nats.go/jetstream"
)

// DLQ provides access to the dead-letter queue for failed task inspection.
type DLQ struct {
	js jetstream.JetStream
}

// NewDLQ creates a DLQ reader from a JetStream connection.
func NewDLQ(js jetstream.JetStream) *DLQ {
	return &DLQ{js: js}
}

// List returns all DLQ entries, most recent first.
// limit controls the maximum number of entries returned (0 = all, up to stream max).
func (d *DLQ) List(ctx context.Context, limit int) ([]distributed.DLQEntry, error) {
	stream, err := d.js.Stream(ctx, distributed.StreamDLQ)
	if err != nil {
		return nil, fmt.Errorf("accessing DLQ stream: %w", err)
	}

	info, err := stream.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting DLQ info: %w", err)
	}
	if info.State.Msgs == 0 {
		return nil, nil
	}

	// Create an ephemeral ordered consumer to read all messages
	cons, err := stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		return nil, fmt.Errorf("creating DLQ consumer: %w", err)
	}

	maxFetch := int(info.State.Msgs)
	if limit > 0 && limit < maxFetch {
		maxFetch = limit
	}

	fetchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	batch, err := cons.Fetch(maxFetch, jetstream.FetchMaxWait(5*time.Second))
	if err != nil {
		return nil, fmt.Errorf("fetching DLQ messages: %w", err)
	}

	var entries []distributed.DLQEntry
	for msg := range batch.Messages() {
		select {
		case <-fetchCtx.Done():
			break
		default:
		}
		var entry distributed.DLQEntry
		if err := json.Unmarshal(msg.Data(), &entry); err != nil {
			continue
		}
		entries = append(entries, entry)
	}

	// Most recent first
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Timestamp.After(entries[j].Timestamp)
	})

	if limit > 0 && len(entries) > limit {
		entries = entries[:limit]
	}

	return entries, nil
}

// Get retrieves a specific DLQ entry by its entry ID.
func (d *DLQ) Get(ctx context.Context, entryID string) (*distributed.DLQEntry, error) {
	entries, err := d.List(ctx, 0)
	if err != nil {
		return nil, err
	}
	for i := range entries {
		if entries[i].EntryID == entryID {
			return &entries[i], nil
		}
	}
	return nil, fmt.Errorf("DLQ entry %q not found", entryID)
}

// Purge deletes all DLQ entries.
func (d *DLQ) Purge(ctx context.Context) error {
	stream, err := d.js.Stream(ctx, distributed.StreamDLQ)
	if err != nil {
		return fmt.Errorf("accessing DLQ stream: %w", err)
	}
	return stream.Purge(ctx)
}

// Count returns the number of entries in the DLQ.
func (d *DLQ) Count(ctx context.Context) (uint64, error) {
	stream, err := d.js.Stream(ctx, distributed.StreamDLQ)
	if err != nil {
		return 0, fmt.Errorf("accessing DLQ stream: %w", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting DLQ info: %w", err)
	}
	return info.State.Msgs, nil
}
