package coordinator

import (
	"context"
	"sync"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/nats-io/nats.go"
)

// stageProgressBridge fans per-task progress messages (published by
// workers from inside their hot loops) out to the per-stage progress
// channels that awaitStageProgress reads.
//
// Lifecycle: executeStageDAG creates one bridge per query, opens a
// single NATS subscription to wadjet.task.progress.<queryID>.>, and
// stashes the bridge in ctx via withStageProgressBridge. Each
// dispatch helper Register()s its (stageID, progressCh) pair when it
// starts a stage and calls the returned cleanup func when the stage
// completes.
//
// Routing happens by stageID embedded in each TaskProgress message,
// not by NATS subject — keeps the subject layout flat.
type stageProgressBridge struct {
	mu    sync.RWMutex
	chans map[string]chan<- struct{}
	sub   *nats.Subscription
}

func newStageProgressBridge(nc *nats.Conn, queryID string) (*stageProgressBridge, error) {
	b := &stageProgressBridge{chans: make(map[string]chan<- struct{})}
	if nc == nil {
		return b, nil
	}
	subject := distributed.SubjectTaskProgress + "." + queryID + ".>"
	sub, err := nc.Subscribe(subject, func(msg *nats.Msg) {
		var tp distributed.TaskProgress
		if err := distributed.Unmarshal(msg.Data, &tp); err != nil {
			return
		}
		b.mu.RLock()
		ch, ok := b.chans[tp.StageID]
		b.mu.RUnlock()
		if !ok {
			return
		}
		select {
		case ch <- struct{}{}:
		default:
		}
	})
	if err != nil {
		return nil, err
	}
	b.sub = sub
	return b, nil
}

// Register associates a stage's progress channel with the bridge for
// the lifetime of the stage. Returns a cleanup func that the caller
// MUST defer; if cleanup is missed the bridge holds the channel
// reference past stage end (small memory leak, no correctness issue).
func (b *stageProgressBridge) Register(stageID string, ch chan<- struct{}) func() {
	if b == nil {
		return func() {}
	}
	b.mu.Lock()
	b.chans[stageID] = ch
	b.mu.Unlock()
	return func() {
		b.mu.Lock()
		delete(b.chans, stageID)
		b.mu.Unlock()
	}
}

// Close unsubscribes the NATS handler. Called by executeStageDAG on
// query completion (success or failure). Per-stage cleanup funcs
// remove their channels independently.
func (b *stageProgressBridge) Close() {
	if b == nil || b.sub == nil {
		return
	}
	_ = b.sub.Unsubscribe()
}

type stageProgressBridgeKey struct{}

// withStageProgressBridge attaches a bridge to ctx for the dispatch
// helpers to find via stageProgressBridgeFromContext. Bridge is
// per-query; nested calls just shadow if needed.
func withStageProgressBridge(ctx context.Context, b *stageProgressBridge) context.Context {
	return context.WithValue(ctx, stageProgressBridgeKey{}, b)
}

func stageProgressBridgeFromContext(ctx context.Context) *stageProgressBridge {
	if ctx == nil {
		return nil
	}
	v := ctx.Value(stageProgressBridgeKey{})
	if v == nil {
		return nil
	}
	b, _ := v.(*stageProgressBridge)
	return b
}
