package coordinator

import (
	"context"
	"sync"

	"github.com/citc-tech/wadjet/internal/dataplane"
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
	mu      sync.RWMutex
	chans   map[string]chan<- struct{}
	sub     *nats.Subscription
	dpSrv   *dataplane.Server
	queryID string
}

func newStageProgressBridge(nc *nats.Conn, dpSrv *dataplane.Server, queryID string) (*stageProgressBridge, error) {
	b := &stageProgressBridge{
		chans:   make(map[string]chan<- struct{}),
		dpSrv:   dpSrv,
		queryID: queryID,
	}
	// gRPC path: register a per-query handler on the data-plane server.
	// Bridge.Close removes it; Phase E moves TaskProgress off NATS when
	// dpSrv is set, but the NATS subscribe below is kept so mixed-mode
	// queries (legacy workers still on NATS) continue to route.
	if dpSrv != nil {
		dpSrv.RegisterTaskProgressHandler(queryID, func(tp *dataplane.TaskProgress) {
			b.route(tp.StageID)
		})
	}
	if nc == nil {
		return b, nil
	}
	subject := distributed.SubjectTaskProgress + "." + queryID + ".>"
	sub, err := nc.Subscribe(subject, func(msg *nats.Msg) {
		var tp distributed.TaskProgress
		if err := distributed.Unmarshal(msg.Data, &tp); err != nil {
			return
		}
		b.route(tp.StageID)
	})
	if err != nil {
		if dpSrv != nil {
			dpSrv.UnregisterTaskProgressHandler(queryID)
		}
		return nil, err
	}
	b.sub = sub
	return b, nil
}

// route fans a progress arrival to the stage's progress channel, if
// any is currently registered. Non-blocking — a full channel means the
// stage already has a pending tick.
func (b *stageProgressBridge) route(stageID string) {
	if b == nil {
		return
	}
	b.mu.RLock()
	ch, ok := b.chans[stageID]
	b.mu.RUnlock()
	if !ok {
		return
	}
	select {
	case ch <- struct{}{}:
	default:
	}
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

// Close unsubscribes the NATS handler AND removes the gRPC progress
// handler (if either was wired). Called by executeStageDAG on query
// completion (success or failure). Per-stage cleanup funcs remove
// their channels independently.
func (b *stageProgressBridge) Close() {
	if b == nil {
		return
	}
	if b.sub != nil {
		_ = b.sub.Unsubscribe()
	}
	if b.dpSrv != nil {
		b.dpSrv.UnregisterTaskProgressHandler(b.queryID)
	}
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
