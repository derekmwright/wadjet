package dataplane

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	dpv1 "github.com/citc-tech/wadjet/gen/dataplane/v1"
)

// ErrNoWorkers is returned by SendTaskDispatch when no worker is
// currently connected to the data-plane server.
var ErrNoWorkers = errors.New("dataplane: no connected workers")

// ResultBatch is the transport-neutral form of a worker→coord result
// chunk. The gRPC adapter translates ResultBatch proto messages into
// this struct before invoking a registered ResultHandler; coord owns
// the handler and routes by QueryID. Mirrors distributed.GatherBatchMsg
// minus the QueryID (which gRPC needs for routing — NATS gets it from
// the subject).
type ResultBatch struct {
	QueryID  string
	WorkerID string
	Terminal bool
	RowCount int32
	Payload  []byte // WSHF-encoded
	Err      string
}

// ResultHandler consumes ResultBatch messages for a single query.
// Registered by coord with Server.RegisterResultHandler at query start
// and removed via UnregisterResultHandler at query end. Must be
// thread-safe — multiple workers may stream concurrently to the same
// query.
type ResultHandler func(*ResultBatch)

// TaskProgress is the transport-neutral form of a worker→coord
// progress update. Mirrors distributed.TaskProgress (the NATS path's
// shape) minus the JSON tags so coord-side bridges can reuse the
// existing handler bodies.
type TaskProgress struct {
	QueryID           string
	StageID           string
	TaskID            string
	WorkerID          string
	RowsProcessed     int64
	BytesProcessed    int64
	TimestampUnixNano int64
}

// TaskProgressHandler consumes TaskProgress messages. Server invokes
// the global handler (used by WorkerRegistry for liveness) AND the
// per-query handler (used by stageProgressBridge) on every arrival;
// both are optional.
type TaskProgressHandler func(*TaskProgress)

// Server is the coord-side data-plane gRPC listener. Workers dial it
// and open one long-lived bidi stream each.
type Server struct {
	dpv1.UnimplementedDataPlaneServer

	cfg    ServerConfig
	logger *slog.Logger

	grpcSrv *grpc.Server
	lis     net.Listener

	mu      sync.RWMutex
	workers map[string]*workerSession // keyed by worker_id
	order   []string                  // ConnectedWorkers stable ordering for RR

	rrCursor atomic.Uint64

	resultHandlers   sync.Map     // map[string]ResultHandler keyed by query_id
	progressHandlers sync.Map     // map[string]TaskProgressHandler keyed by query_id
	globalProgress   atomic.Value // TaskProgressHandler; fires on every TaskProgress
}

// workerSession is the coord's view of a connected worker. Phase A
// held just identity; Phase C adds the per-worker outbound stream so
// the scheduler can push TaskDispatch messages over it. gRPC
// stream.Send is NOT safe for concurrent use, so sendMu serializes
// writes; reads (Recv) only happen on the Connect goroutine.
type workerSession struct {
	id        string
	buildSHA  string
	numCPUs   int32
	memLimit  int64
	connected time.Time

	sendMu sync.Mutex
	stream grpc.BidiStreamingServer[dpv1.WorkerEnvelope, dpv1.CoordEnvelope]
}

// NewServer constructs a Server. Call Start to begin accepting.
func NewServer(cfg ServerConfig, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		cfg:     cfg,
		logger:  logger.With("component", "dataplane.server"),
		workers: make(map[string]*workerSession),
	}
}

// Start binds the listener and runs the gRPC server in the background.
// Returns once the listener is bound; the serve loop runs until Stop.
func (s *Server) Start() error {
	lis, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return fmt.Errorf("dataplane: listen on %s: %w", s.cfg.Addr, err)
	}
	s.lis = lis

	var opts []grpc.ServerOption
	if s.cfg.TLSConfig != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(s.cfg.TLSConfig)))
	} else {
		opts = append(opts, grpc.Creds(insecure.NewCredentials()))
	}
	if s.cfg.MaxConcurrentStreams > 0 {
		opts = append(opts, grpc.MaxConcurrentStreams(s.cfg.MaxConcurrentStreams))
	}

	s.grpcSrv = grpc.NewServer(opts...)
	dpv1.RegisterDataPlaneServer(s.grpcSrv, s)

	go func() {
		if err := s.grpcSrv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			s.logger.Error("serve loop exited", "err", err)
		}
	}()

	s.logger.Info("dataplane server listening", "addr", lis.Addr().String(), "tls", s.cfg.TLSConfig != nil)
	return nil
}

// Stop shuts the server down. Tries graceful first (waits up to
// gracePeriod for in-flight RPCs to finish), then forces if any are
// still active. A gracePeriod of 0 means force immediately, useful for
// tests where worker streams stay open by design.
func (s *Server) Stop(gracePeriod time.Duration) {
	if s.grpcSrv == nil {
		return
	}
	if gracePeriod <= 0 {
		s.grpcSrv.Stop()
		return
	}
	done := make(chan struct{})
	go func() {
		s.grpcSrv.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(gracePeriod):
		s.grpcSrv.Stop()
		<-done
	}
}

// Addr returns the actual bound address (useful when Cfg.Addr was ":0").
func (s *Server) Addr() string {
	if s.lis == nil {
		return s.cfg.Addr
	}
	return s.lis.Addr().String()
}

// Connect implements DataPlaneServer. Phase A only handshakes
// Hello → Welcome and holds the stream open until the worker closes.
// Phases B–E route TaskDispatch outbound and consume the worker's
// result/progress messages.
func (s *Server) Connect(stream grpc.BidiStreamingServer[dpv1.WorkerEnvelope, dpv1.CoordEnvelope]) error {
	first, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("dataplane: read Hello: %w", err)
	}
	hello := first.GetHello()
	if hello == nil {
		return fmt.Errorf("dataplane: first message must be Hello, got %T", first.GetMsg())
	}

	sess := &workerSession{
		id:        hello.GetWorkerId(),
		buildSHA:  hello.GetBuildSha(),
		numCPUs:   hello.GetNumCpus(),
		memLimit:  hello.GetGomemlimit(),
		connected: time.Now(),
		stream:    stream,
	}
	if err := s.register(sess); err != nil {
		return err
	}
	defer s.unregister(sess.id)

	s.logger.Info("worker connected",
		"worker_id", sess.id,
		"build_sha", sess.buildSHA,
		"num_cpus", sess.numCPUs,
		"gomemlimit_mb", sess.memLimit/(1<<20))

	welcome := &dpv1.CoordEnvelope{
		Msg: &dpv1.CoordEnvelope_Welcome{
			Welcome: &dpv1.Welcome{
				ClusterId:  s.cfg.ClusterID,
				ServerTime: time.Now().UnixNano(),
			},
		},
	}
	if err := stream.Send(welcome); err != nil {
		return fmt.Errorf("dataplane: send Welcome: %w", err)
	}

	// Phase A: hold the stream open. Future phases consume task results,
	// progress, completion here.
	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			s.logger.Info("worker disconnected", "worker_id", sess.id, "reason", ctx.Err())
			return nil
		default:
		}

		msg, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				s.logger.Info("worker stream closed cleanly", "worker_id", sess.id)
				return nil
			}
			s.logger.Info("worker stream error", "worker_id", sess.id, "err", err)
			return err
		}
		s.routeWorkerEnvelope(msg)
	}
}

// routeWorkerEnvelope dispatches a post-Hello message to its handler.
// Phase B handles ResultBatch; Phase E adds TaskProgress.
func (s *Server) routeWorkerEnvelope(env *dpv1.WorkerEnvelope) {
	switch m := env.GetMsg().(type) {
	case *dpv1.WorkerEnvelope_ResultBatch:
		s.dispatchResultBatch(m.ResultBatch)
	case *dpv1.WorkerEnvelope_TaskProgress:
		s.dispatchTaskProgress(m.TaskProgress)
	default:
		// Unknown / not-yet-implemented variant; drop.
	}
}

func (s *Server) dispatchResultBatch(rb *dpv1.ResultBatch) {
	v, ok := s.resultHandlers.Load(rb.GetQueryId())
	if !ok {
		// No registered handler — query may have already finished or
		// never registered for gRPC delivery. Drop, like NATS would
		// drop a publish with no subscriber.
		return
	}
	h, ok := v.(ResultHandler)
	if !ok {
		return
	}
	h(&ResultBatch{
		QueryID:  rb.GetQueryId(),
		WorkerID: rb.GetWorkerId(),
		Terminal: rb.GetTerminal(),
		RowCount: rb.GetRowCount(),
		Payload:  rb.GetPayload(),
		Err:      rb.GetErr(),
	})
}

// RegisterResultHandler installs a per-query handler. Multiple
// concurrent registrations for the same queryID overwrite the previous
// one (workers can only stream to whichever handler is current). Must
// be paired with UnregisterResultHandler when the query finishes.
func (s *Server) RegisterResultHandler(queryID string, h ResultHandler) {
	if s == nil {
		return
	}
	s.resultHandlers.Store(queryID, h)
}

// UnregisterResultHandler removes the handler. Late-arriving batches
// after this call are dropped. Safe to call when no handler was ever
// registered.
func (s *Server) UnregisterResultHandler(queryID string) {
	if s == nil {
		return
	}
	s.resultHandlers.Delete(queryID)
}

func (s *Server) dispatchTaskProgress(tp *dpv1.TaskProgress) {
	out := &TaskProgress{
		QueryID:           tp.GetQueryId(),
		StageID:           tp.GetStageId(),
		TaskID:            tp.GetTaskId(),
		WorkerID:          tp.GetWorkerId(),
		RowsProcessed:     tp.GetRowsProcessed(),
		BytesProcessed:    tp.GetBytesProcessed(),
		TimestampUnixNano: tp.GetTimestampUnixNano(),
	}
	// Global handler fires on every TaskProgress (used by
	// WorkerRegistry for worker liveness).
	if v := s.globalProgress.Load(); v != nil {
		if h, ok := v.(TaskProgressHandler); ok && h != nil {
			h(out)
		}
	}
	// Per-query handler — fans to the stage bridge for the active
	// query, if registered.
	if v, ok := s.progressHandlers.Load(out.QueryID); ok {
		if h, ok := v.(TaskProgressHandler); ok && h != nil {
			h(out)
		}
	}
}

// SetGlobalTaskProgressHandler installs (or clears, when h is nil) the
// global TaskProgress handler. Used by WorkerRegistry to drive the
// multi-signal liveness path off gRPC TaskProgress arrivals.
func (s *Server) SetGlobalTaskProgressHandler(h TaskProgressHandler) {
	if s == nil {
		return
	}
	if h == nil {
		// atomic.Value disallows storing nil. Replace with a typed no-op.
		s.globalProgress.Store(TaskProgressHandler(func(*TaskProgress) {}))
		return
	}
	s.globalProgress.Store(h)
}

// RegisterTaskProgressHandler installs the per-query progress handler.
// Each query overrides any prior registration for the same queryID.
// Must be paired with UnregisterTaskProgressHandler at query end.
func (s *Server) RegisterTaskProgressHandler(queryID string, h TaskProgressHandler) {
	if s == nil || h == nil {
		return
	}
	s.progressHandlers.Store(queryID, h)
}

// UnregisterTaskProgressHandler removes the per-query handler.
func (s *Server) UnregisterTaskProgressHandler(queryID string) {
	if s == nil {
		return
	}
	s.progressHandlers.Delete(queryID)
}

func (s *Server) register(sess *workerSession) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.workers[sess.id]; ok {
		return fmt.Errorf("dataplane: worker %q already connected", sess.id)
	}
	s.workers[sess.id] = sess
	s.order = append(s.order, sess.id)
	return nil
}

func (s *Server) unregister(id string) {
	s.mu.Lock()
	delete(s.workers, id)
	for i, w := range s.order {
		if w == id {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
	s.mu.Unlock()
}

// ConnectedWorkers returns a snapshot of worker IDs currently registered.
// Order is registration order — useful for tests; the scheduler uses
// PickWorker for round-robin dispatch.
func (s *Server) ConnectedWorkers() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, len(s.order))
	copy(out, s.order)
	return out
}

// PickWorker returns the next worker ID via round-robin across the
// currently-connected set. Returns ("", false) when no worker is
// connected. Safe for concurrent callers.
func (s *Server) PickWorker() (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := len(s.order)
	if n == 0 {
		return "", false
	}
	i := s.rrCursor.Add(1) - 1
	return s.order[int(i%uint64(n))], true
}

// SendTaskDispatch pushes a TaskDispatch envelope to the named worker.
// Blocks on HTTP/2 flow control if the worker's recv side is full —
// that's the backpressure path. Returns ErrNotConnected if the worker
// is no longer connected.
//
// task_blob is the result of distributed.Marshal(Task). The scheduler
// owns the marshal; the data-plane layer only carries bytes.
func (s *Server) SendTaskDispatch(workerID, taskID, queryID, stageID string, taskBlob []byte, deadlineUnixNano int64) error {
	s.mu.RLock()
	sess, ok := s.workers[workerID]
	s.mu.RUnlock()
	if !ok {
		return ErrNotConnected
	}
	env := &dpv1.CoordEnvelope{
		Msg: &dpv1.CoordEnvelope_TaskDispatch{
			TaskDispatch: &dpv1.TaskDispatch{
				TaskId:           taskID,
				QueryId:          queryID,
				StageId:          stageID,
				TaskBlob:         taskBlob,
				DeadlineUnixNano: deadlineUnixNano,
			},
		},
	}
	sess.sendMu.Lock()
	defer sess.sendMu.Unlock()
	if sess.stream == nil {
		return ErrNotConnected
	}
	if err := sess.stream.Send(env); err != nil {
		return fmt.Errorf("dataplane: dispatch to %s: %w", workerID, err)
	}
	return nil
}

