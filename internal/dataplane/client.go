package dataplane

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	dpv1 "github.com/citc-tech/wadjet/gen/dataplane/v1"
)

// ErrNotConnected is returned by SendResultBatch (and future send
// methods) when the data-plane stream is not currently usable. Callers
// can choose to fall back to NATS or treat it as fatal.
var ErrNotConnected = errors.New("dataplane: not connected")

// Client is the worker-side data-plane gRPC connection. It maintains a
// single long-lived bidi stream to coord, reconnecting on disconnect.
//
// Phase A: open the stream, exchange Hello/Welcome, hold open.
// Phase B: send ResultBatch messages via the stream's send side.
// Phases C–E add task dispatch consumption.
type Client struct {
	cfg    ClientConfig
	logger *slog.Logger

	cancel func()
	doneCh chan struct{}

	connected atomic.Bool
	cluster   atomic.Value // string

	// currentStream holds the active stream's send handle. nil when
	// not connected. streamMu protects the assignment; sendMu serializes
	// stream.Send calls (gRPC client-side Send is NOT safe for
	// concurrent use).
	streamMu      sync.RWMutex
	currentStream dpv1.DataPlane_ConnectClient
	sendMu        sync.Mutex
}

// NewClient constructs a Client. Call Start to dial.
func NewClient(cfg ClientConfig, logger *slog.Logger) *Client {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.ReconnectBackoff == 0 {
		cfg.ReconnectBackoff = 500 * time.Millisecond
	}
	return &Client{
		cfg:    cfg,
		logger: logger.With("component", "dataplane.client"),
		doneCh: make(chan struct{}),
	}
}

// Start begins the reconnect loop. Returns immediately; the loop runs
// in the background until Stop is called.
func (c *Client) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	go c.run(ctx)
}

// Stop cancels the reconnect loop and waits for it to exit.
func (c *Client) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
	<-c.doneCh
}

// Connected reports whether the most recent connect attempt produced a
// live stream that has received Welcome.
func (c *Client) Connected() bool { return c.connected.Load() }

// ClusterID returns the coord-reported cluster identity, or "" if not
// yet connected.
func (c *Client) ClusterID() string {
	if v := c.cluster.Load(); v != nil {
		return v.(string)
	}
	return ""
}

func (c *Client) run(ctx context.Context) {
	defer close(c.doneCh)

	backoff := c.cfg.ReconnectBackoff
	const maxBackoff = 10 * time.Second

	for {
		if err := ctx.Err(); err != nil {
			return
		}

		err := c.connectOnce(ctx)
		c.connected.Store(false)

		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.logger.Warn("dataplane connection ended", "err", err, "backoff", backoff)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

func (c *Client) connectOnce(ctx context.Context) error {
	var dialOpts []grpc.DialOption
	if c.cfg.TLSConfig != nil {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(c.cfg.TLSConfig)))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.NewClient(c.cfg.CoordAddr, dialOpts...)
	if err != nil {
		return fmt.Errorf("dataplane: dial %s: %w", c.cfg.CoordAddr, err)
	}
	defer conn.Close()

	client := dpv1.NewDataPlaneClient(conn)
	stream, err := client.Connect(ctx)
	if err != nil {
		return fmt.Errorf("dataplane: open stream: %w", err)
	}

	hello := &dpv1.WorkerEnvelope{
		Msg: &dpv1.WorkerEnvelope_Hello{
			Hello: &dpv1.Hello{
				WorkerId:   c.cfg.WorkerID,
				BuildSha:   c.cfg.BuildSHA,
				NumCpus:    int32(runtime.NumCPU()),
				Gomemlimit: gomemlimit(),
			},
		},
	}
	if err := stream.Send(hello); err != nil {
		return fmt.Errorf("dataplane: send Hello: %w", err)
	}

	first, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("dataplane: recv Welcome: %w", err)
	}
	welcome := first.GetWelcome()
	if welcome == nil {
		return fmt.Errorf("dataplane: first server message must be Welcome, got %T", first.GetMsg())
	}

	c.cluster.Store(welcome.GetClusterId())
	c.streamMu.Lock()
	c.currentStream = stream
	c.streamMu.Unlock()
	c.connected.Store(true)
	defer func() {
		c.streamMu.Lock()
		c.currentStream = nil
		c.streamMu.Unlock()
	}()
	c.logger.Info("dataplane connected",
		"coord_addr", c.cfg.CoordAddr,
		"cluster_id", welcome.GetClusterId(),
		"server_time", time.Unix(0, welcome.GetServerTime()))

	// Phase B: SendResultBatch uses currentStream directly under
	// streamMu. The recv loop here is currently a drain — Phases C–E
	// will route TaskDispatch back into the worker scheduler.
	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}
		_ = msg
	}
}

// SendResultBatch sends a result chunk over the data-plane stream.
// Returns ErrNotConnected if the stream is not currently usable.
// Callers can choose to fall back to NATS or surface the error.
// Safe for concurrent callers — gRPC stream.Send is serialized
// internally via sendMu.
func (c *Client) SendResultBatch(rb ResultBatch) error {
	c.streamMu.RLock()
	stream := c.currentStream
	c.streamMu.RUnlock()
	if stream == nil {
		return ErrNotConnected
	}
	env := &dpv1.WorkerEnvelope{
		Msg: &dpv1.WorkerEnvelope_ResultBatch{
			ResultBatch: &dpv1.ResultBatch{
				QueryId:  rb.QueryID,
				WorkerId: rb.WorkerID,
				Terminal: rb.Terminal,
				RowCount: rb.RowCount,
				Payload:  rb.Payload,
				Err:      rb.Err,
			},
		},
	}
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	if err := stream.Send(env); err != nil {
		return fmt.Errorf("dataplane: send result: %w", err)
	}
	return nil
}

// gomemlimit returns the current GOMEMLIMIT in bytes, or 0 if unset.
// Reading the runtime value avoids importing debug.SetMemoryLimit
// indirection; the actual mechanism on workers is the env var.
func gomemlimit() int64 {
	// Phase A: stub; Phase B can plumb the actual value from the worker's
	// memory.Tracker budget so the coord scheduler sees real capacity.
	return 0
}
