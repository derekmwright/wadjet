package harness

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/nats-io/nats.go"
)

// ClusterConfig describes a local-mode cluster to spawn.
type ClusterConfig struct {
	WadjetBin  string // path to wadjet binary
	RunDir     string // /tmp/wadjet-harness/run-X
	NumWorkers int
	GoMemLimit int64
	PgAddr     string // pgwire listen address for coordinator (default ":15433")
	DataDir    string // local data dir (FileStore) for storage-type=file
	Logger     *slog.Logger
}

// Cluster is a process supervisor for one coordinator + N workers.
// The coordinator owns the embedded NATS server; the harness connects
// to it via TCP to seed catalog data and subscribe to heartbeats.
type Cluster struct {
	cfg ClusterConfig

	mu           sync.Mutex
	natsPort     int
	natsURL      string
	httpPort     int
	grpcPort     int
	coord        *managedProcess
	workers      []*managedProcess
	shutdownOnce sync.Once
	shutdownErr  error
}

type managedProcess struct {
	role      string // "coord" or "worker-N"
	cmd       *exec.Cmd
	logFile   *os.File
	exitedC   chan struct{} // closed when the process exits
	exitErr   error
	debugPort int // HTTP metrics/pprof port; 0 if not assigned
}

// NewCluster constructs a Cluster but does not start anything.
func NewCluster(cfg ClusterConfig) *Cluster {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if cfg.PgAddr == "" {
		cfg.PgAddr = ":15433"
	}
	return &Cluster{cfg: cfg}
}

// PgAddr returns the pgwire address of the coordinator.
func (c *Cluster) PgAddr() string {
	return c.cfg.PgAddr
}

// DebugPorts returns a map of role → HTTP port for pprof access.
// Only valid after StartCoordinator and StartWorkers have been called.
func (c *Cluster) DebugPorts() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	ports := make(map[string]int, 1+len(c.workers))
	if c.httpPort > 0 {
		ports["coord"] = c.httpPort
	}
	for _, w := range c.workers {
		if w.debugPort > 0 {
			ports[w.role] = w.debugPort
		}
	}
	return ports
}

// StartCoordinator spawns the coordinator process (which owns the embedded
// NATS server) and waits until NATS is accepting connections. Call this
// first, then seed data via ConnectNATS, then call StartWorkers.
func (c *Cluster) StartCoordinator(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := os.MkdirAll(filepath.Join(c.cfg.RunDir, "logs"), 0755); err != nil {
		return fmt.Errorf("creating logs dir: %w", err)
	}

	// Pick random free ports for NATS, HTTP, and gRPC to avoid conflicts.
	c.natsPort = freePort()
	c.httpPort = freePort()
	c.grpcPort = freePort()
	c.natsURL = fmt.Sprintf("nats://127.0.0.1:%d", c.natsPort)

	// Use standalone mode: it starts pgwire + embedded NATS + 1 internal
	// worker. External workers join via --nats-url in StartWorkers.
	coordArgs := []string{
		"serve",
		"--mode=standalone",
		"--nats-port=" + strconv.Itoa(c.natsPort),
		"--pg-addr=" + c.cfg.PgAddr,
		"--http-addr=:" + strconv.Itoa(c.httpPort),
		"--grpc-addr=:" + strconv.Itoa(c.grpcPort),
		"--spill-dir=" + filepath.Join(c.cfg.RunDir, "spill", "coord"),
		"--nats-store-dir=" + filepath.Join(c.cfg.RunDir, "nats"),
	}
	if c.cfg.DataDir != "" {
		coordArgs = append(coordArgs, "--storage-type=file", "--data-dir="+c.cfg.DataDir)
	}
	coord, err := c.spawn("coord", coordArgs)
	if err != nil {
		return fmt.Errorf("spawning coordinator: %w", err)
	}
	c.coord = coord

	// Wait for NATS and pgwire to accept connections.
	if err := c.waitNATSReady(ctx, 15*time.Second); err != nil {
		return fmt.Errorf("coordinator NATS not ready: %w", err)
	}
	if err := c.waitPortReady(ctx, c.cfg.PgAddr, 15*time.Second); err != nil {
		return fmt.Errorf("coordinator pgwire not ready: %w", err)
	}

	c.cfg.Logger.Info("coordinator up", "nats", c.natsURL, "pg", c.cfg.PgAddr)
	return nil
}

// NATSURL returns the coordinator's NATS URL. Only valid after StartCoordinator.
func (c *Cluster) NATSURL() string {
	return c.natsURL
}

// ConnectNATS returns a NATS connection to the coordinator's embedded NATS.
func (c *Cluster) ConnectNATS() (*nats.Conn, error) {
	return distributed.Connect(c.natsURL, nil)
}

// StartWorkers spawns worker processes and waits for them to register.
// Must be called after StartCoordinator and after seeding the catalog.
func (c *Cluster) StartWorkers(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := 0; i < c.cfg.NumWorkers; i++ {
		role := fmt.Sprintf("worker-%d", i)
		metricsPort := freePort()
		workerArgs := []string{
			"serve",
			"--mode=worker",
			"--nats-url=" + c.natsURL,
			"--spill-dir=" + filepath.Join(c.cfg.RunDir, "spill", role),
			"--metrics-addr=:" + strconv.Itoa(metricsPort),
		}
		if c.cfg.DataDir != "" {
			workerArgs = append(workerArgs, "--storage-type=file", "--data-dir="+c.cfg.DataDir)
		}
		w, err := c.spawn(role, workerArgs)
		if err != nil {
			return fmt.Errorf("spawning %s: %w", role, err)
		}
		w.debugPort = metricsPort
		c.workers = append(c.workers, w)
	}

	if err := c.waitWorkersReady(ctx, 30*time.Second); err != nil {
		return fmt.Errorf("workers not ready: %w", err)
	}

	c.cfg.Logger.Info("cluster ready", "workers", c.cfg.NumWorkers)
	return nil
}

// Start is a convenience that calls StartCoordinator + StartWorkers
// without seeding data in between.
func (c *Cluster) Start(ctx context.Context) error {
	if err := c.StartCoordinator(ctx); err != nil {
		return err
	}
	return c.StartWorkers(ctx)
}

// waitNATSReady polls until the coordinator's NATS port accepts a connection.
func (c *Cluster) waitNATSReady(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	addr := fmt.Sprintf("127.0.0.1:%d", c.natsPort)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return nil
		}
		// Check if coordinator crashed.
		select {
		case <-c.coord.exitedC:
			return fmt.Errorf("coordinator exited before NATS was ready: %v", c.coord.exitErr)
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	return fmt.Errorf("NATS not ready at %s after %s", addr, timeout)
}

// waitPortReady polls until the given address accepts a TCP connection.
func (c *Cluster) waitPortReady(ctx context.Context, addr string, timeout time.Duration) error {
	// Normalize ":15433" to "127.0.0.1:15433".
	if addr[0] == ':' {
		addr = "127.0.0.1" + addr
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return nil
		}
		select {
		case <-c.coord.exitedC:
			return fmt.Errorf("coordinator exited before port %s was ready: %v", addr, c.coord.exitErr)
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	return fmt.Errorf("port %s not ready after %s", addr, timeout)
}

func (c *Cluster) spawn(role string, args []string) (*managedProcess, error) {
	logPath := filepath.Join(c.cfg.RunDir, "logs", role+".log")
	logFile, err := os.Create(logPath)
	if err != nil {
		return nil, err
	}

	cmd := exec.Command(c.cfg.WadjetBin, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("GOMEMLIMIT=%d", c.cfg.GoMemLimit),
	)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true, // own process group for clean shutdown
	}

	if err := cmd.Start(); err != nil {
		logFile.Close()
		return nil, err
	}

	mp := &managedProcess{
		role:    role,
		cmd:     cmd,
		logFile: logFile,
		exitedC: make(chan struct{}),
	}
	go func() {
		mp.exitErr = cmd.Wait()
		close(mp.exitedC)
	}()
	return mp, nil
}

func (c *Cluster) waitWorkersReady(ctx context.Context, timeout time.Duration) error {
	nc, err := distributed.Connect(c.natsURL, nil)
	if err != nil {
		return fmt.Errorf("connecting to NATS for worker check: %w", err)
	}
	defer nc.Close()

	seen := make(map[string]bool)
	var mu sync.Mutex
	sub, err := nc.Subscribe(distributed.SubjectHeartbeat, func(msg *nats.Msg) {
		var hb distributed.WorkerHeartbeat
		if err := distributed.Unmarshal(msg.Data, &hb); err == nil {
			mu.Lock()
			seen[hb.WorkerID] = true
			mu.Unlock()
		}
	})
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(seen)
		mu.Unlock()
		if n >= c.cfg.NumWorkers {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	mu.Lock()
	n := len(seen)
	mu.Unlock()
	return fmt.Errorf("only %d/%d workers registered after %s", n, c.cfg.NumWorkers, timeout)
}

// Shutdown stops all child processes and the embedded NATS. Idempotent.
func (c *Cluster) Shutdown(ctx context.Context) error {
	c.shutdownOnce.Do(func() {
		c.shutdownErr = c.shutdown(ctx)
	})
	return c.shutdownErr
}

func (c *Cluster) shutdown(_ context.Context) error {
	c.mu.Lock()
	procs := append([]*managedProcess{}, c.workers...)
	if c.coord != nil {
		procs = append(procs, c.coord)
	}
	c.mu.Unlock()

	// SIGTERM all process groups.
	for _, p := range procs {
		_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGTERM)
	}

	// Wait up to 5s, then SIGKILL.
	deadline := time.After(5 * time.Second)
	for _, p := range procs {
		select {
		case <-p.exitedC:
		case <-deadline:
			_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
			<-p.exitedC
		}
		if p.logFile != nil {
			p.logFile.Close()
		}
	}

	if err := checkNoOrphanedWadjet(); err != nil {
		return fmt.Errorf("post-shutdown orphan check: %w", err)
	}
	return nil
}

// freePort asks the kernel for an available TCP port.
func freePort() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}
