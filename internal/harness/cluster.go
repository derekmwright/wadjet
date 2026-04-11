package harness

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
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

// Cluster is a process supervisor for one coordinator + N workers running
// against an embedded NATS started in this process. Shutdown is idempotent.
type Cluster struct {
	cfg ClusterConfig

	mu           sync.Mutex
	embeddedNATS *distributed.EmbeddedNATS
	natsURL      string
	coord        *managedProcess
	workers      []*managedProcess
	shutdownOnce sync.Once
	shutdownErr  error
}

type managedProcess struct {
	role    string // "coord" or "worker-N"
	cmd     *exec.Cmd
	logFile *os.File
	exitedC chan struct{} // closed when the process exits
	exitErr error
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

// StartNATS brings up the embedded NATS server and JetStream streams.
// Call this first, then seed data into the catalog, then call StartProcesses.
func (c *Cluster) StartNATS(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := os.MkdirAll(filepath.Join(c.cfg.RunDir, "logs"), 0755); err != nil {
		return fmt.Errorf("creating logs dir: %w", err)
	}

	natsCfg := distributed.DefaultNATSConfig()
	natsCfg.Port = -1 // random free port
	natsCfg.StoreDir = filepath.Join(c.cfg.RunDir, "nats")
	embedded, err := distributed.NewEmbeddedNATS(natsCfg, c.cfg.Logger)
	if err != nil {
		return fmt.Errorf("starting embedded NATS: %w", err)
	}
	c.embeddedNATS = embedded
	c.natsURL = embedded.ClientURL()
	c.cfg.Logger.Info("embedded NATS up", "url", c.natsURL)

	// Set up JetStream streams so the catalog KV and task queues are ready.
	nc, err := distributed.ConnectInProcess(c.embeddedNATS.Server())
	if err != nil {
		return fmt.Errorf("connecting in-process for stream setup: %w", err)
	}
	defer nc.Close()

	js, err := distributed.NewJetStream(nc)
	if err != nil {
		return fmt.Errorf("creating JetStream: %w", err)
	}
	if err := distributed.SetupStreams(ctx, js); err != nil {
		return fmt.Errorf("setting up streams: %w", err)
	}

	return nil
}

// NATSURL returns the embedded NATS client URL. Only valid after StartNATS.
func (c *Cluster) NATSURL() string {
	return c.natsURL
}

// EmbeddedNATS returns the embedded NATS server. Only valid after StartNATS.
func (c *Cluster) EmbeddedNATS() *distributed.EmbeddedNATS {
	return c.embeddedNATS
}

// StartProcesses spawns the coordinator and workers, and blocks until
// all workers have registered or until ctx is cancelled.
// Must be called after StartNATS and after seeding the catalog.
func (c *Cluster) StartProcesses(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Spawn coordinator.
	coordArgs := []string{
		"serve",
		"--mode=coordinator",
		"--nats-url=" + c.natsURL,
		"--pg-addr=" + c.cfg.PgAddr,
		"--spill-dir=" + filepath.Join(c.cfg.RunDir, "spill", "coord"),
	}
	if c.cfg.DataDir != "" {
		coordArgs = append(coordArgs, "--storage-type=file", "--data-dir="+c.cfg.DataDir)
	}
	coord, err := c.spawn("coord", coordArgs)
	if err != nil {
		return fmt.Errorf("spawning coordinator: %w", err)
	}
	c.coord = coord

	// Spawn workers.
	for i := 0; i < c.cfg.NumWorkers; i++ {
		role := fmt.Sprintf("worker-%d", i)
		workerArgs := []string{
			"serve",
			"--mode=worker",
			"--nats-url=" + c.natsURL,
			"--spill-dir=" + filepath.Join(c.cfg.RunDir, "spill", role),
		}
		if c.cfg.DataDir != "" {
			workerArgs = append(workerArgs, "--storage-type=file", "--data-dir="+c.cfg.DataDir)
		}
		w, err := c.spawn(role, workerArgs)
		if err != nil {
			return fmt.Errorf("spawning %s: %w", role, err)
		}
		c.workers = append(c.workers, w)
	}

	// Health check: poll worker registry until NumWorkers report in.
	if err := c.waitWorkersReady(ctx, 30*time.Second); err != nil {
		return fmt.Errorf("workers not ready: %w", err)
	}

	c.cfg.Logger.Info("cluster ready", "workers", c.cfg.NumWorkers)
	return nil
}

// Start is a convenience that calls StartNATS + StartProcesses. Use the
// two-phase API (StartNATS, seed data, StartProcesses) when you need to
// populate the catalog before the coordinator boots.
func (c *Cluster) Start(ctx context.Context) error {
	if err := c.StartNATS(ctx); err != nil {
		return err
	}
	return c.StartProcesses(ctx)
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
	nc, err := distributed.ConnectInProcess(c.embeddedNATS.Server())
	if err != nil {
		return fmt.Errorf("connecting in-process: %w", err)
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

	if c.embeddedNATS != nil {
		c.embeddedNATS.Shutdown()
		c.embeddedNATS = nil
	}

	if err := checkNoOrphanedWadjet(); err != nil {
		return fmt.Errorf("post-shutdown orphan check: %w", err)
	}
	return nil
}
