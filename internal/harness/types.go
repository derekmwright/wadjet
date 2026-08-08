package harness

import (
	"time"
)

// Mode is the harness run mode.
type Mode string

const (
	ModeLocal  Mode = "local"
	ModeGolden Mode = "golden"
)

// Slice identifies a local-mode data slice configuration.
type Slice string

const (
	SliceSmall Slice = "small"
	SliceLarge Slice = "large"
)

// Exit codes returned by cmd/tpch-harness. Higher numbers outrank lower
// when multiple failures occur in one run.
const (
	ExitOK          = 0
	ExitRegression  = 1 // perf regression, missing spill paths, or hang
	ExitSetup       = 2 // setup error, cluster crash, internal harness bug
	ExitCorrectness = 3 // row count or checksum diverged
)

// Config is the parsed flag set passed into Run.
type Config struct {
	Mode           Mode
	Slice          Slice  // local only
	CoordURL       string // golden only
	DataDir        string // local only; default /tmp/sf100-sample
	BaselinePath   string
	OutPath        string
	Queries        []string // empty means all 22 + micros
	UpdateBaseline bool
	NoCompare      bool
	WadjetBin      string // path to wadjet binary; auto-built if empty
	PgAddr         string // local only; override for coordinator pgwire listen addr
	NumWorkers     int    // local only; cluster size to spawn (0 = default of 2)

	// Runs repeats the whole query suite against the same live cluster
	// (0/1 = once). The EC2 SF100 protocol is benchmark_runs=2 — run 1
	// populates the NVMe base-table cache cold, run 2 measures the
	// steady regime over it (docs/benchmarks/
	// steady-slower-than-cold-2026-08-08.md); this is the local mirror.
	// Baseline comparison applies to run 1 only; later runs are recorded
	// with QueryMeasurement.Run set, plus a cross-run row-count/value-sig
	// identity check.
	Runs int

	// ScaleFactor is the TPC-H data volume for local mode generation.
	// 0 (zero value) defaults to 0.01 (~10 MB total, lineitem 60K rows).
	// Larger values let the harness exercise per-stage round-trip and
	// memory-pressure paths at meaningful scale without EC2 spend:
	//   0.1  → ~150 MB total, lineitem 600K rows  (~5-10 sec total benchmark)
	//   1.0  → ~1.5 GB total, lineitem 6M rows    (~30-60 sec)
	//   10.0 → ~15 GB total — local generation will take minutes; only useful
	//          on machines with disk + memory to spare.
	ScaleFactor float64 // 0 = SF0.01 default

	// S3 source (Source=="s3" only)
	Source     string // "local" (default) or "s3"
	Bucket     string
	Region     string
	Endpoint   string
	SSL        bool
	DataPrefix string // prefix under Bucket containing table data (e.g. "tables/")

	// DataPlane selects worker↔coord transport for the spawned cluster.
	// Empty / "nats" uses the legacy NATS reply-subject path. "grpc"
	// enables the data-plane gRPC stream (Phase B+).
	DataPlane string

	// ExtraServeArgs are appended verbatim to every spawned `wadjet serve`
	// command (coordinator and workers). Use to exercise deploy-gated
	// flags (e.g. --bounded-dirty-writes, --mmap-relief) through the
	// local harness before an EC2 run.
	ExtraServeArgs []string

	// SpawnWrapper is prepended to every spawned process argv (see
	// ClusterConfig.SpawnWrapper). E.g. a docker memory-cap wrapper for
	// edge-box simulation.
	SpawnWrapper []string
}

// QueryMeasurement is the result of running one query (or micro).
type QueryMeasurement struct {
	Query         string    `json:"query"`
	WallMs        int64     `json:"wall_ms"`
	PeakHeapMB    int64     `json:"peak_heap_mb"`
	AllocCount    int64     `json:"alloc_count"`
	SpillBytes    int64     `json:"spill_bytes"`
	RowCount      int64     `json:"row_count"`
	RowChecksum   string    `json:"row_checksum"`
	ValueSig      string    `json:"value_sig,omitempty"`
	GoroutinePeak int       `json:"goroutine_peak"`
	Hung          bool      `json:"hung"`
	HangDumpPath  string    `json:"hang_dump_path,omitempty"`
	StartedAt     time.Time `json:"started_at"`
	// Run numbers the suite pass this measurement came from under
	// Config.Runs > 1 (0/absent = single-pass run or run 1).
	Run int `json:"run,omitempty"`
}

// QueryDelta records a single per-metric drift between projected and baseline.
type QueryDelta struct {
	Query        string  `json:"query"`
	Metric       string  `json:"metric"`
	Baseline     float64 `json:"baseline"`
	Projected    float64 `json:"projected"`
	DriftPct     float64 `json:"drift_pct"`
	TolerancePct float64 `json:"tolerance_pct"`
	Status       string  `json:"status"` // "PASS", "REGRESS"
	// Detail carries non-scalar divergence context (e.g. which value-
	// signature column diverged and by how much).
	Detail string `json:"detail,omitempty"`
}

// RunResult is the top-level structured output written to the result JSON.
type RunResult struct {
	Mode         Mode               `json:"mode"`
	Slice        Slice              `json:"slice,omitempty"`
	StartedAt    time.Time          `json:"started_at"`
	DurationMs   int64              `json:"duration_ms"`
	Queries      []QueryMeasurement `json:"queries"`
	BaselinePath string             `json:"baseline_path"`
	Regressions  []QueryDelta       `json:"regressions"`
	Hangs        []string           `json:"hangs"`
	Passed       bool               `json:"passed"`
	ExitCode     int                `json:"exit_code"`
}
