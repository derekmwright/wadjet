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

	// S3 source (Source=="s3" only)
	Source     string // "local" (default) or "s3"
	Bucket     string
	Region     string
	Endpoint   string
	SSL        bool
	DataPrefix string // prefix under Bucket containing table data (e.g. "tables/")

	// LegacyMode: pass --legacy-mode to the spawned coordinator. Native-DAG
	// is the default execution path; legacy is the opt-in fallback for
	// queries native-DAG can't yet handle.
	LegacyMode bool
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
	GoroutinePeak int       `json:"goroutine_peak"`
	Hung          bool      `json:"hung"`
	HangDumpPath  string    `json:"hang_dump_path,omitempty"`
	StartedAt     time.Time `json:"started_at"`
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
