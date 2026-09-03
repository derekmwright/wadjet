// Package config provides YAML configuration loading for Wadjet.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the top-level configuration for Wadjet.
type Config struct {
	Mode        string      `yaml:"mode"` // standalone, coordinator, worker
	Storage     Storage     `yaml:"storage"`
	NATS        NATS        `yaml:"nats"`
	HTTP        HTTP        `yaml:"http"`
	GRPC        GRPC        `yaml:"grpc"`
	Worker      Worker      `yaml:"worker"`
	Parquet     Parquet     `yaml:"parquet"`
	Auth        Auth        `yaml:"auth"`
	GeoIP       GeoIP       `yaml:"geoip"`
	Alerts      Alerts      `yaml:"alerts"`       // CREATE ALERT DDL + scheduler
	QueryLimits QueryLimits `yaml:"query_limits"` // global query cost limits
	Query       Query       `yaml:"query"`        // coordinator query lifecycle
	Telemetry   Telemetry   `yaml:"telemetry"`    // OpenTelemetry tracing export
}

// Alerts configures the alert DDL and scheduler.
type Alerts struct {
	Enabled bool `yaml:"enabled"` // enable CREATE ALERT DDL and the scheduler
}

// Query configures the coordinator's query lifecycle. Zero values mean
// "use the built-in default", matching the flags of the same name.
type Query struct {
	// IntermediateTTL is the age after which the coordinator's periodic
	// sweep reclaims a queries/<id>/* prefix the per-query cleanup did not.
	IntermediateTTL time.Duration `yaml:"intermediate_ttl"`
	// IntermediateSweep is how often that sweep runs.
	IntermediateSweep time.Duration `yaml:"intermediate_sweep"`
}

// Telemetry configures OpenTelemetry tracing export.
type Telemetry struct {
	Endpoint   string  `yaml:"endpoint"`    // OTLP gRPC endpoint (e.g., "localhost:4317")
	Insecure   bool    `yaml:"insecure"`    // use plaintext gRPC (no TLS)
	SampleRate float64 `yaml:"sample_rate"` // 0.0-1.0 (default: 1.0 = always)
}

// GeoIP configures MaxMind GeoIP database paths.
type GeoIP struct {
	CityDB string `yaml:"city_db"` // path to GeoLite2-City.mmdb
	ASNDB  string `yaml:"asn_db"`  // path to GeoLite2-ASN.mmdb
}

// QueryLimits configures cost-based query guards. Zero values mean unlimited.
// Per-role limits in Auth.Roles override these global defaults.
type QueryLimits struct {
	MaxScanBytes            int64 `yaml:"max_scan_bytes"`             // max estimated bytes across all scans
	MaxScanRows             int64 `yaml:"max_scan_rows"`              // max estimated rows across all scans
	MaxScanFiles            int   `yaml:"max_scan_files"`             // max files across all scans
	RequireFilterAboveBytes int64 `yaml:"require_filter_above_bytes"` // require WHERE on tables exceeding this size
	RequireLimitAboveRows   int64 `yaml:"require_limit_above_rows"`   // require LIMIT on scans exceeding this row count
}

// Auth configures authentication and authorization.
type Auth struct {
	Enabled      bool         `yaml:"enabled"`
	APIKeys      []AuthAPIKey `yaml:"api_keys"`
	JWT          AuthJWT      `yaml:"jwt"`
	MTLS         AuthMTLS     `yaml:"mtls"`
	Roles        []AuthRole   `yaml:"roles"`
	Policies     []AuthPolicy `yaml:"policies"`      // cell-level access policies (legacy)
	ABACPolicies []ABACPolicy `yaml:"abac_policies"` // ABAC access control policies
}

// AuthAPIKey defines an API key credential.
type AuthAPIKey struct {
	Key  string `yaml:"key"`
	Name string `yaml:"name"`
	Role string `yaml:"role"`
}

// AuthJWT configures JWT authentication.
type AuthJWT struct {
	Enabled       bool   `yaml:"enabled"`
	Secret        string `yaml:"secret"`
	PublicKeyFile string `yaml:"public_key_file"`
	RoleClaim     string `yaml:"role_claim"`
	Issuer        string `yaml:"issuer"`
}

// AuthMTLS configures mutual TLS authentication.
type AuthMTLS struct {
	Enabled     bool              `yaml:"enabled"`
	CAFile      string            `yaml:"ca_file"`
	CertFile    string            `yaml:"cert_file"` // server TLS cert
	KeyFile     string            `yaml:"key_file"`  // server TLS key
	RoleMap     map[string]string `yaml:"role_map"`  // CN/SAN -> role
	DefaultRole string            `yaml:"default_role"`
}

// AuthRole defines a role with table access and permissions.
type AuthRole struct {
	Name        string       `yaml:"name"`
	Tables      []string     `yaml:"tables"`       // table names or "*" for all
	Allow       []string     `yaml:"allow"`        // "read", "write", "admin"
	QueryLimits *QueryLimits `yaml:"query_limits"` // per-role overrides (nil = use global)
}

// AuthPolicy defines a cell-level access policy for a table+role.
type AuthPolicy struct {
	Table     string            `yaml:"table"`
	Role      string            `yaml:"role"`
	Columns   map[string]string `yaml:"columns"`    // column -> "allow", "mask", "deny"
	RowFilter string            `yaml:"row_filter"` // SQL WHERE predicate
}

// ABACPolicy defines an attribute-based access control policy in config.
type ABACPolicy struct {
	Name        string     `yaml:"name"`
	Description string     `yaml:"description"`
	Priority    int        `yaml:"priority"` // lower = evaluated first
	Enabled     *bool      `yaml:"enabled"`  // nil = true
	Rules       []ABACRule `yaml:"rules"`
}

// ABACRule defines a single rule within an ABAC policy.
type ABACRule struct {
	Effect      string           `yaml:"effect"` // "allow" or "deny"
	Conditions  []ABACCondition  `yaml:"conditions"`
	Obligations []ABACObligation `yaml:"obligations"` // only for "allow" rules
}

// ABACCondition defines a condition that must match for the rule to apply.
type ABACCondition struct {
	Attribute string `yaml:"attribute"` // e.g. "subject.role", "resource.name", "env.source_ip"
	Operator  string `yaml:"operator"`  // eq, neq, in, not_in, gt, lt, gte, lte, contains, regex, exists, not_exists
	Value     string `yaml:"value"`     // single value or comma-separated for "in"/"not_in"
}

// ABACObligation defines a side-effect obligation on an allow rule.
type ABACObligation struct {
	Type   string `yaml:"type"`   // row_filter, mask_column, deny_column, query_limit
	Target string `yaml:"target"` // column name or table name
	Value  string `yaml:"value"`  // filter expression, mask function, limit value
}

// Storage configures the object store connection.
type Storage struct {
	Type      string `yaml:"type"`     // "s3" (default) or "file"
	DataDir   string `yaml:"data_dir"` // local directory for type=file
	Endpoint  string `yaml:"endpoint"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
	Bucket    string `yaml:"bucket"`
	UseSSL    bool   `yaml:"use_ssl"`
	Region    string `yaml:"region"`
	// Circuit configures the per-operation-class object-store circuit
	// breaker (ADR-0028).
	Circuit StorageCircuit `yaml:"circuit"`
}

// StorageCircuit configures the object-store circuit breaker. Zero values
// mean "use the built-in default", matching the flags of the same name.
type StorageCircuit struct {
	FailureThreshold int           `yaml:"failure_threshold"` // consecutive failures in one class before the class opens
	ResetTimeout     time.Duration `yaml:"reset_timeout"`     // how long an open breaker stays open
	RequestTimeout   time.Duration `yaml:"request_timeout"`   // per-request timeout for non-streaming operations
}

// NATS configures the embedded NATS server or client connection.
type NATS struct {
	Port        int      `yaml:"port"`
	URL         string   `yaml:"url"`          // for worker mode: coordinator's NATS URL
	StoreDir    string   `yaml:"store_dir"`    // JetStream storage directory
	ClusterID   string   `yaml:"cluster_id"`   // unique cluster identifier (e.g., "central", "afb-east")
	LeafRemotes []string `yaml:"leaf_remotes"` // remote NATS URLs for leaf node connections
	TLSCert     string   `yaml:"tls_cert"`     // TLS certificate file (server or client)
	TLSKey      string   `yaml:"tls_key"`      // TLS private key file
	TLSCA       string   `yaml:"tls_ca"`       // CA certificate for verifying peers (enables mTLS)
}

// HTTP configures the HTTP API server.
type HTTP struct {
	Addr string `yaml:"addr"`
}

// GRPC configures the gRPC API server.
type GRPC struct {
	Addr string `yaml:"addr"`
}

// Worker configures the worker.
type Worker struct {
	MaxConcurrent    int    `yaml:"max_concurrent"`
	CacheBytes       int64  `yaml:"cache_bytes"`
	MemoryBudget     int64  `yaml:"memory_budget"`      // per-task memory budget in bytes (0 = unlimited, no spill)
	SpillDir         string `yaml:"spill_dir"`          // directory for spill files (default: os temp dir)
	ResultStoreBytes int64  `yaml:"result_store_bytes"` // in-memory result store capacity (0 = disabled)
}

// Parquet configures Parquet file writing.
type Parquet struct {
	Compression    string `yaml:"compression"`      // snappy, zstd, gzip, lz4, none
	RowGroupSize   int    `yaml:"row_group_size"`   // rows per row group
	PageBufferSize int    `yaml:"page_buffer_size"` // page size in bytes
}

// DefaultConfig returns a configuration with sensible defaults.
func DefaultConfig() Config {
	return Config{
		Mode: "standalone",
		Storage: Storage{
			Endpoint:  "localhost:9000",
			AccessKey: "minioadmin",
			SecretKey: "minioadmin",
			Bucket:    "wadjet",
		},
		NATS: NATS{
			Port:     4222,
			StoreDir: filepath.Join(os.Getenv("HOME"), ".wadjet", "nats"),
		},
		HTTP: HTTP{
			Addr: ":8080",
		},
		GRPC: GRPC{
			Addr: ":9090",
		},
		Worker: Worker{
			MaxConcurrent: 4,
			CacheBytes:    256 * 1024 * 1024,
		},
		Parquet: Parquet{
			Compression:    "snappy",
			RowGroupSize:   128 * 1024,
			PageBufferSize: 256 * 1024,
		},
	}
}

// Load reads a YAML config file and merges with defaults.
//
// The decode is STRICT: a key the schema does not define is an error naming
// it. Before the precedence loader a mistyped `storage:` key was inert
// anyway, because the whole section was; now the difference between
// `bucket:` and `buckett:` is the difference between reading the right
// bucket and the wrong one, with nothing said at startup. PostgreSQL
// refuses an unrecognised parameter in postgresql.conf for the same reason,
// and ADR-0029 already takes its precedence order.
//
// The cost is forward compatibility: an older binary refuses a file written
// for a newer one. That is the trade this repo wants — a silently ignored
// key is a silently wrong deployment, which is the whole of #808.
func Load(path string) (*Config, error) {
	cfg := DefaultConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	if err := strictUnmarshal(data, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// strictUnmarshal decodes YAML into out, refusing any key the schema does
// not define. An empty document is not an error.
func strictUnmarshal(data []byte, out *Config) error {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(out); err != nil {
		if errors.Is(err, io.EOF) {
			return nil // an empty file is a valid empty configuration
		}
		return fmt.Errorf("parsing config file: %w", err)
	}
	return nil
}

// LoadOrDefault loads a config file if it exists, otherwise returns defaults.
// Environment variables with WADJET_ prefix always override file/default values.
func LoadOrDefault(path string) *Config {
	var cfg *Config
	if path == "" {
		def := DefaultConfig()
		cfg = &def
	} else {
		var err error
		cfg, err = Load(path)
		if err != nil {
			def := DefaultConfig()
			cfg = &def
		}
	}
	applyEnvOverrides(cfg)
	return cfg
}

// applyEnvOverrides reads WADJET_* environment variables and overrides
// config values. It is the environment TIER of the resolver (resolve.go) run
// on its own, for callers that have no command line.
//
// The variable set is the configuration registry's (registry.go), so this
// function, the resolver, the admin endpoint's effective-value report and
// the list below cannot disagree about which variables exist.
// TestEnvironmentVariableNamesAgreeEverywhere asserts that the list below
// and docs/configuration.md's tables name exactly this set.
//
// Supported variables:
//
//	WADJET_MODE                             - standalone, coordinator, worker
//	WADJET_STORAGE_TYPE                     - s3, file
//	WADJET_STORAGE_ENDPOINT                 - S3/MinIO endpoint
//	WADJET_STORAGE_ACCESS_KEY               - S3 access key
//	WADJET_STORAGE_SECRET_KEY               - S3 secret key
//	WADJET_STORAGE_BUCKET                   - S3 bucket name
//	WADJET_STORAGE_USE_SSL                  - true/false
//	WADJET_STORAGE_REGION                   - S3 region
//	WADJET_STORAGE_CIRCUIT_THRESHOLD        - consecutive failures before a breaker class opens
//	WADJET_STORAGE_CIRCUIT_RESET            - how long an open breaker stays open (duration)
//	WADJET_STORAGE_CIRCUIT_REQUEST_TIMEOUT  - per-request object-store timeout (duration)
//	WADJET_NATS_PORT                        - NATS listen port
//	WADJET_NATS_URL                         - NATS URL (worker mode)
//	WADJET_NATS_CLUSTER_ID                  - cluster identifier
//	WADJET_NATS_LEAF_REMOTES                - comma-separated remote NATS URLs
//	WADJET_NATS_TLS_CERT                    - NATS TLS certificate file
//	WADJET_NATS_TLS_KEY                     - NATS TLS private key file
//	WADJET_NATS_TLS_CA                      - NATS TLS CA file (enables mTLS)
//	WADJET_HTTP_ADDR                        - HTTP listen address
//	WADJET_GRPC_ADDR                        - gRPC listen address
//	WADJET_WORKER_MAX_CONCURRENT            - max concurrent tasks
//	WADJET_WORKER_MEMORY_BUDGET             - per-task memory budget (bytes)
//	WADJET_WORKER_SPILL_DIR                 - spill directory
//	WADJET_ENABLE_ALERTS                    - true/false (CREATE ALERT DDL and scheduler)
//	WADJET_GEOIP_CITY_DB                    - GeoLite2-City.mmdb path
//	WADJET_GEOIP_ASN_DB                     - GeoLite2-ASN.mmdb path
//	WADJET_QUERY_INTERMEDIATE_TTL           - queries/<id>/ reclaim age (duration)
//	WADJET_QUERY_INTERMEDIATE_SWEEP         - queries/ sweep interval (duration)
//	WADJET_QUERY_MAX_SCAN_BYTES             - max estimated scan bytes per query
//	WADJET_QUERY_MAX_SCAN_ROWS              - max estimated scan rows per query
//	WADJET_QUERY_MAX_SCAN_FILES             - max scan files per query
//	WADJET_OTEL_ENDPOINT                    - OTLP gRPC endpoint (e.g. localhost:4317)
//	WADJET_OTEL_INSECURE                    - true/false (plaintext gRPC)
//	WADJET_OTEL_SAMPLE_RATE                 - 0.0-1.0 sampling rate
func applyEnvOverrides(cfg *Config) {
	for _, k := range keys {
		if v, ok := envValue(k, os.LookupEnv); ok {
			k.Set(cfg, v)
		}
	}
}

// EffectiveQueryLimits extracts the cost guard from a loaded config: the
// global limits, and the per-role map a planner resolves against for the
// identity answering a query.
//
// EVERY configured role gets an entry, nil meaning unlimited — a role that
// declares no `query_limits` OVERRIDES the global limits rather than
// inheriting them, which is what docs/security.md's "Per-Role Limits" section
// says and how its `admin` example is meant to read. A role nobody configured
// (and an unauthenticated request) falls back to the global limits.
//
// Returns (nil, nil) for a config with neither, so an unconfigured deployment
// stays unlimited and pays nothing.
func (c *Config) EffectiveQueryLimits() (*QueryLimits, map[string]*QueryLimits) {
	if c == nil {
		return nil, nil
	}
	var global *QueryLimits
	if c.QueryLimits != (QueryLimits{}) {
		g := c.QueryLimits
		global = &g
	}
	if len(c.Auth.Roles) == 0 {
		return global, nil
	}
	perRole := make(map[string]*QueryLimits, len(c.Auth.Roles))
	for _, r := range c.Auth.Roles {
		if r.QueryLimits == nil {
			perRole[r.Name] = nil
			continue
		}
		lim := *r.QueryLimits
		perRole[r.Name] = &lim
	}
	return global, perRole
}
