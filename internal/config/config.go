// Package config provides YAML configuration loading for Wadjet.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

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
	QueryLimits QueryLimits `yaml:"query_limits"` // global query cost limits
	Telemetry   Telemetry   `yaml:"telemetry"`    // OpenTelemetry tracing export
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
func Load(path string) (*Config, error) {
	cfg := DefaultConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}

	return &cfg, nil
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

// applyEnvOverrides reads WADJET_* environment variables and overrides config values.
// Supported variables:
//
//	WADJET_MODE                   - standalone, coordinator, worker
//	WADJET_HTTP_ADDR              - HTTP listen address
//	WADJET_GRPC_ADDR              - gRPC listen address
//	WADJET_STORAGE_TYPE           - s3, file
//	WADJET_STORAGE_ENDPOINT       - S3/MinIO endpoint
//	WADJET_STORAGE_ACCESS_KEY     - S3 access key
//	WADJET_STORAGE_SECRET_KEY     - S3 secret key
//	WADJET_STORAGE_BUCKET         - S3 bucket name
//	WADJET_STORAGE_REGION         - S3 region
//	WADJET_STORAGE_USE_SSL        - true/false
//	WADJET_NATS_PORT              - NATS listen port
//	WADJET_NATS_URL               - NATS URL (worker mode)
//	WADJET_NATS_CLUSTER_ID        - cluster identifier
//	WADJET_NATS_LEAF_REMOTES      - comma-separated remote NATS URLs
//	WADJET_NATS_TLS_CERT          - NATS TLS certificate file
//	WADJET_NATS_TLS_KEY           - NATS TLS private key file
//	WADJET_NATS_TLS_CA            - NATS TLS CA file (enables mTLS)
//	WADJET_WORKER_MAX_CONCURRENT  - max concurrent tasks
//	WADJET_WORKER_MEMORY_BUDGET   - per-task memory budget (bytes)
//	WADJET_WORKER_SPILL_DIR       - spill directory
//	WADJET_GEOIP_CITY_DB          - GeoLite2-City.mmdb path
//	WADJET_GEOIP_ASN_DB           - GeoLite2-ASN.mmdb path
//	WADJET_OTEL_ENDPOINT          - OTLP gRPC endpoint (e.g. localhost:4317)
//	WADJET_OTEL_INSECURE          - true/false (plaintext gRPC)
//	WADJET_OTEL_SAMPLE_RATE       - 0.0-1.0 sampling rate
func applyEnvOverrides(cfg *Config) {
	if v := os.Getenv("WADJET_MODE"); v != "" {
		cfg.Mode = v
	}
	if v := os.Getenv("WADJET_HTTP_ADDR"); v != "" {
		cfg.HTTP.Addr = v
	}
	if v := os.Getenv("WADJET_GRPC_ADDR"); v != "" {
		cfg.GRPC.Addr = v
	}
	if v := os.Getenv("WADJET_STORAGE_TYPE"); v != "" {
		cfg.Storage.Type = v
	}
	if v := os.Getenv("WADJET_STORAGE_ENDPOINT"); v != "" {
		cfg.Storage.Endpoint = v
	}
	if v := os.Getenv("WADJET_STORAGE_ACCESS_KEY"); v != "" {
		cfg.Storage.AccessKey = v
	}
	if v := os.Getenv("WADJET_STORAGE_SECRET_KEY"); v != "" {
		cfg.Storage.SecretKey = v
	}
	if v := os.Getenv("WADJET_STORAGE_BUCKET"); v != "" {
		cfg.Storage.Bucket = v
	}
	if v := os.Getenv("WADJET_STORAGE_REGION"); v != "" {
		cfg.Storage.Region = v
	}
	if v := os.Getenv("WADJET_STORAGE_USE_SSL"); v != "" {
		cfg.Storage.UseSSL = strings.EqualFold(v, "true") || v == "1"
	}
	if v := os.Getenv("WADJET_NATS_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.NATS.Port = n
		}
	}
	if v := os.Getenv("WADJET_NATS_URL"); v != "" {
		cfg.NATS.URL = v
	}
	if v := os.Getenv("WADJET_NATS_CLUSTER_ID"); v != "" {
		cfg.NATS.ClusterID = v
	}
	if v := os.Getenv("WADJET_NATS_LEAF_REMOTES"); v != "" {
		cfg.NATS.LeafRemotes = strings.Split(v, ",")
	}
	if v := os.Getenv("WADJET_NATS_TLS_CERT"); v != "" {
		cfg.NATS.TLSCert = v
	}
	if v := os.Getenv("WADJET_NATS_TLS_KEY"); v != "" {
		cfg.NATS.TLSKey = v
	}
	if v := os.Getenv("WADJET_NATS_TLS_CA"); v != "" {
		cfg.NATS.TLSCA = v
	}
	if v := os.Getenv("WADJET_WORKER_MAX_CONCURRENT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Worker.MaxConcurrent = n
		}
	}
	if v := os.Getenv("WADJET_WORKER_MEMORY_BUDGET"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			cfg.Worker.MemoryBudget = n
		}
	}
	if v := os.Getenv("WADJET_WORKER_SPILL_DIR"); v != "" {
		cfg.Worker.SpillDir = v
	}
	if v := os.Getenv("WADJET_GEOIP_CITY_DB"); v != "" {
		cfg.GeoIP.CityDB = v
	}
	if v := os.Getenv("WADJET_GEOIP_ASN_DB"); v != "" {
		cfg.GeoIP.ASNDB = v
	}
	if v := os.Getenv("WADJET_QUERY_MAX_SCAN_BYTES"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			cfg.QueryLimits.MaxScanBytes = n
		}
	}
	if v := os.Getenv("WADJET_QUERY_MAX_SCAN_ROWS"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			cfg.QueryLimits.MaxScanRows = n
		}
	}
	if v := os.Getenv("WADJET_QUERY_MAX_SCAN_FILES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.QueryLimits.MaxScanFiles = n
		}
	}
	if v := os.Getenv("WADJET_OTEL_ENDPOINT"); v != "" {
		cfg.Telemetry.Endpoint = v
	}
	if v := os.Getenv("WADJET_OTEL_INSECURE"); v != "" {
		cfg.Telemetry.Insecure = strings.EqualFold(v, "true") || v == "1"
	}
	if v := os.Getenv("WADJET_OTEL_SAMPLE_RATE"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			cfg.Telemetry.SampleRate = f
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
