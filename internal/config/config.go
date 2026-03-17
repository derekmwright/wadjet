// Package config provides YAML configuration loading for Caelum.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config is the top-level configuration for Caelum.
type Config struct {
	Mode    string  `yaml:"mode"`    // standalone, coordinator, worker
	Storage Storage `yaml:"storage"`
	NATS    NATS    `yaml:"nats"`
	HTTP    HTTP    `yaml:"http"`
	GRPC    GRPC    `yaml:"grpc"`
	Worker  Worker  `yaml:"worker"`
	Parquet Parquet `yaml:"parquet"`
	Auth    Auth    `yaml:"auth"`
	GeoIP   GeoIP   `yaml:"geoip"`
}

// GeoIP configures MaxMind GeoIP database paths.
type GeoIP struct {
	CityDB string `yaml:"city_db"` // path to GeoLite2-City.mmdb
	ASNDB  string `yaml:"asn_db"`  // path to GeoLite2-ASN.mmdb
}

// Auth configures authentication and authorization.
type Auth struct {
	Enabled  bool              `yaml:"enabled"`
	APIKeys  []AuthAPIKey      `yaml:"api_keys"`
	JWT      AuthJWT           `yaml:"jwt"`
	MTLS     AuthMTLS          `yaml:"mtls"`
	Roles    []AuthRole        `yaml:"roles"`
	Policies []AuthPolicy      `yaml:"policies"` // cell-level access policies
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
	CertFile    string            `yaml:"cert_file"`    // server TLS cert
	KeyFile     string            `yaml:"key_file"`     // server TLS key
	RoleMap     map[string]string `yaml:"role_map"`     // CN/SAN -> role
	DefaultRole string            `yaml:"default_role"`
}

// AuthRole defines a role with table access and permissions.
type AuthRole struct {
	Name   string   `yaml:"name"`
	Tables []string `yaml:"tables"` // table names or "*" for all
	Allow  []string `yaml:"allow"`  // "read", "write", "admin"
}

// AuthPolicy defines a cell-level access policy for a table+role.
type AuthPolicy struct {
	Table     string            `yaml:"table"`
	Role      string            `yaml:"role"`
	Columns   map[string]string `yaml:"columns"`    // column -> "allow", "mask", "deny"
	RowFilter string            `yaml:"row_filter"` // SQL WHERE predicate
}

// Storage configures the object store connection.
type Storage struct {
	Type      string `yaml:"type"`       // "s3" (default) or "file"
	DataDir   string `yaml:"data_dir"`   // local directory for type=file
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
	SpillDir         string `yaml:"spill_dir"`           // directory for spill files (default: os temp dir)
	ResultStoreBytes int64  `yaml:"result_store_bytes"`  // in-memory result store capacity (0 = disabled)
}

// Parquet configures Parquet file writing.
type Parquet struct {
	Compression    string `yaml:"compression"`     // snappy, zstd, gzip, lz4, none
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
			Bucket:    "caelum",
		},
		NATS: NATS{
			Port:     4222,
			StoreDir: filepath.Join(os.Getenv("HOME"), ".caelum", "nats"),
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
// Environment variables with CAELUM_ prefix always override file/default values.
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

// applyEnvOverrides reads CAELUM_* environment variables and overrides config values.
// Supported variables:
//
//	CAELUM_MODE                   - standalone, coordinator, worker
//	CAELUM_HTTP_ADDR              - HTTP listen address
//	CAELUM_GRPC_ADDR              - gRPC listen address
//	CAELUM_STORAGE_TYPE           - s3, file
//	CAELUM_STORAGE_ENDPOINT       - S3/MinIO endpoint
//	CAELUM_STORAGE_ACCESS_KEY     - S3 access key
//	CAELUM_STORAGE_SECRET_KEY     - S3 secret key
//	CAELUM_STORAGE_BUCKET         - S3 bucket name
//	CAELUM_STORAGE_REGION         - S3 region
//	CAELUM_STORAGE_USE_SSL        - true/false
//	CAELUM_NATS_PORT              - NATS listen port
//	CAELUM_NATS_URL               - NATS URL (worker mode)
//	CAELUM_NATS_CLUSTER_ID        - cluster identifier
//	CAELUM_NATS_LEAF_REMOTES      - comma-separated remote NATS URLs
//	CAELUM_WORKER_MAX_CONCURRENT  - max concurrent tasks
//	CAELUM_WORKER_MEMORY_BUDGET   - per-task memory budget (bytes)
//	CAELUM_WORKER_SPILL_DIR       - spill directory
//	CAELUM_GEOIP_CITY_DB          - GeoLite2-City.mmdb path
//	CAELUM_GEOIP_ASN_DB           - GeoLite2-ASN.mmdb path
func applyEnvOverrides(cfg *Config) {
	if v := os.Getenv("CAELUM_MODE"); v != "" {
		cfg.Mode = v
	}
	if v := os.Getenv("CAELUM_HTTP_ADDR"); v != "" {
		cfg.HTTP.Addr = v
	}
	if v := os.Getenv("CAELUM_GRPC_ADDR"); v != "" {
		cfg.GRPC.Addr = v
	}
	if v := os.Getenv("CAELUM_STORAGE_TYPE"); v != "" {
		cfg.Storage.Type = v
	}
	if v := os.Getenv("CAELUM_STORAGE_ENDPOINT"); v != "" {
		cfg.Storage.Endpoint = v
	}
	if v := os.Getenv("CAELUM_STORAGE_ACCESS_KEY"); v != "" {
		cfg.Storage.AccessKey = v
	}
	if v := os.Getenv("CAELUM_STORAGE_SECRET_KEY"); v != "" {
		cfg.Storage.SecretKey = v
	}
	if v := os.Getenv("CAELUM_STORAGE_BUCKET"); v != "" {
		cfg.Storage.Bucket = v
	}
	if v := os.Getenv("CAELUM_STORAGE_REGION"); v != "" {
		cfg.Storage.Region = v
	}
	if v := os.Getenv("CAELUM_STORAGE_USE_SSL"); v != "" {
		cfg.Storage.UseSSL = strings.EqualFold(v, "true") || v == "1"
	}
	if v := os.Getenv("CAELUM_NATS_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.NATS.Port = n
		}
	}
	if v := os.Getenv("CAELUM_NATS_URL"); v != "" {
		cfg.NATS.URL = v
	}
	if v := os.Getenv("CAELUM_NATS_CLUSTER_ID"); v != "" {
		cfg.NATS.ClusterID = v
	}
	if v := os.Getenv("CAELUM_NATS_LEAF_REMOTES"); v != "" {
		cfg.NATS.LeafRemotes = strings.Split(v, ",")
	}
	if v := os.Getenv("CAELUM_WORKER_MAX_CONCURRENT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Worker.MaxConcurrent = n
		}
	}
	if v := os.Getenv("CAELUM_WORKER_MEMORY_BUDGET"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			cfg.Worker.MemoryBudget = n
		}
	}
	if v := os.Getenv("CAELUM_WORKER_SPILL_DIR"); v != "" {
		cfg.Worker.SpillDir = v
	}
	if v := os.Getenv("CAELUM_GEOIP_CITY_DB"); v != "" {
		cfg.GeoIP.CityDB = v
	}
	if v := os.Getenv("CAELUM_GEOIP_ASN_DB"); v != "" {
		cfg.GeoIP.ASNDB = v
	}
}
