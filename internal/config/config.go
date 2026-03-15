// Package config provides YAML configuration loading for Caelum.
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config is the top-level configuration for Caelum.
type Config struct {
	Mode    string  `yaml:"mode"`    // standalone, coordinator, worker
	Storage Storage `yaml:"storage"`
	NATS    NATS    `yaml:"nats"`
	HTTP    HTTP    `yaml:"http"`
	Worker  Worker  `yaml:"worker"`
	Parquet Parquet `yaml:"parquet"`
	Auth    Auth    `yaml:"auth"`
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
	Endpoint  string `yaml:"endpoint"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
	Bucket    string `yaml:"bucket"`
	UseSSL    bool   `yaml:"use_ssl"`
	Region    string `yaml:"region"`
}

// NATS configures the embedded NATS server or client connection.
type NATS struct {
	Port     int    `yaml:"port"`
	URL      string `yaml:"url"`       // for worker mode: coordinator's NATS URL
	StoreDir string `yaml:"store_dir"` // JetStream storage directory
}

// HTTP configures the HTTP API server.
type HTTP struct {
	Addr string `yaml:"addr"`
}

// Worker configures the worker.
type Worker struct {
	MaxConcurrent int   `yaml:"max_concurrent"`
	CacheBytes    int64 `yaml:"cache_bytes"`
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
			StoreDir: "/tmp/caelum-nats",
		},
		HTTP: HTTP{
			Addr: ":8080",
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
func LoadOrDefault(path string) *Config {
	if path == "" {
		cfg := DefaultConfig()
		return &cfg
	}
	cfg, err := Load(path)
	if err != nil {
		def := DefaultConfig()
		return &def
	}
	return cfg
}
