package config

import (
	"strconv"
	"strings"
	"time"
)

// Kind is the Go type a configuration key carries. It decides how an
// environment variable's string is parsed and how a flag value is asserted.
type Kind int

const (
	KindString Kind = iota
	KindInt
	KindInt64
	KindBool
	KindFloat64
	KindDuration
	KindStringSlice
)

func (k Kind) String() string {
	switch k {
	case KindString:
		return "string"
	case KindInt:
		return "int"
	case KindInt64:
		return "int64"
	case KindBool:
		return "bool"
	case KindFloat64:
		return "float64"
	case KindDuration:
		return "duration"
	case KindStringSlice:
		return "[]string"
	}
	return "unknown"
}

// Key describes one configuration setting: where it can come from, what it
// carries, and how to read or write it on a Config.
//
// The registry is the single source of truth for the precedence machinery.
// The resolver (resolve.go), applyEnvOverrides, the admin endpoint's
// effective-value report and the docs' name-set gate all read this table, so
// a setting cannot exist on one of those paths and be missing from another.
type Key struct {
	// Name is the dotted config-file path, e.g. "storage.bucket".
	Name string
	// Env is the environment variable that sets this key ("" = none).
	Env string
	// Flag is the root command's persistent flag name ("" = none).
	Flag string
	// Kind is the value type.
	Kind Kind
	// Secret marks a value that must never be echoed back (admin GET
	// reports its SOURCE but redacts the value).
	Secret bool
	// Deferred marks a key that is parsed, resolved and reported but does
	// NOT reach a runtime consumer, with the structural change that would
	// be required named in DeferredWhy. Rule 11 of
	// docs/design/correctness-fix-protocol.md: such a key is deferred
	// explicitly, never left half-live — every write path refuses it.
	Deferred    bool
	DeferredWhy string

	get func(*Config) any
	set func(*Config, any)
}

// Get reads the key's value from a config.
func (k Key) Get(c *Config) any { return k.get(c) }

// Set writes the key's value into a config. v must carry the key's Kind.
func (k Key) Set(c *Config, v any) { k.set(c, v) }

// IsZero reports whether v is the zero value for the key's Kind.
func (k Key) IsZero(v any) bool {
	switch k.Kind {
	case KindString:
		return v.(string) == ""
	case KindInt:
		return v.(int) == 0
	case KindInt64:
		return v.(int64) == 0
	case KindBool:
		return !v.(bool)
	case KindFloat64:
		return v.(float64) == 0
	case KindDuration:
		return v.(time.Duration) == 0
	case KindStringSlice:
		return len(v.([]string)) == 0
	}
	return false
}

// ParseEnv converts an environment variable's text to the key's Kind.
// An unparseable value is reported as not-ok and leaves the lower tier in
// place, which is what applyEnvOverrides has always done.
func (k Key) ParseEnv(s string) (any, bool) {
	switch k.Kind {
	case KindString:
		return s, true
	case KindInt:
		n, err := strconv.Atoi(s)
		if err != nil {
			return nil, false
		}
		return n, true
	case KindInt64:
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, false
		}
		return n, true
	case KindBool:
		return strings.EqualFold(s, "true") || s == "1", true
	case KindFloat64:
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, false
		}
		return f, true
	case KindDuration:
		d, err := time.ParseDuration(s)
		if err != nil {
			return nil, false
		}
		return d, true
	case KindStringSlice:
		return strings.Split(s, ","), true
	}
	return nil, false
}

// keys is the registry. Order is the order the admin endpoint reports in.
var keys = []Key{
	{
		Name: "mode", Env: "WADJET_MODE", Flag: "mode", Kind: KindString,
		get: func(c *Config) any { return c.Mode },
		set: func(c *Config, v any) { c.Mode = v.(string) },
	},
	{
		Name: "storage.type", Env: "WADJET_STORAGE_TYPE", Flag: "storage-type", Kind: KindString,
		get: func(c *Config) any { return c.Storage.Type },
		set: func(c *Config, v any) { c.Storage.Type = v.(string) },
	},
	{
		Name: "storage.data_dir", Flag: "data-dir", Kind: KindString,
		get: func(c *Config) any { return c.Storage.DataDir },
		set: func(c *Config, v any) { c.Storage.DataDir = v.(string) },
	},
	{
		Name: "storage.endpoint", Env: "WADJET_STORAGE_ENDPOINT", Flag: "endpoint", Kind: KindString,
		get: func(c *Config) any { return c.Storage.Endpoint },
		set: func(c *Config, v any) { c.Storage.Endpoint = v.(string) },
	},
	{
		Name: "storage.access_key", Env: "WADJET_STORAGE_ACCESS_KEY", Flag: "access-key", Kind: KindString, Secret: true,
		get: func(c *Config) any { return c.Storage.AccessKey },
		set: func(c *Config, v any) { c.Storage.AccessKey = v.(string) },
	},
	{
		Name: "storage.secret_key", Env: "WADJET_STORAGE_SECRET_KEY", Flag: "secret-key", Kind: KindString, Secret: true,
		get: func(c *Config) any { return c.Storage.SecretKey },
		set: func(c *Config, v any) { c.Storage.SecretKey = v.(string) },
	},
	{
		Name: "storage.bucket", Env: "WADJET_STORAGE_BUCKET", Flag: "bucket", Kind: KindString,
		get: func(c *Config) any { return c.Storage.Bucket },
		set: func(c *Config, v any) { c.Storage.Bucket = v.(string) },
	},
	{
		Name: "storage.use_ssl", Env: "WADJET_STORAGE_USE_SSL", Flag: "ssl", Kind: KindBool,
		get: func(c *Config) any { return c.Storage.UseSSL },
		set: func(c *Config, v any) { c.Storage.UseSSL = v.(bool) },
	},
	{
		Name: "storage.region", Env: "WADJET_STORAGE_REGION", Flag: "region", Kind: KindString,
		get: func(c *Config) any { return c.Storage.Region },
		set: func(c *Config, v any) { c.Storage.Region = v.(string) },
	},
	{
		Name: "storage.circuit.failure_threshold", Env: "WADJET_STORAGE_CIRCUIT_THRESHOLD", Flag: "storage-circuit-threshold", Kind: KindInt,
		get: func(c *Config) any { return c.Storage.Circuit.FailureThreshold },
		set: func(c *Config, v any) { c.Storage.Circuit.FailureThreshold = v.(int) },
	},
	{
		Name: "storage.circuit.reset_timeout", Env: "WADJET_STORAGE_CIRCUIT_RESET", Flag: "storage-circuit-reset", Kind: KindDuration,
		get: func(c *Config) any { return c.Storage.Circuit.ResetTimeout },
		set: func(c *Config, v any) { c.Storage.Circuit.ResetTimeout = v.(time.Duration) },
	},
	{
		Name: "storage.circuit.request_timeout", Env: "WADJET_STORAGE_CIRCUIT_REQUEST_TIMEOUT", Flag: "storage-circuit-request-timeout", Kind: KindDuration,
		get: func(c *Config) any { return c.Storage.Circuit.RequestTimeout },
		set: func(c *Config, v any) { c.Storage.Circuit.RequestTimeout = v.(time.Duration) },
	},
	{
		Name: "nats.port", Env: "WADJET_NATS_PORT", Flag: "nats-port", Kind: KindInt,
		get: func(c *Config) any { return c.NATS.Port },
		set: func(c *Config, v any) { c.NATS.Port = v.(int) },
	},
	{
		Name: "nats.url", Env: "WADJET_NATS_URL", Flag: "nats-url", Kind: KindString,
		get: func(c *Config) any { return c.NATS.URL },
		set: func(c *Config, v any) { c.NATS.URL = v.(string) },
	},
	{
		Name: "nats.store_dir", Flag: "nats-store-dir", Kind: KindString,
		get: func(c *Config) any { return c.NATS.StoreDir },
		set: func(c *Config, v any) { c.NATS.StoreDir = v.(string) },
	},
	{
		Name: "nats.cluster_id", Env: "WADJET_NATS_CLUSTER_ID", Flag: "cluster-id", Kind: KindString,
		get: func(c *Config) any { return c.NATS.ClusterID },
		set: func(c *Config, v any) { c.NATS.ClusterID = v.(string) },
	},
	{
		Name: "nats.leaf_remotes", Env: "WADJET_NATS_LEAF_REMOTES", Flag: "leaf-remote", Kind: KindStringSlice,
		get: func(c *Config) any { return c.NATS.LeafRemotes },
		set: func(c *Config, v any) { c.NATS.LeafRemotes = v.([]string) },
	},
	{
		Name: "nats.tls_cert", Env: "WADJET_NATS_TLS_CERT", Flag: "nats-tls-cert", Kind: KindString,
		get: func(c *Config) any { return c.NATS.TLSCert },
		set: func(c *Config, v any) { c.NATS.TLSCert = v.(string) },
	},
	{
		Name: "nats.tls_key", Env: "WADJET_NATS_TLS_KEY", Flag: "nats-tls-key", Kind: KindString,
		get: func(c *Config) any { return c.NATS.TLSKey },
		set: func(c *Config, v any) { c.NATS.TLSKey = v.(string) },
	},
	{
		Name: "nats.tls_ca", Env: "WADJET_NATS_TLS_CA", Flag: "nats-tls-ca", Kind: KindString,
		get: func(c *Config) any { return c.NATS.TLSCA },
		set: func(c *Config, v any) { c.NATS.TLSCA = v.(string) },
	},
	{
		Name: "http.addr", Env: "WADJET_HTTP_ADDR", Flag: "http-addr", Kind: KindString,
		get: func(c *Config) any { return c.HTTP.Addr },
		set: func(c *Config, v any) { c.HTTP.Addr = v.(string) },
	},
	{
		Name: "grpc.addr", Env: "WADJET_GRPC_ADDR", Flag: "grpc-addr", Kind: KindString,
		get: func(c *Config) any { return c.GRPC.Addr },
		set: func(c *Config, v any) { c.GRPC.Addr = v.(string) },
	},
	{
		Name: "worker.max_concurrent", Env: "WADJET_WORKER_MAX_CONCURRENT", Flag: "max-concurrent", Kind: KindInt,
		get: func(c *Config) any { return c.Worker.MaxConcurrent },
		set: func(c *Config, v any) { c.Worker.MaxConcurrent = v.(int) },
	},
	{
		Name: "worker.cache_bytes", Flag: "cache-bytes", Kind: KindInt64,
		get: func(c *Config) any { return c.Worker.CacheBytes },
		set: func(c *Config, v any) { c.Worker.CacheBytes = v.(int64) },
	},
	{
		Name: "worker.memory_budget", Env: "WADJET_WORKER_MEMORY_BUDGET", Flag: "memory-budget", Kind: KindInt64,
		get: func(c *Config) any { return c.Worker.MemoryBudget },
		set: func(c *Config, v any) { c.Worker.MemoryBudget = v.(int64) },
	},
	{
		Name: "worker.spill_dir", Env: "WADJET_WORKER_SPILL_DIR", Flag: "spill-dir", Kind: KindString,
		get: func(c *Config) any { return c.Worker.SpillDir },
		set: func(c *Config, v any) { c.Worker.SpillDir = v.(string) },
	},
	{
		Name: "worker.result_store_bytes", Flag: "result-store", Kind: KindInt64,
		get: func(c *Config) any { return c.Worker.ResultStoreBytes },
		set: func(c *Config, v any) { c.Worker.ResultStoreBytes = v.(int64) },
	},
	{
		Name: "parquet.compression", Kind: KindString,
		Deferred:    true,
		DeferredWhy: parquetDeferral,
		get:         func(c *Config) any { return c.Parquet.Compression },
		set:         func(c *Config, v any) { c.Parquet.Compression = v.(string) },
	},
	{
		Name: "parquet.row_group_size", Kind: KindInt,
		Deferred:    true,
		DeferredWhy: parquetDeferral,
		get:         func(c *Config) any { return c.Parquet.RowGroupSize },
		set:         func(c *Config, v any) { c.Parquet.RowGroupSize = v.(int) },
	},
	{
		Name: "parquet.page_buffer_size", Kind: KindInt,
		Deferred:    true,
		DeferredWhy: parquetDeferral,
		get:         func(c *Config) any { return c.Parquet.PageBufferSize },
		set:         func(c *Config, v any) { c.Parquet.PageBufferSize = v.(int) },
	},
	{
		Name: "alerts.enabled", Env: "WADJET_ENABLE_ALERTS", Flag: "enable-alerts", Kind: KindBool,
		get: func(c *Config) any { return c.Alerts.Enabled },
		set: func(c *Config, v any) { c.Alerts.Enabled = v.(bool) },
	},
	{
		Name: "geoip.city_db", Env: "WADJET_GEOIP_CITY_DB", Flag: "geoip-city", Kind: KindString,
		get: func(c *Config) any { return c.GeoIP.CityDB },
		set: func(c *Config, v any) { c.GeoIP.CityDB = v.(string) },
	},
	{
		Name: "geoip.asn_db", Env: "WADJET_GEOIP_ASN_DB", Flag: "geoip-asn", Kind: KindString,
		get: func(c *Config) any { return c.GeoIP.ASNDB },
		set: func(c *Config, v any) { c.GeoIP.ASNDB = v.(string) },
	},
	{
		Name: "query.intermediate_ttl", Env: "WADJET_QUERY_INTERMEDIATE_TTL", Flag: "query-intermediate-ttl", Kind: KindDuration,
		get: func(c *Config) any { return c.Query.IntermediateTTL },
		set: func(c *Config, v any) { c.Query.IntermediateTTL = v.(time.Duration) },
	},
	{
		Name: "query.intermediate_sweep", Env: "WADJET_QUERY_INTERMEDIATE_SWEEP", Flag: "query-intermediate-sweep", Kind: KindDuration,
		get: func(c *Config) any { return c.Query.IntermediateSweep },
		set: func(c *Config, v any) { c.Query.IntermediateSweep = v.(time.Duration) },
	},
	{
		Name: "query_limits.max_scan_bytes", Env: "WADJET_QUERY_MAX_SCAN_BYTES", Kind: KindInt64,
		get: func(c *Config) any { return c.QueryLimits.MaxScanBytes },
		set: func(c *Config, v any) { c.QueryLimits.MaxScanBytes = v.(int64) },
	},
	{
		Name: "query_limits.max_scan_rows", Env: "WADJET_QUERY_MAX_SCAN_ROWS", Kind: KindInt64,
		get: func(c *Config) any { return c.QueryLimits.MaxScanRows },
		set: func(c *Config, v any) { c.QueryLimits.MaxScanRows = v.(int64) },
	},
	{
		Name: "query_limits.max_scan_files", Env: "WADJET_QUERY_MAX_SCAN_FILES", Kind: KindInt,
		get: func(c *Config) any { return c.QueryLimits.MaxScanFiles },
		set: func(c *Config, v any) { c.QueryLimits.MaxScanFiles = v.(int) },
	},
	{
		Name: "query_limits.require_filter_above_bytes", Kind: KindInt64,
		get: func(c *Config) any { return c.QueryLimits.RequireFilterAboveBytes },
		set: func(c *Config, v any) { c.QueryLimits.RequireFilterAboveBytes = v.(int64) },
	},
	{
		Name: "query_limits.require_limit_above_rows", Kind: KindInt64,
		get: func(c *Config) any { return c.QueryLimits.RequireLimitAboveRows },
		set: func(c *Config, v any) { c.QueryLimits.RequireLimitAboveRows = v.(int64) },
	},
	{
		Name: "telemetry.endpoint", Env: "WADJET_OTEL_ENDPOINT", Flag: "otel-endpoint", Kind: KindString,
		get: func(c *Config) any { return c.Telemetry.Endpoint },
		set: func(c *Config, v any) { c.Telemetry.Endpoint = v.(string) },
	},
	{
		Name: "telemetry.insecure", Env: "WADJET_OTEL_INSECURE", Flag: "otel-insecure", Kind: KindBool,
		get: func(c *Config) any { return c.Telemetry.Insecure },
		set: func(c *Config, v any) { c.Telemetry.Insecure = v.(bool) },
	},
	{
		Name: "telemetry.sample_rate", Env: "WADJET_OTEL_SAMPLE_RATE", Kind: KindFloat64,
		get: func(c *Config) any { return c.Telemetry.SampleRate },
		set: func(c *Config, v any) { c.Telemetry.SampleRate = v.(float64) },
	},
}

// parquetDeferral is the mechanism note carried by every parquet.* key.
//
// The `parquet:` section has NO runtime consumer to reach. Every writer the
// serve path can reach is built from `ingest.DefaultConfig()` at seven call
// sites across `wadjet/dml.go`, `internal/server/server.go` and
// `internal/server/pgwire/server.go`, none of which sees the CLI config;
// `ingest.Config` carries only RowGroupSize, not Compression or
// PageBufferSize. Making these keys live is a plumbing change through
// `wadjet.Config` → `ingest.Config` → `parquet.WriterConfig` at those call
// sites, which is a structural change in three other packages and its own
// piece of work. Rule 11: DEFERRED with the mechanism, not left half-live —
// the keys resolve and report like any other, and every write path REFUSES
// them rather than accepting a value nothing consumes (#828).
const parquetDeferral = "no runtime consumer: every ingest writer is built from ingest.DefaultConfig(); " +
	"reaching the writer needs wadjet.Config -> ingest.Config -> parquet.WriterConfig plumbing at seven call sites"

// Keys returns the configuration registry.
func Keys() []Key {
	out := make([]Key, len(keys))
	copy(out, keys)
	return out
}

// KeyByName looks a key up by its dotted name.
func KeyByName(name string) (Key, bool) {
	for _, k := range keys {
		if k.Name == name {
			return k, true
		}
	}
	return Key{}, false
}

// EnvNames returns every environment variable the registry reads, in
// registry order.
func EnvNames() []string {
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		if k.Env != "" {
			out = append(out, k.Env)
		}
	}
	return out
}
