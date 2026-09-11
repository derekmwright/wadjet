# Config environment registry

Source: internal/config/config.go — func applyEnvOverrides(cfg *Config) {, moved 2026-09-11 (#1026)

applyEnvOverrides reads WADJET_* environment variables and overrides
config values. It is the environment TIER of the resolver (resolve.go) run
on its own, for callers that have no command line.

The variable set is the configuration registry's (registry.go), so this
function, the resolver, the admin endpoint's effective-value report and
the list below cannot disagree about which variables exist.
TestEnvironmentVariableNamesAgreeEverywhere asserts that the list below
and docs/configuration.md's tables name exactly this set.

Supported variables:

	WADJET_MODE                             - standalone, coordinator, worker
	WADJET_STORAGE_TYPE                     - s3, file
	WADJET_STORAGE_ENDPOINT                 - S3/MinIO endpoint
	WADJET_STORAGE_ACCESS_KEY               - S3 access key
	WADJET_STORAGE_SECRET_KEY               - S3 secret key
	WADJET_STORAGE_BUCKET                   - S3 bucket name
	WADJET_STORAGE_USE_SSL                  - true/false
	WADJET_STORAGE_REGION                   - S3 region
	WADJET_STORAGE_CIRCUIT_THRESHOLD        - consecutive failures before a breaker class opens
	WADJET_STORAGE_CIRCUIT_RESET            - how long an open breaker stays open (duration)
	WADJET_STORAGE_CIRCUIT_REQUEST_TIMEOUT  - per-request object-store timeout (duration)
	WADJET_NATS_PORT                        - NATS listen port
	WADJET_NATS_URL                         - NATS URL (worker mode)
	WADJET_NATS_CLUSTER_ID                  - cluster identifier
	WADJET_NATS_LEAF_REMOTES                - comma-separated remote NATS URLs
	WADJET_NATS_TLS_CERT                    - NATS TLS certificate file
	WADJET_NATS_TLS_KEY                     - NATS TLS private key file
	WADJET_NATS_TLS_CA                      - NATS TLS CA file (enables mTLS)
	WADJET_HTTP_ADDR                        - HTTP listen address
	WADJET_GRPC_ADDR                        - gRPC listen address
	WADJET_WORKER_MAX_CONCURRENT            - max concurrent tasks
	WADJET_WORKER_MEMORY_BUDGET             - per-task memory budget (bytes)
	WADJET_WORKER_SPILL_DIR                 - spill directory
	WADJET_ENABLE_ALERTS                    - true/false (CREATE ALERT DDL and scheduler)
	WADJET_GEOIP_CITY_DB                    - GeoLite2-City.mmdb path
	WADJET_GEOIP_ASN_DB                     - GeoLite2-ASN.mmdb path
	WADJET_QUERY_INTERMEDIATE_TTL           - queries/<id>/ reclaim age (duration)
	WADJET_QUERY_INTERMEDIATE_SWEEP         - queries/ sweep interval (duration)
	WADJET_QUERY_MAX_SCAN_BYTES             - max estimated scan bytes per query
	WADJET_QUERY_MAX_SCAN_ROWS              - max estimated scan rows per query
	WADJET_QUERY_MAX_SCAN_FILES             - max scan files per query
	WADJET_OTEL_ENDPOINT                    - OTLP gRPC endpoint (e.g. localhost:4317)
	WADJET_OTEL_INSECURE                    - true/false (plaintext gRPC)
	WADJET_OTEL_SAMPLE_RATE                 - 0.0-1.0 sampling rate
