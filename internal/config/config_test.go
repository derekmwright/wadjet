package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Mode != "standalone" {
		t.Fatalf("expected mode=standalone, got %s", cfg.Mode)
	}
	if cfg.Storage.Bucket != "caelum" {
		t.Fatalf("expected bucket=caelum, got %s", cfg.Storage.Bucket)
	}
	if cfg.NATS.Port != 4222 {
		t.Fatalf("expected nats port=4222, got %d", cfg.NATS.Port)
	}
	if cfg.Parquet.Compression != "snappy" {
		t.Fatalf("expected compression=snappy, got %s", cfg.Parquet.Compression)
	}
}

func TestLoad(t *testing.T) {
	yaml := `
mode: coordinator
storage:
  endpoint: minio.prod:9000
  access_key: prod-key
  secret_key: prod-secret
  bucket: analytics
  use_ssl: true
nats:
  port: 4223
http:
  addr: ":9090"
worker:
  max_concurrent: 8
  cache_bytes: 536870912
parquet:
  compression: zstd
  row_group_size: 262144
  page_buffer_size: 1048576
`
	dir := t.TempDir()
	path := filepath.Join(dir, "caelum.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}

	if cfg.Mode != "coordinator" {
		t.Fatalf("expected mode=coordinator, got %s", cfg.Mode)
	}
	if cfg.Storage.Endpoint != "minio.prod:9000" {
		t.Fatalf("expected endpoint=minio.prod:9000, got %s", cfg.Storage.Endpoint)
	}
	if !cfg.Storage.UseSSL {
		t.Fatal("expected use_ssl=true")
	}
	if cfg.NATS.Port != 4223 {
		t.Fatalf("expected nats port=4223, got %d", cfg.NATS.Port)
	}
	if cfg.HTTP.Addr != ":9090" {
		t.Fatalf("expected addr=:9090, got %s", cfg.HTTP.Addr)
	}
	if cfg.Worker.MaxConcurrent != 8 {
		t.Fatalf("expected max_concurrent=8, got %d", cfg.Worker.MaxConcurrent)
	}
	if cfg.Parquet.Compression != "zstd" {
		t.Fatalf("expected compression=zstd, got %s", cfg.Parquet.Compression)
	}
	if cfg.Parquet.PageBufferSize != 1048576 {
		t.Fatalf("expected page_buffer_size=1048576, got %d", cfg.Parquet.PageBufferSize)
	}
}

func TestLoadPartial(t *testing.T) {
	// Only override some fields — rest should keep defaults
	yaml := `
mode: worker
nats:
  url: nats://coordinator:4222
`
	dir := t.TempDir()
	path := filepath.Join(dir, "caelum.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}

	if cfg.Mode != "worker" {
		t.Fatalf("expected mode=worker, got %s", cfg.Mode)
	}
	if cfg.NATS.URL != "nats://coordinator:4222" {
		t.Fatalf("expected nats url, got %s", cfg.NATS.URL)
	}
	// Defaults should still be present
	if cfg.Storage.Bucket != "caelum" {
		t.Fatalf("expected default bucket=caelum, got %s", cfg.Storage.Bucket)
	}
	if cfg.Worker.MaxConcurrent != 4 {
		t.Fatalf("expected default max_concurrent=4, got %d", cfg.Worker.MaxConcurrent)
	}
}

func TestLoadOrDefault_Missing(t *testing.T) {
	cfg := LoadOrDefault("/nonexistent/path.yaml")
	if cfg.Mode != "standalone" {
		t.Fatalf("expected default mode=standalone, got %s", cfg.Mode)
	}
}

func TestLoadAuth(t *testing.T) {
	yaml := `
mode: standalone
auth:
  enabled: true
  api_keys:
    - key: "ck_prod_abc123"
      name: "grafana"
      role: "reader"
    - key: "ck_prod_xyz789"
      name: "etl-pipeline"
      role: "writer"
  jwt:
    enabled: true
    secret: "my-jwt-secret"
    role_claim: "caelum_role"
    issuer: "auth.example.com"
  mtls:
    enabled: true
    ca_file: "/etc/caelum/ca.pem"
    cert_file: "/etc/caelum/server.pem"
    key_file: "/etc/caelum/server-key.pem"
    role_map:
      grafana.internal: reader
      etl.internal: writer
    default_role: reader
  roles:
    - name: admin
      tables: ["*"]
      allow: ["read", "write", "admin"]
    - name: writer
      tables: ["*"]
      allow: ["read", "write"]
    - name: reader
      tables: ["events", "metrics"]
      allow: ["read"]
  policies:
    - table: "users"
      role: "reader"
      columns:
        name: allow
        email: mask
        ssn: deny
      row_filter: "department = 'engineering'"
`
	dir := t.TempDir()
	path := filepath.Join(dir, "caelum.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}

	if !cfg.Auth.Enabled {
		t.Fatal("expected auth.enabled=true")
	}
	if len(cfg.Auth.APIKeys) != 2 {
		t.Fatalf("expected 2 API keys, got %d", len(cfg.Auth.APIKeys))
	}
	if cfg.Auth.APIKeys[0].Name != "grafana" {
		t.Fatalf("expected first key name=grafana, got %s", cfg.Auth.APIKeys[0].Name)
	}
	if !cfg.Auth.JWT.Enabled {
		t.Fatal("expected jwt.enabled=true")
	}
	if cfg.Auth.JWT.RoleClaim != "caelum_role" {
		t.Fatalf("expected role_claim=caelum_role, got %s", cfg.Auth.JWT.RoleClaim)
	}
	if cfg.Auth.JWT.Issuer != "auth.example.com" {
		t.Fatalf("expected issuer=auth.example.com, got %s", cfg.Auth.JWT.Issuer)
	}
	if !cfg.Auth.MTLS.Enabled {
		t.Fatal("expected mtls.enabled=true")
	}
	if cfg.Auth.MTLS.CAFile != "/etc/caelum/ca.pem" {
		t.Fatalf("expected ca_file path, got %s", cfg.Auth.MTLS.CAFile)
	}
	if cfg.Auth.MTLS.RoleMap["grafana.internal"] != "reader" {
		t.Fatalf("expected role_map grafana.internal=reader, got %s", cfg.Auth.MTLS.RoleMap["grafana.internal"])
	}
	if len(cfg.Auth.Roles) != 3 {
		t.Fatalf("expected 3 roles, got %d", len(cfg.Auth.Roles))
	}
	if len(cfg.Auth.Policies) != 1 {
		t.Fatalf("expected 1 policy, got %d", len(cfg.Auth.Policies))
	}
	if cfg.Auth.Policies[0].Columns["ssn"] != "deny" {
		t.Fatalf("expected ssn=deny policy, got %s", cfg.Auth.Policies[0].Columns["ssn"])
	}
	if cfg.Auth.Policies[0].RowFilter != "department = 'engineering'" {
		t.Fatalf("expected row_filter, got %s", cfg.Auth.Policies[0].RowFilter)
	}
}

func TestLoadAuthDisabledByDefault(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Auth.Enabled {
		t.Fatal("auth should be disabled by default")
	}
}
