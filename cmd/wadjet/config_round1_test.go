package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Round-1 review findings, each with the gate that would have caught it.

// TestTheCostGuardReachesTheRunPathFromTheEnvironmentAlone is P1's gate.
//
// The two `EffectiveQueryLimits()` assignments used to live inside
// `if configFile != ""`, so a deployment that exported
// WADJET_QUERY_MAX_SCAN_BYTES and passed no config file resolved the key,
// reported it through GET /v1/admin/config — and ran with NO cost guard.
// The old gate could not see it: it asserted the resolved config OBJECT,
// which was always right, and it always passed a `--config`.
//
// This one asserts the RUN PATH's own expression with no config file at all.
func TestTheCostGuardReachesTheRunPathFromTheEnvironmentAlone(t *testing.T) {
	t.Setenv("WADJET_QUERY_MAX_SCAN_BYTES", "12345")
	t.Setenv("WADJET_QUERY_MAX_SCAN_ROWS", "678")
	t.Setenv("WADJET_QUERY_MAX_SCAN_FILES", "9")

	if _, err := resolveThroughTheRealCommand(t, nil); err != nil {
		t.Fatal(err)
	}
	if configFile != "" {
		t.Fatalf("test setup: a config file leaked in (%q)", configFile)
	}

	// Exactly the expression runStandalone and runCoordinator evaluate.
	global, roles := effectiveConfig().EffectiveQueryLimits()
	if global == nil {
		t.Fatal("the cost guard is nil with the environment set and no --config: " +
			"srvCfg.QueryLimits and coord.SetQueryLimits would both receive nil, " +
			"which means UNLIMITED (#808)")
	}
	if global.MaxScanBytes != 12345 || global.MaxScanRows != 678 || global.MaxScanFiles != 9 {
		t.Fatalf("the guard carries %+v, want bytes=12345 rows=678 files=9", *global)
	}
	if roles != nil {
		t.Fatalf("no roles are configured, so the per-role map should be nil; got %v", roles)
	}
}

// TestTheCostGuardIsNilWhenNothingConfiguresIt is the mirror: hoisting the
// wiring out of the `--config` block must not turn an unconfigured
// deployment into a limited one.
func TestTheCostGuardIsNilWhenNothingConfiguresIt(t *testing.T) {
	for _, v := range []string{
		"WADJET_QUERY_MAX_SCAN_BYTES", "WADJET_QUERY_MAX_SCAN_ROWS", "WADJET_QUERY_MAX_SCAN_FILES",
	} {
		t.Setenv(v, "")
	}
	if _, err := resolveThroughTheRealCommand(t, nil); err != nil {
		t.Fatal(err)
	}
	if global, _ := effectiveConfig().EffectiveQueryLimits(); global != nil {
		t.Fatalf("an unconfigured deployment gained a cost guard: %+v", *global)
	}
}

// TestAnUnknownConfigKeyIsAStartupError is P3's gate.
//
// Before the loader a mistyped `storage:` key was inert anyway, because the
// whole section was. Now the difference between `bucket:` and `buckett:` is
// the difference between reading the right bucket and the wrong one, so the
// decode is strict and names what it did not recognise.
func TestAnUnknownConfigKeyIsAStartupError(t *testing.T) {
	for _, tc := range []struct {
		name, body, wantIn string
	}{
		{"a misspelled key", "storage:\n  buckett: typo-bucket\n", "buckett"},
		{"a misspelled section", "stroage:\n  bucket: typo-bucket\n", "stroage"},
		{"a key from another product", "storage:\n  bucket: b\n  minio_secure: true\n", "minio_secure"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "wadjet.yaml")
			if err := os.WriteFile(path, []byte(tc.body), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := resolveThroughTheRealCommand(t, []string{"--config", path})
			if err == nil {
				t.Fatalf("%q was accepted; the process would run on a default the "+
					"operator thinks they overrode, with nothing said at startup", tc.wantIn)
			}
			if !strings.Contains(err.Error(), tc.wantIn) {
				t.Fatalf("the error does not name the unrecognised key %q: %v", tc.wantIn, err)
			}
		})
	}
}

// TestAWellFormedConfigStillLoads is the mirror for P3: strictness must not
// be satisfiable by refusing every file. It uses one key from every section
// so a schema/tag mismatch shows up here rather than in a deployment.
func TestAWellFormedConfigStillLoads(t *testing.T) {
	body := `mode: coordinator
storage:
  type: file
  data_dir: /tmp/x
  bucket: b
  circuit:
    failure_threshold: 7
nats:
  port: 4555
  leaf_remotes:
    - nats://a:4222
http:
  addr: ":18080"
grpc:
  addr: ":19090"
worker:
  max_concurrent: 9
  result_store_bytes: 1024
geoip:
  city_db: ""
alerts:
  enabled: true
query:
  intermediate_ttl: 4h
query_limits:
  max_scan_bytes: 777
telemetry:
  endpoint: otel:4317
  sample_rate: 0.25
auth:
  enabled: true
  api_keys:
    - key: k
      name: n
      role: r
  roles:
    - name: r
      tables: ["*"]
      allow: [read]
      query_limits:
        max_scan_rows: 5
`
	path := filepath.Join(t.TempDir(), "wadjet.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	res, err := resolveThroughTheRealCommand(t, []string{"--config", path})
	if err != nil {
		t.Fatalf("a config file using every section was refused: %v", err)
	}
	if got := res.Config().Storage.Circuit.FailureThreshold; got != 7 {
		t.Errorf("storage.circuit.failure_threshold = %d, want 7", got)
	}
	if _, roles := res.Config().EffectiveQueryLimits(); roles == nil || roles["r"] == nil {
		t.Errorf("per-role query limits did not survive the strict decode: %v", roles)
	}
}

// TestADeferredSectionIsRefusedAtStartup is P4's gate.
//
// The deferral was honest on every WRITE path and silent at startup: a
// `parquet:` block was parsed, reported with "reaches_runtime": false, and
// otherwise ignored. An operator who never opens the admin endpoint sees a
// setting accepted and does nothing — the silent-inert shape #808 was filed
// for, which Rule 11's "never left half-live" forbids.
func TestADeferredSectionIsRefusedAtStartup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wadjet.yaml")
	body := "parquet:\n  compression: zstd\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := resolveThroughTheRealCommand(t, []string{"--config", path})
	if err == nil {
		t.Fatal("a `parquet:` block was accepted at startup; no writer reads it, so " +
			"the operator's setting is silently inert")
	}
	if !strings.Contains(err.Error(), "parquet.compression") {
		t.Fatalf("the refusal does not name the key: %v", err)
	}
	if !strings.Contains(err.Error(), "ingest.DefaultConfig()") {
		t.Fatalf("the refusal does not carry the deferral's mechanism: %v", err)
	}
}

// TestAConfigWithoutADeferredSectionStartsFine is the mirror for P4.
func TestAConfigWithoutADeferredSectionStartsFine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wadjet.yaml")
	if err := os.WriteFile(path, []byte("storage:\n  bucket: fine\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	res, err := resolveThroughTheRealCommand(t, []string{"--config", path})
	if err != nil {
		t.Fatalf("an ordinary config file was refused: %v", err)
	}
	if got := res.Config().Storage.Bucket; got != "fine" {
		t.Fatalf("storage.bucket = %q, want %q", got, "fine")
	}
}

// TestTelemetryReachesEveryModeThatHasAConsumer is P2's gate.
//
// initTelemetry was called from runCoordinator and runWorker and NOT from
// runStandalone, so `telemetry:` and WADJET_OTEL_* reached nothing in the
// default run mode. The assertion is on the call sites, because standing an
// OTLP collector up in a unit test would gate a wiring fact behind a network
// service.
func TestTelemetryReachesEveryModeThatHasAConsumer(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(src)
	for _, fn := range []string{"func runStandalone(", "func runCoordinator(", "func runWorker("} {
		start := strings.Index(text, fn)
		if start < 0 {
			t.Fatalf("%s not found", fn)
		}
		// The body runs to the next top-level func declaration.
		end := strings.Index(text[start+len(fn):], "\nfunc ")
		if end < 0 {
			end = len(text) - start - len(fn)
		}
		body := text[start : start+len(fn)+end]
		if !strings.Contains(body, "initTelemetry(") {
			t.Errorf("%s never calls initTelemetry: the telemetry: section and "+
				"WADJET_OTEL_* reach nothing in that mode", strings.TrimSuffix(fn, "("))
		}
	}
}
