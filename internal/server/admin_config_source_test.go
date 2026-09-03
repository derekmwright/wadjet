package server

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/derekmwright/wadjet/internal/config"
)

// The admin config endpoint gates for #828.
//
// Before this arc GET reported `manager.Current()` — the config file merged
// over the defaults — while the process ran on the flag variables, and PUT
// wrote worker.* and parquet.* that the only subscriber in the tree (the
// auth reload) never applied, answering {"status":"applied"}. An operator
// read a configuration that was not the running one, changed it, was told
// it was applied, and nothing happened.

// adminOverResolution builds an admin API over a REAL resolution, so the
// per-key sources under test are the ones the loader produces.
func adminOverResolution(t *testing.T, subscribed []string) (*config.Manager, chi.Router, *atomic.Int32) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	fileCfg := config.DefaultConfig()
	fileCfg.Storage.Bucket = "bucket-from-file"
	fileCfg.Worker.MaxConcurrent = 6

	res := config.Resolve(config.Inputs{
		File:     &fileCfg,
		FileKeys: map[string]bool{"storage.bucket": true, "worker.max_concurrent": true},
		Lookup: func(name string) (string, bool) {
			if name == "WADJET_HTTP_ADDR" {
				return ":9999", true
			}
			return "", false
		},
	})

	mgr := config.NewManagerFromResolution(res, logger)
	var applied atomic.Int32
	if subscribed != nil {
		mgr.SubscribeKeys(subscribed, func(config.ChangeEvent) { applied.Add(1) })
	}
	r := chi.NewRouter()
	NewAdminAPI(mgr, nil, logger).RegisterRoutes(r)
	return mgr, r, &applied
}

func adminGet(t *testing.T, r chi.Router) map[string]any {
	t.Helper()
	w := httptest.NewRecorder()
	req := withAdminCtx(httptest.NewRequest("GET", "/v1/admin/config", nil))
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET /v1/admin/config: %d", w.Code)
	}
	var body map[string]any
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	return body
}

// TestAdminConfigReportsEffectiveValuesWithTheirSource: every registry key
// is reported, with the tier that won it.
func TestAdminConfigReportsEffectiveValuesWithTheirSource(t *testing.T) {
	_, router, _ := adminOverResolution(t, nil)
	body := adminGet(t, router)

	keys, ok := body["keys"].(map[string]any)
	if !ok {
		t.Fatalf("no per-key report: %v", body)
	}
	for _, k := range config.Keys() {
		if _, ok := keys[k.Name]; !ok {
			t.Errorf("%s: absent from the report", k.Name)
		}
	}

	for _, tc := range []struct {
		key    string
		value  any
		source string
	}{
		{"storage.bucket", "bucket-from-file", "file"},
		{"worker.max_concurrent", float64(6), "file"},
		{"http.addr", ":9999", "env"},
		{"grpc.addr", ":9090", "default"},
	} {
		entry, ok := keys[tc.key].(map[string]any)
		if !ok {
			t.Errorf("%s: not reported", tc.key)
			continue
		}
		if entry["value"] != tc.value {
			t.Errorf("%s: value %v, want %v", tc.key, entry["value"], tc.value)
		}
		if entry["source"] != tc.source {
			t.Errorf("%s: source %v, want %q", tc.key, entry["source"], tc.source)
		}
	}
}

// TestAdminConfigRedactsSecretsButNotTheirSource: an operator can see WHERE
// a credential came from without the endpoint echoing it back.
func TestAdminConfigRedactsSecretsButNotTheirSource(t *testing.T) {
	_, router, _ := adminOverResolution(t, nil)
	keys := adminGet(t, router)["keys"].(map[string]any)
	for _, name := range []string{"storage.access_key", "storage.secret_key"} {
		entry, ok := keys[name].(map[string]any)
		if !ok {
			t.Fatalf("%s: not reported", name)
		}
		if _, present := entry["value"]; present {
			t.Errorf("%s: the endpoint echoed the secret back: %v", name, entry)
		}
		if entry["redacted"] != true {
			t.Errorf("%s: not marked redacted: %v", name, entry)
		}
		if entry["source"] == nil {
			t.Errorf("%s: source withheld along with the value", name)
		}
	}
}

// TestAdminConfigRefusesAWriteNothingConsumes is #828's headline: a PUT of a
// key with no subscriber is refused BY NAME, not answered "applied".
func TestAdminConfigRefusesAWriteNothingConsumes(t *testing.T) {
	mgr, router, applied := adminOverResolution(t, []string{"auth"})

	body := []byte(`{"Worker":{"MaxConcurrent":32,"CacheBytes":1}}`)
	w := httptest.NewRecorder()
	req := withAdminCtx(httptest.NewRequest("PUT", "/v1/admin/config", bytes.NewReader(body)))
	router.ServeHTTP(w, req)

	if w.Code < 400 || w.Code >= 500 {
		t.Fatalf("a write nothing consumes returned %d; #828 wants a 4xx naming the key", w.Code)
	}
	var errBody map[string]string
	json.NewDecoder(w.Body).Decode(&errBody)
	if !strings.Contains(errBody["error"], "worker.max_concurrent") {
		t.Fatalf("the refusal does not name the key: %v", errBody)
	}
	if got := mgr.Current().Worker.MaxConcurrent; got != 6 {
		t.Fatalf("the refused write landed anyway: max_concurrent = %d, want 6", got)
	}
	if applied.Load() != 0 {
		t.Fatalf("a refused write still notified %d subscriber(s)", applied.Load())
	}
}

// TestAdminConfigAppliesAWriteASubscriberConsumes is the mirror: the refusal
// must not be satisfiable by refusing everything.
func TestAdminConfigAppliesAWriteASubscriberConsumes(t *testing.T) {
	mgr, router, applied := adminOverResolution(t, []string{"worker"})

	body := []byte(`{"Worker":{"MaxConcurrent":32,"CacheBytes":268435456}}`)
	w := httptest.NewRecorder()
	req := withAdminCtx(httptest.NewRequest("PUT", "/v1/admin/config", bytes.NewReader(body)))
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		var errBody map[string]string
		json.NewDecoder(w.Body).Decode(&errBody)
		t.Fatalf("a write with a subscriber returned %d: %v", w.Code, errBody)
	}
	if got := mgr.Current().Worker.MaxConcurrent; got != 32 {
		t.Fatalf("the accepted write did not land: max_concurrent = %d", got)
	}
	if applied.Load() != 1 {
		t.Fatalf("the subscriber was notified %d times, want 1", applied.Load())
	}

	// And the report follows the write, sourced as an admin change.
	entry := adminGet(t, router)["keys"].(map[string]any)["worker.max_concurrent"].(map[string]any)
	if entry["value"] != float64(32) || entry["source"] != "admin" {
		t.Fatalf("after the write the report says %v; want value 32 from source \"admin\"", entry)
	}
}

// TestAdminConfigReportsADeferredKeyAsNotReachingRuntime: parquet.* is
// resolved and reported, and says so — Rule 11's "never left half-live".
func TestAdminConfigReportsADeferredKeyAsNotReachingRuntime(t *testing.T) {
	_, router, _ := adminOverResolution(t, []string{"parquet"})
	keys := adminGet(t, router)["keys"].(map[string]any)

	entry, ok := keys["parquet.compression"].(map[string]any)
	if !ok {
		t.Fatal("parquet.compression is not reported at all")
	}
	if entry["reaches_runtime"] != false {
		t.Errorf("parquet.compression does not declare that it reaches no consumer: %v", entry)
	}
	if entry["hot_reloadable"] != false {
		t.Errorf("parquet.compression reports hot-reloadable despite having no consumer: %v", entry)
	}
	if s, _ := entry["deferred_reason"].(string); !strings.Contains(s, "ingest.DefaultConfig()") {
		t.Errorf("parquet.compression does not carry the deferral's mechanism: %v", entry)
	}
}

// flagTierDefaults is a FlagValues carrying only the flag DEFAULTS that
// DIVERGE from config.DefaultConfig() — the pair ADR-0029 decision 2 names.
// It is what makes the reload fixture resemble a real process: the running
// config's default tier is the flag's default, not DefaultConfig()'s.
type flagTierDefaults struct{}

func (flagTierDefaults) Changed(string) bool { return false }

func (f flagTierDefaults) Value(flag string) (any, bool) { return f.Default(flag) }

func (flagTierDefaults) Default(flag string) (any, bool) {
	switch flag {
	case "access-key", "secret-key":
		return "", true // --access-key defaults to ""; DefaultConfig says "minioadmin"
	case "cache-bytes":
		return int64(0), true // --cache-bytes defaults to 0; DefaultConfig says 256 MiB
	}
	return nil, false
}

// adminOverFlagTierResolution builds an admin API over a resolution that has
// a FLAG tier, from a config file that mentions nothing but `auth:` — the
// ordinary deployment shape.
func adminOverFlagTierResolution(t *testing.T, subscribed []string) (*config.Manager, chi.Router, string) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	path := filepath.Join(t.TempDir(), "wadjet.yaml")
	body := "auth:\n  enabled: true\n  api_keys:\n    - key: k\n      name: n\n      role: r\n" +
		"  roles:\n    - name: r\n      tables: [\"*\"]\n      allow: [read]\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	fileCfg, fileKeys, err := config.LoadWithKeys(path)
	if err != nil {
		t.Fatal(err)
	}
	res := config.Resolve(config.Inputs{
		File:     fileCfg,
		FileKeys: fileKeys,
		Lookup:   func(string) (string, bool) { return "", false },
		Flags:    flagTierDefaults{},
	})

	mgr := config.NewManagerFromResolution(res, logger)
	if subscribed != nil {
		mgr.SubscribeKeys(subscribed, func(config.ChangeEvent) {})
	}
	r := chi.NewRouter()
	NewAdminAPI(mgr, nil, logger).RegisterRoutes(r)
	return mgr, r, path
}

func postReload(t *testing.T, r chi.Router, path string) map[string]any {
	t.Helper()
	w := httptest.NewRecorder()
	req := withAdminCtx(httptest.NewRequest("POST", "/v1/admin/config/reload",
		bytes.NewBufferString(`{"path":`+strconv.Quote(path)+`}`)))
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("reload returned %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	return resp
}

func notApplied(t *testing.T, resp map[string]any) []string {
	t.Helper()
	raw, present := resp["not_applied"]
	if !present {
		return nil
	}
	var out []string
	for _, v := range raw.([]any) {
		out = append(out, v.(string))
	}
	return out
}

// TestReloadReportsOnlyKeysTheFileActuallySets is N1's gate and the guard
// against the way its first implementation went wrong.
//
// The report has to be "keys the FILE sets that nothing consumes", not a
// diff of the running config against Load(path): the running config's
// default tier is the FLAG's default while Load merges over
// DefaultConfig(), and ADR-0029 decision 2 exists because those differ.
// Diffing named storage.access_key, storage.secret_key and
// worker.cache_bytes on EVERY reload of ANY file in ANY deployment, plus one
// key per flag- or environment-sourced value on top — so the config-file
// watcher warned on every legitimate auth edit, and the one true positive
// arrived buried in sixteen false ones.
//
// The fixture is therefore the ordinary shape: a flag tier, and a file that
// mentions nothing but `auth:`.
func TestReloadReportsOnlyKeysTheFileActuallySets(t *testing.T) {
	_, router, path := adminOverFlagTierResolution(t, []string{"auth"})

	if got := notApplied(t, postReload(t, router, path)); got != nil {
		t.Fatalf("a reload of an auth-only file reported %v as not applied; the file "+
			"sets none of them, and this warning also reaches the watcher's log on "+
			"every legitimate auth edit", got)
	}
}

// TestReloadReportsTheKeysItCouldNotApply is the true-positive half: a file
// that DOES set a startup-only key gets that key back, and only that key.
func TestReloadReportsTheKeysItCouldNotApply(t *testing.T) {
	mgr, router, path := adminOverFlagTierResolution(t, []string{"auth"})

	body := "storage:\n  bucket: from-reload\nauth:\n  enabled: true\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	got := notApplied(t, postReload(t, router, path))
	if !slices.Equal(got, []string{"storage.bucket"}) {
		t.Fatalf("not_applied = %v, want exactly [storage.bucket]", got)
	}
	if b := mgr.Current().Storage.Bucket; b == "from-reload" {
		t.Fatalf("the key reported as NOT applied was applied anyway: bucket = %q", b)
	}
}

// TestReloadAppliesAndStaysQuietForAConsumedKey: a key a subscriber does
// consume is applied and is NOT reported.
func TestReloadAppliesAndStaysQuietForAConsumedKey(t *testing.T) {
	mgr, router, path := adminOverFlagTierResolution(t, []string{"auth", "worker"})

	body := "worker:\n  max_concurrent: 11\nauth:\n  enabled: true\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	resp := postReload(t, router, path)
	if got := notApplied(t, resp); got != nil {
		t.Fatalf("a key with a subscriber was reported as not applied: %v", got)
	}
	if got := mgr.Current().Worker.MaxConcurrent; got != 11 {
		t.Fatalf("worker.max_concurrent = %d, want 11", got)
	}
}
