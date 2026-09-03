package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/derekmwright/wadjet/internal/config"
)

// The precedence census, REAL arm.
//
// internal/config has a loader-chain arm over a model of the command line.
// This one builds the actual *cobra.Command with the actual flag
// registrations and runs the actual PersistentPreRunE through
// cobra.Execute, because a census that only ever ran against a model could
// pass while the binary disagreed with it — which is the shape of #808
// itself: `applyEnvOverrides` was correct and complete, and nothing on the
// serve path called it.

// resolveThroughTheRealCommand runs the real root command with a probe
// subcommand and returns the resolution the probe saw.
func resolveThroughTheRealCommand(t *testing.T, args []string) (*config.Resolution, error) {
	t.Helper()
	root := newRootCmd()
	resolvedConfig.Store(nil)
	t.Cleanup(func() {
		newRootCmd() // restore every bound variable to its default
		resolvedConfig.Store(nil)
	})

	var captured *config.Resolution
	probe := &cobra.Command{
		Use: "probe",
		RunE: func(*cobra.Command, []string) error {
			captured = effectiveResolution()
			return nil
		},
	}
	root.AddCommand(probe)
	root.SetOut(io.Discard)
	root.SetErr(io.Discard)
	root.SetArgs(append([]string{"probe"}, args...))
	err := root.Execute()
	return captured, err
}

// censusTiers are one key's three sentinel values plus their command-line
// and YAML spellings.
type censusTiers struct {
	file, env, flag any
	fileYAML        string // the value as it appears in the config file
	envText         string
	flagText        string
}

func censusSentinels(k config.Key) censusTiers {
	switch k.Kind {
	case config.KindString:
		f, e, g := "file-value", "env-value", "flag-value"
		return censusTiers{f, e, g, f, e, g}
	case config.KindInt:
		return censusTiers{11, 22, 33, "11", "22", "33"}
	case config.KindInt64:
		return censusTiers{int64(111), int64(222), int64(333), "111", "222", "333"}
	case config.KindBool:
		// A bool cannot carry three distinguishable tier values. Making the
		// environment's FALSE against the file's TRUE at least makes the
		// file+env cell move the value, so that cell asserts more than the
		// reported source; the rest ride on source, which is what the admin
		// endpoint publishes anyway.
		return censusTiers{true, false, true, "true", "false", "true"}
	case config.KindFloat64:
		return censusTiers{0.11, 0.22, 0.33, "0.11", "0.22", "0.33"}
	case config.KindDuration:
		return censusTiers{
			11 * time.Second, 22 * time.Second, 33 * time.Second,
			"11s", "22s", "33s",
		}
	case config.KindStringSlice:
		return censusTiers{
			[]string{"file-a", "file-b"}, []string{"env-a"}, []string{"flag-a", "flag-b"},
			"", "env-a", "flag-a,flag-b",
		}
	}
	t := censusTiers{}
	return t
}

// configFileFor writes a YAML file that sets exactly one key. The nesting is
// derived from the key's DOTTED NAME, so the file also asserts that the
// registry's names are the config file's paths.
func configFileFor(t *testing.T, k config.Key, tiers censusTiers) string {
	t.Helper()
	parts := strings.Split(k.Name, ".")
	var sb strings.Builder
	for i, p := range parts[:len(parts)-1] {
		fmt.Fprintf(&sb, "%s%s:\n", strings.Repeat("  ", i), p)
	}
	indent := strings.Repeat("  ", len(parts)-1)
	leaf := parts[len(parts)-1]
	if k.Kind == config.KindStringSlice {
		fmt.Fprintf(&sb, "%s%s:\n", indent, leaf)
		for _, v := range tiers.file.([]string) {
			fmt.Fprintf(&sb, "%s  - %s\n", indent, v)
		}
	} else if k.Kind == config.KindString {
		fmt.Fprintf(&sb, "%s%s: %q\n", indent, leaf, tiers.fileYAML)
	} else {
		fmt.Fprintf(&sb, "%s%s: %s\n", indent, leaf, tiers.fileYAML)
	}
	path := filepath.Join(t.TempDir(), "wadjet.yaml")
	if err := os.WriteFile(path, []byte(sb.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestPrecedenceCensusOnTheRealCommand: every registry key, all eight
// presence combinations of (config file, environment, explicit flag), driven
// through the binary's own command and loader.
func TestPrecedenceCensusOnTheRealCommand(t *testing.T) {
	for _, k := range config.Keys() {
		k := k
		tiers := censusSentinels(k)
		for cell := 0; cell < 8; cell++ {
			hasFile, hasEnv, hasFlag := cell&1 != 0, cell&2 != 0, cell&4 != 0
			envUsable := hasEnv && k.Env != ""
			flagUsable := hasFlag && k.Flag != ""

			t.Run(fmt.Sprintf("%s/file=%v,env=%v,flag=%v", k.Name, hasFile, hasEnv, hasFlag), func(t *testing.T) {
				var args []string
				if hasFile {
					args = append(args, "--config", configFileFor(t, k, tiers))
				}
				if envUsable {
					t.Setenv(k.Env, tiers.envText)
				}
				if flagUsable {
					args = append(args, "--"+k.Flag+"="+tiers.flagText)
				}

				res, err := resolveThroughTheRealCommand(t, args)

				// A DEFERRED key set from any real tier is a startup error
				// naming it: the section has no consumer, so accepting it
				// would be the silent no-op #808 was filed for (Rule 11).
				// Only the config file can reach one — none has a flag or an
				// environment variable — so the file cells are the refusals.
				if k.Deferred && hasFile {
					if err == nil {
						t.Fatalf("%s was accepted from the config file; a key no consumer "+
							"reads must be refused at startup, not parsed and ignored", k.Name)
					}
					if !strings.Contains(err.Error(), k.Name) {
						t.Fatalf("the refusal does not name the key: %v", err)
					}
					return
				}
				if err != nil {
					t.Fatalf("running the command: %v", err)
				}

				wantSource := config.SourceDefault
				var wantValue any
				switch {
				case flagUsable:
					wantValue, wantSource = tiers.flag, config.SourceFlag
				case envUsable:
					wantValue, wantSource = tiers.env, config.SourceEnv
				case hasFile:
					wantValue, wantSource = tiers.file, config.SourceFile
				default:
					wantValue = defaultForKey(k)
				}

				if got := k.Get(res.Config()); !reflect.DeepEqual(got, wantValue) {
					t.Errorf("%s = %#v, want %#v (from %s)", k.Name, got, wantValue, wantSource)
				}
				if got := res.Source(k.Name); got != wantSource {
					t.Errorf("%s source = %q, want %q", k.Name, got, wantSource)
				}
			})
		}
	}
}

// defaultForKey is the value the binary runs on when nothing is configured:
// the flag's registered default where the key has a flag.
func defaultForKey(k config.Key) any {
	if k.Flag != "" {
		if v, ok := configFlagDefaults[k.Flag]; ok {
			return v
		}
	}
	d := config.DefaultConfig()
	return k.Get(&d)
}

// changedBehaviourFlags are the flags whose DEFAULT is not the zero value
// and which shadow a tier that already existed before this arc — so a
// deployment that set the same setting two ways changes behaviour. The
// maintainer accepted this blast radius on #808; docs/configuration.md and
// the release notes name the same list.
var changedBehaviourFlags = map[string]string{
	"mode":           "mode",
	"http-addr":      "http.addr",
	"grpc-addr":      "grpc.addr",
	"storage-type":   "storage.type",
	"endpoint":       "storage.endpoint",
	"bucket":         "storage.bucket",
	"nats-port":      "nats.port",
	"cluster-id":     "nats.cluster_id",
	"max-concurrent": "worker.max_concurrent",
	// The tenth, found by this pass: --result-store (512 MiB) shadows
	// worker.result_store_bytes, which is a config-FILE key with no
	// environment variable. Nine was the count of env-shadowing flags.
	"result-store": "worker.result_store_bytes",
}

// newKeysWithNonZeroDefaults are non-zero-default flags whose file and
// environment tiers DID NOT EXIST before this arc, so no deployment can
// change behaviour under them.
var newKeysWithNonZeroDefaults = map[string]bool{
	"storage.circuit.failure_threshold": true,
	"storage.circuit.reset_timeout":     true,
	"storage.circuit.request_timeout":   true,
	"query.intermediate_ttl":            true,
	"query.intermediate_sweep":          true,
}

// TestEveryNonZeroDefaultFlagIsAccountedFor: the changed-behaviour list is
// asserted in BOTH directions, so a new non-zero-default flag cannot quietly
// join the blast radius without the docs naming it.
func TestEveryNonZeroDefaultFlagIsAccountedFor(t *testing.T) {
	newRootCmd()
	found := map[string]string{}
	for _, k := range config.Keys() {
		if k.Flag == "" {
			continue
		}
		d, ok := configFlagDefaults[k.Flag]
		if !ok {
			t.Errorf("%s: flag --%s has no snapshotted default", k.Name, k.Flag)
			continue
		}
		if k.IsZero(d) || newKeysWithNonZeroDefaults[k.Name] {
			continue
		}
		found[k.Flag] = k.Name
	}
	if !reflect.DeepEqual(found, changedBehaviourFlags) {
		t.Fatalf("the set of flags whose default now loses to a lower tier changed.\n got: %v\nwant: %v\n"+
			"Every entry is a deployment-visible behaviour change and must be named in "+
			"docs/configuration.md and the release notes (#808).", found, changedBehaviourFlags)
	}
}

// TestAFlagLeftAtItsDefaultLosesToTheEnvironment is the nine (now ten)
// named subtests: the exact settings whose behaviour changes.
func TestAFlagLeftAtItsDefaultLosesToTheEnvironment(t *testing.T) {
	names := make([]string, 0, len(changedBehaviourFlags))
	for f := range changedBehaviourFlags {
		names = append(names, f)
	}
	sort.Strings(names)

	for _, flagName := range names {
		flagName := flagName
		keyName := changedBehaviourFlags[flagName]
		t.Run("--"+flagName, func(t *testing.T) {
			k, ok := config.KeyByName(keyName)
			if !ok {
				t.Fatalf("%s left the registry", keyName)
			}
			tiers := censusSentinels(k)

			// The lower tier is the environment where one exists, and the
			// config file for the flag-and-file-only settings.
			var args []string
			wantSource := config.SourceEnv
			if k.Env != "" {
				t.Setenv(k.Env, tiers.envText)
			} else {
				args = append(args, "--config", configFileFor(t, k, tiers))
				wantSource = config.SourceFile
			}

			res, err := resolveThroughTheRealCommand(t, args)
			if err != nil {
				t.Fatal(err)
			}
			want := tiers.env
			if wantSource == config.SourceFile {
				want = tiers.file
			}
			if got := k.Get(res.Config()); !reflect.DeepEqual(got, want) {
				t.Fatalf("%s = %#v, want %#v — the flag's default %#v beat the %s tier (#808)",
					keyName, got, want, defaultForKey(k), wantSource)
			}
		})
	}
}

// TestConfigFlagBindingsMatchTheRegistry: the write-back table and the
// registry name the same flags, and every one of them is a real persistent
// flag of the root command.
func TestConfigFlagBindingsMatchTheRegistry(t *testing.T) {
	root := newRootCmd()

	inRegistry := map[string]bool{}
	for _, k := range config.Keys() {
		if k.Flag == "" {
			continue
		}
		inRegistry[k.Flag] = true
		if _, ok := configFlagBindings[k.Flag]; !ok {
			t.Errorf("%s names flag --%s with no binding: the loader cannot write it back", k.Name, k.Flag)
		}
		if root.PersistentFlags().Lookup(k.Flag) == nil {
			t.Errorf("%s names flag --%s, which the root command does not register", k.Name, k.Flag)
		}
	}
	for flagName := range configFlagBindings {
		if !inRegistry[flagName] {
			t.Errorf("--%s is bound for write-back but no registry key claims it", flagName)
		}
	}
}

// TestEveryConfigKeyReachesItsFlagVariable: the write-back is TOTAL over the
// registry. It is what lets a consumer keep reading a flag variable and
// still see the resolved value — the property that makes the two-pass load
// safe without editing twenty-four binding points.
func TestEveryConfigKeyReachesItsFlagVariable(t *testing.T) {
	// Set every flagged key from the environment or the file, so every
	// bound variable has to MOVE off its default for the check to pass.
	var args []string
	dir := t.TempDir()
	var yaml strings.Builder
	yaml.WriteString("storage:\n  data_dir: /file/data\n")
	yaml.WriteString("nats:\n  store_dir: /file/nats\n")
	yaml.WriteString("worker:\n  cache_bytes: 4242\n  result_store_bytes: 4343\n")
	path := filepath.Join(dir, "wadjet.yaml")
	if err := os.WriteFile(path, []byte(yaml.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	args = append(args, "--config", path)

	for _, k := range config.Keys() {
		if k.Flag == "" || k.Env == "" {
			continue
		}
		t.Setenv(k.Env, censusSentinels(k).envText)
	}

	res, err := resolveThroughTheRealCommand(t, args)
	if err != nil {
		t.Fatal(err)
	}
	// The probe has returned but the cleanup has not run yet, so the bound
	// variables still hold what the loader wrote.
	for _, k := range config.Keys() {
		if k.Flag == "" {
			continue
		}
		b, ok := configFlagBindings[k.Flag]
		if !ok {
			t.Errorf("%s: no binding", k.Name)
			continue
		}
		want := k.Get(res.Config())
		if got := b.get(); !reflect.DeepEqual(got, want) {
			t.Errorf("%s: the variable --%s is bound to holds %#v, the resolved config says %#v",
				k.Name, k.Flag, got, want)
		}
	}
}

// TestConfigFileSectionsReachRuntime is the consumer-visible half: for each
// formerly dead section, an assertion made through the expression the run
// path itself uses. `mode`, `storage.*`, `nats.*`, `http.*`, `grpc.*`,
// `worker.*` and `telemetry.*` were parsed, validated and reported — and
// consumed by nothing (#808).
func TestConfigFileSectionsReachRuntime(t *testing.T) {
	dataDir := t.TempDir()
	yaml := fmt.Sprintf(`mode: coordinator
storage:
  type: file
  data_dir: %s
nats:
  port: 4555
  cluster_id: from-file
  store_dir: /file/nats/store
  leaf_remotes:
    - nats://leaf-one:4222
http:
  addr: ":18080"
grpc:
  addr: ":19090"
worker:
  max_concurrent: 9
  spill_dir: /file/spill
telemetry:
  endpoint: otel-from-file:4317
  sample_rate: 0.25
query_limits:
  max_scan_bytes: 777
query:
  intermediate_ttl: 4h
`, dataDir)
	path := filepath.Join(t.TempDir(), "wadjet.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := resolveThroughTheRealCommand(t, []string{"--config", path}); err != nil {
		t.Fatal(err)
	}

	// mode — the expression `serve` dispatches on.
	if got := serveMode(); got != "coordinator" {
		t.Errorf("serveMode() = %q, want %q — the mode switch still reads the flag variable", got, "coordinator")
	}

	// storage — the store the run functions actually build.
	store, err := newStore()
	if err != nil {
		t.Fatalf("newStore: %v", err)
	}
	if _, ok := store.(interface{ Root() string }); !ok {
		// FileStore is what `storage.type: file` must produce; the exact
		// accessor differs, so fall back to the type name.
		if name := fmt.Sprintf("%T", store); !strings.Contains(name, "FileStore") {
			t.Errorf("newStore() built %s; `storage.type: file` did not reach it", name)
		}
	}

	// nats — the embedded server configuration both serve modes build.
	nats := natsServerConfig()
	if nats.Port != 4555 || nats.ClusterID != "from-file" || nats.StoreDir != "/file/nats/store" {
		t.Errorf("natsServerConfig() = {port %d, cluster %q, store %q}; the nats: section did not reach it",
			nats.Port, nats.ClusterID, nats.StoreDir)
	}
	if len(nats.LeafRemotes) != 1 || nats.LeafRemotes[0] != "nats://leaf-one:4222" {
		t.Errorf("natsServerConfig() leaf remotes = %v", nats.LeafRemotes)
	}

	cfg := effectiveConfig()
	if httpAddr != ":18080" || cfg.HTTP.Addr != ":18080" {
		t.Errorf("http.addr: server sees %q, resolved %q", httpAddr, cfg.HTTP.Addr)
	}
	if grpcAddr != ":19090" || cfg.GRPC.Addr != ":19090" {
		t.Errorf("grpc.addr: server sees %q, resolved %q", grpcAddr, cfg.GRPC.Addr)
	}
	if maxConcurrent != 9 || spillDir != "/file/spill" {
		t.Errorf("worker: max_concurrent %d, spill_dir %q", maxConcurrent, spillDir)
	}
	if cfg.Telemetry.Endpoint != "otel-from-file:4317" || cfg.Telemetry.SampleRate != 0.25 {
		t.Errorf("telemetry: %+v", cfg.Telemetry)
	}
	if queryIntermediateTTL != 4*time.Hour {
		t.Errorf("query.intermediate_ttl = %v, want 4h", queryIntermediateTTL)
	}
	global, _ := cfg.EffectiveQueryLimits()
	if global == nil || global.MaxScanBytes != 777 {
		t.Errorf("query_limits did not reach the cost guard: %+v", global)
	}
}

// TestNATSTLSAgreesWithTheResolvedConfig is the anti-drift pin over the one
// place the flag/env/file walk still exists by hand: resolveNATSTLSPaths is
// #827's security control and keeps its own gates, so the loader must not be
// able to disagree with it in any presence cell.
func TestNATSTLSAgreesWithTheResolvedConfig(t *testing.T) {
	for cell := 0; cell < 8; cell++ {
		hasFile, hasEnv, hasFlag := cell&1 != 0, cell&2 != 0, cell&4 != 0
		t.Run(fmt.Sprintf("file=%v,env=%v,flag=%v", hasFile, hasEnv, hasFlag), func(t *testing.T) {
			var args []string
			if hasFile {
				path := filepath.Join(t.TempDir(), "wadjet.yaml")
				body := "nats:\n  tls_cert: /file/cert.pem\n  tls_key: /file/key.pem\n  tls_ca: /file/ca.pem\n"
				if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
					t.Fatal(err)
				}
				args = append(args, "--config", path)
			}
			if hasEnv {
				t.Setenv("WADJET_NATS_TLS_CERT", "/env/cert.pem")
				t.Setenv("WADJET_NATS_TLS_KEY", "/env/key.pem")
				t.Setenv("WADJET_NATS_TLS_CA", "/env/ca.pem")
			} else {
				t.Setenv("WADJET_NATS_TLS_CERT", "")
				t.Setenv("WADJET_NATS_TLS_KEY", "")
				t.Setenv("WADJET_NATS_TLS_CA", "")
			}
			if hasFlag {
				args = append(args,
					"--nats-tls-cert=/flag/cert.pem",
					"--nats-tls-key=/flag/key.pem",
					"--nats-tls-ca=/flag/ca.pem")
			}

			res, err := resolveThroughTheRealCommand(t, args)
			if err != nil {
				t.Fatal(err)
			}
			fileCfg, err := loadConfigForNATSTLS()
			if err != nil {
				t.Fatalf("loadConfigForNATSTLS: %v", err)
			}
			cert, key, ca, err := resolveNATSTLSPaths(fileCfg)
			if err != nil {
				t.Fatalf("resolveNATSTLSPaths: %v", err)
			}
			n := res.Config().NATS
			if cert != n.TLSCert || key != n.TLSKey || ca != n.TLSCA {
				t.Fatalf("the NATS TLS resolver and the loader disagree: "+
					"resolver (%q, %q, %q), loader (%q, %q, %q)",
					cert, key, ca, n.TLSCert, n.TLSKey, n.TLSCA)
			}
		})
	}
}

// TestAnUnparseableConfigFileStopsTheCommand: the loader propagates a parse
// failure on EVERY command, not only the ones that happened to load the file
// again for another reason (#802's doctrine, #827's tier, #808's loader).
func TestAnUnparseableConfigFileStopsTheCommand(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wadjet.yaml")
	// YAML forbids a tab in indentation — the commonest way a hand-edited
	// config becomes unreadable.
	body := "storage:\n  bucket: prod\n\tregion: us-east-2\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := resolveThroughTheRealCommand(t, []string{"--config", path})
	if err == nil {
		t.Fatal("an unparseable config file was accepted; the process would run on values " +
			"the operator did not write")
	}
	if !strings.Contains(err.Error(), path) {
		t.Fatalf("the error does not name the file: %v", err)
	}
}
