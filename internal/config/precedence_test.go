package config

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
)

// fakeFlags is a parsed command line for the model arm of the census. The
// real arm lives in cmd/wadjet and drives the actual *cobra.Command, so a
// model that drifts from the binary cannot carry the gate on its own.
type fakeFlags struct {
	def     map[string]any
	val     map[string]any
	changed map[string]bool
}

func (f fakeFlags) Changed(flag string) bool { return f.changed[flag] }

func (f fakeFlags) Default(flag string) (any, bool) {
	v, ok := f.def[flag]
	return v, ok
}

func (f fakeFlags) Value(flag string) (any, bool) {
	if v, ok := f.val[flag]; ok {
		return v, true
	}
	v, ok := f.def[flag]
	return v, ok
}

// tierValues are one key's three sentinel values, one per settable tier.
type tierValues struct {
	file    any
	env     any
	envText string
	flag    any
}

func sentinels(k Key) tierValues {
	switch k.Kind {
	case KindString:
		return tierValues{
			file:    "file-" + k.Name,
			env:     "env-" + k.Name,
			envText: "env-" + k.Name,
			flag:    "flag-" + k.Name,
		}
	case KindInt:
		return tierValues{file: 11, env: 22, envText: "22", flag: 33}
	case KindInt64:
		return tierValues{file: int64(111), env: int64(222), envText: "222", flag: int64(333)}
	case KindBool:
		// A bool cannot carry three distinguishable values. The environment
		// sentinel is FALSE against the file's TRUE so that the file+env
		// cell moves the VALUE and not only the reported source; the other
		// cells ride on Source, which is what the admin endpoint publishes.
		return tierValues{file: true, env: false, envText: "false", flag: true}
	case KindFloat64:
		return tierValues{file: 0.11, env: 0.22, envText: "0.22", flag: 0.33}
	case KindDuration:
		return tierValues{file: 11 * time.Second, env: 22 * time.Second, envText: "22s", flag: 33 * time.Second}
	case KindStringSlice:
		return tierValues{
			file:    []string{"file-a", "file-b"},
			env:     []string{"env-a"},
			envText: "env-a",
			flag:    []string{"flag-a", "flag-b"},
		}
	}
	panic("unhandled kind " + k.Kind.String())
}

// TestPrecedenceCensus is the loader-chain arm of the census: every registry
// key, every presence combination of (config file, environment, explicit
// flag), asserting BOTH the winning value and the reported source.
//
// The order under test is ADR-0029's: explicit flag > env > file > default.
// Before this arc the real order was "flag, even at its default, beats a
// config file that reached only three sections, with the environment
// nowhere" — the measured census on 1f0974d5 had the environment layer
// winning 0 of 24 cells (#808).
func TestPrecedenceCensus(t *testing.T) {
	for _, k := range Keys() {
		k := k
		sv := sentinels(k)
		for cell := 0; cell < 8; cell++ {
			hasFile := cell&1 != 0
			hasEnv := cell&2 != 0
			hasFlag := cell&4 != 0

			// A key with no env var cannot take the env tier; a key with no
			// flag cannot take the flag tier. Those cells collapse onto the
			// next tier down, which is itself the property under test.
			envUsable := hasEnv && k.Env != ""
			flagUsable := hasFlag && k.Flag != ""

			name := fmt.Sprintf("%s/file=%v,env=%v,flag=%v", k.Name, hasFile, hasEnv, hasFlag)
			t.Run(name, func(t *testing.T) {
				fileCfg := DefaultConfig()
				fileKeys := map[string]bool{}
				if hasFile {
					k.Set(&fileCfg, sv.file)
					fileKeys[k.Name] = true
				}

				env := map[string]string{}
				if envUsable {
					env[k.Env] = sv.envText
				}

				flags := fakeFlags{
					def:     map[string]any{},
					val:     map[string]any{},
					changed: map[string]bool{},
				}
				flagDefault := defaultValue(k)
				if k.Flag != "" {
					flags.def[k.Flag] = flagDefault
					if flagUsable {
						flags.val[k.Flag] = sv.flag
						flags.changed[k.Flag] = true
					}
				}

				res := Resolve(Inputs{
					File:     &fileCfg,
					FileKeys: fileKeys,
					Lookup:   lookupFrom(env),
					Flags:    flags,
				})

				wantValue, wantSource := flagDefault, SourceDefault
				switch {
				case flagUsable:
					wantValue, wantSource = sv.flag, SourceFlag
				case envUsable:
					wantValue, wantSource = sv.env, SourceEnv
				case hasFile:
					wantValue, wantSource = sv.file, SourceFile
				}

				got := k.Get(res.Config())
				if !reflect.DeepEqual(got, wantValue) {
					t.Errorf("%s: got %#v, want %#v (%s tier)", k.Name, got, wantValue, wantSource)
				}
				if src := res.Source(k.Name); src != wantSource {
					t.Errorf("%s: source %q, want %q", k.Name, src, wantSource)
				}
			})
		}
	}
}

// TestEnvironmentTierWinsOverAFlagLeftAtItsDefault is #808's headline in one
// assertion: a flag's DEFAULT never beats an environment variable.
func TestEnvironmentTierWinsOverAFlagLeftAtItsDefault(t *testing.T) {
	for _, k := range Keys() {
		if k.Env == "" || k.Flag == "" {
			continue
		}
		k := k
		t.Run(k.Name, func(t *testing.T) {
			sv := sentinels(k)
			def := defaultValue(k)
			flags := fakeFlags{
				def:     map[string]any{k.Flag: def},
				val:     map[string]any{},
				changed: map[string]bool{}, // the operator typed nothing
			}
			res := Resolve(Inputs{
				Lookup: lookupFrom(map[string]string{k.Env: sv.envText}),
				Flags:  flags,
			})
			if src := res.Source(k.Name); src != SourceEnv {
				t.Fatalf("%s resolved from %q, want %q — the flag's default beat the environment (#808)",
					k.Name, src, SourceEnv)
			}
			if got := k.Get(res.Config()); !reflect.DeepEqual(got, sv.env) {
				t.Fatalf("%s = %#v, want the environment's %#v", k.Name, got, sv.env)
			}
		})
	}
}

// TestBlankValuesNeverOverrideALowerTier pins the convention Resolve states:
// an empty string does not override, on any tier. It is what makes
// `--nats-tls-cert=""` read as "unset" rather than as "explicitly blank",
// and it is why the hand-rolled NATS TLS resolver and the loader cannot
// disagree.
func TestBlankValuesNeverOverrideALowerTier(t *testing.T) {
	k, ok := KeyByName("storage.bucket")
	if !ok {
		t.Fatal("storage.bucket left the registry")
	}
	fileCfg := DefaultConfig()
	fileCfg.Storage.Bucket = "from-file"

	res := Resolve(Inputs{
		File:     &fileCfg,
		FileKeys: map[string]bool{"storage.bucket": true},
		Lookup:   lookupFrom(map[string]string{"WADJET_STORAGE_BUCKET": ""}),
		Flags: fakeFlags{
			def:     map[string]any{k.Flag: "wadjet"},
			val:     map[string]any{k.Flag: ""},
			changed: map[string]bool{k.Flag: true},
		},
	})
	if got := res.Config().Storage.Bucket; got != "from-file" {
		t.Fatalf("bucket = %q, want %q — a blank flag and a blank env must both fall through", got, "from-file")
	}
	if src := res.Source("storage.bucket"); src != SourceFile {
		t.Fatalf("source = %q, want %q", src, SourceFile)
	}
}

// TestLoadWithKeysReportsOnlyWhatTheFileSet: the file tier is decided by
// presence, not by the merged value, so a file that omits a key never beats
// an environment variable with the default it inherited from Load.
func TestLoadWithKeysReportsOnlyWhatTheFileSet(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wadjet.yaml")
	yaml := "mode: coordinator\nstorage:\n  bucket: from-file\nquery:\n  intermediate_ttl: 15m\n"
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, set, err := LoadWithKeys(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"mode", "storage.bucket", "query.intermediate_ttl"} {
		if !set[want] {
			t.Errorf("%s: file key not detected", want)
		}
	}
	for _, notWant := range []string{"http.addr", "nats.port", "worker.max_concurrent"} {
		if set[notWant] {
			t.Errorf("%s: reported as set by a file that does not mention it", notWant)
		}
	}
	if cfg.Query.IntermediateTTL != 15*time.Minute {
		t.Errorf("query.intermediate_ttl = %v, want 15m", cfg.Query.IntermediateTTL)
	}
	if cfg.HTTP.Addr != ":8080" {
		t.Errorf("merged view lost the default http.addr: %q", cfg.HTTP.Addr)
	}
}

// TestDeferredKeysAreNamedAndNeverHotReloadable pins the Rule 11 deferral:
// parquet.* resolves and reports like any other key, carries the structural
// mechanism that would be needed to make it live, and is refused by every
// write path rather than left half-live (#808 / #828).
func TestDeferredKeysAreNamedAndNeverHotReloadable(t *testing.T) {
	want := map[string]bool{
		"parquet.compression":      true,
		"parquet.row_group_size":   true,
		"parquet.page_buffer_size": true,
	}
	got := map[string]bool{}
	for _, k := range Keys() {
		if !k.Deferred {
			continue
		}
		got[k.Name] = true
		if k.DeferredWhy == "" {
			t.Errorf("%s is deferred with no mechanism recorded", k.Name)
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("deferred keys = %v, want %v — a deferral was added or removed without updating the pin", got, want)
	}

	cfg := DefaultConfig()
	m := NewManager(&cfg, nil)
	m.SubscribeKeys([]string{"parquet"}, func(ChangeEvent) {})
	for name := range want {
		if m.HotReloadable(name) {
			t.Errorf("%s reports hot-reloadable; a deferred key has no consumer to reach", name)
		}
	}
}

// TestEnvironmentVariableNamesAgreeEverywhere is position 3's gate: the
// registry applyEnvOverrides runs on, the doc comment above it, and
// docs/configuration.md's tables name exactly one set of variables.
//
// The drift this pass found: WADJET_QUERY_MAX_SCAN_BYTES / _ROWS / _FILES
// were read by the code and named in the doc's table but missing from the
// doc comment.
func TestEnvironmentVariableNamesAgreeEverywhere(t *testing.T) {
	registry := EnvNames()
	sort.Strings(registry)

	comment := envNamesInDocComment(t)
	if !reflect.DeepEqual(registry, comment) {
		t.Errorf("applyEnvOverrides' doc comment disagrees with the registry:\n%s",
			diffNameSets("registry", registry, "doc comment", comment))
	}

	doc := envNamesInConfigurationDoc(t)
	if !reflect.DeepEqual(registry, doc) {
		t.Errorf("docs/configuration.md disagrees with the registry:\n%s",
			diffNameSets("registry", registry, "docs/configuration.md", doc))
	}
}

var envVarPattern = regexp.MustCompile(`WADJET_[A-Z0-9_]+`)

// envNamesInDocComment extracts the WADJET_* names listed in the doc comment
// above applyEnvOverrides in this package's source.
func envNamesInDocComment(t *testing.T) []string {
	t.Helper()
	src, err := os.ReadFile("config.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(src)
	start := strings.Index(text, "// Supported variables:")
	if start < 0 {
		t.Fatal("config.go no longer carries a 'Supported variables:' list")
	}
	end := strings.Index(text[start:], "func applyEnvOverrides")
	if end < 0 {
		t.Fatal("config.go: no applyEnvOverrides after the 'Supported variables:' list")
	}
	return uniqueSorted(envVarPattern.FindAllString(text[start:start+end], -1))
}

// envNamesInConfigurationDoc extracts the WADJET_* names from the TABLE ROWS
// of docs/configuration.md's "Environment Variables" section. Prose in that
// section (and every other section, which lists benchmark and debug
// variables that are not configuration) is deliberately out of scope.
func envNamesInConfigurationDoc(t *testing.T) []string {
	t.Helper()
	path := filepath.Join("..", "..", "docs", "configuration.md")
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	inSection := false
	for _, line := range strings.Split(string(src), "\n") {
		if strings.HasPrefix(line, "## ") {
			inSection = strings.TrimSpace(line) == "## Environment Variables"
			continue
		}
		if !inSection || !strings.HasPrefix(strings.TrimSpace(line), "|") {
			continue
		}
		names = append(names, envVarPattern.FindAllString(line, -1)...)
	}
	if len(names) == 0 {
		t.Fatalf("%s: no WADJET_* table rows found under '## Environment Variables'", path)
	}
	return uniqueSorted(names)
}

func uniqueSorted(in []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(in))
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}

func diffNameSets(aName string, a []string, bName string, b []string) string {
	inB := map[string]bool{}
	for _, s := range b {
		inB[s] = true
	}
	inA := map[string]bool{}
	for _, s := range a {
		inA[s] = true
	}
	var sb strings.Builder
	for _, s := range a {
		if !inB[s] {
			fmt.Fprintf(&sb, "  in %s, missing from %s: %s\n", aName, bName, s)
		}
	}
	for _, s := range b {
		if !inA[s] {
			fmt.Fprintf(&sb, "  in %s, missing from %s: %s\n", bName, aName, s)
		}
	}
	return sb.String()
}

func lookupFrom(env map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		v, ok := env[name]
		return v, ok
	}
}
