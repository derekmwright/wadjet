package main

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/config"
)

// The BOUNDARY of the precedence repair, as a fixture.
//
// #808's root cause was not one bad tier, it was several independent
// hand-rolled reads of the environment, each with its own convention, none
// of which any gate knew about. The loader folded the configuration ones in.
// A gate that only asserts the loader is right cannot notice a NEW
// hand-rolled read appearing beside it, which is how this defect got here in
// the first place — so the boundary is enumerated and asserted, and a
// `os.Getenv("WADJET_…")` added to cmd/wadjet fails this test until it is
// either a registry key or an explicitly reasoned exemption.
//
// The exemptions are not configuration in the sense ADR-0029 governs: they
// are runtime/debug knobs with no config-file meaning, per-invocation
// credentials, or flags LOCAL to a subcommand (the registry's flag tier is
// the ROOT command's persistent flags, by construction — a process-wide
// resolution cannot be built from a flag only one subcommand has).
var handRolledEnvExemptions = map[string]string{
	// Runtime and debug knobs. No config-file meaning; several must be read
	// before any config exists (the log sink and the GC envelope are set up
	// before the loader could have run).
	"WADJET_SYNC_LOG":               "log sink selection, read before the logger exists",
	"WADJET_GOGC":                   "GC mode, applied with GOMEMLIMIT before any query surface",
	"WADJET_BG_GC_INTERVAL":         "background GC cadence, debug knob",
	"WADJET_BLOCK_PROFILE_RATE":     "runtime block profiler, benchmark/profiling deploys",
	"WADJET_MUTEX_PROFILE_FRACTION": "runtime mutex profiler, benchmark/profiling deploys",
	"WADJET_HEAP_DUMP_INTERVAL":     "opt-in heap dumper for OOM debugging",
	"WADJET_HEAP_DUMP_DIR":          "opt-in heap dumper output directory",
	"WADJET_DYNAMIC_FILTERS":        "execution kill switch (internal/optswitch convention)",

	// Per-invocation credentials and provider selection for embed(). These
	// are secrets and endpoints for an outbound API, not server settings;
	// putting them in a config file is the thing you do NOT want.
	"WADJET_MCP_API_KEY":       "per-invocation MCP credential, paired with the local --api-key flag",
	"WADJET_OPENAI_API_KEY":    "embed() provider credential",
	"WADJET_VOYAGE_API_KEY":    "embed() provider credential",
	"WADJET_VOYAGE_INPUT_TYPE": "embed() provider parameter",
	"WADJET_OLLAMA_URL":        "embed() provider endpoint",
	"WADJET_EMBED_PROVIDER":    "embed() provider selection",
	"WADJET_EMBED_MODEL":       "embed() model selection",
	"WADJET_EMBED_DIM":         "embed() dimension selection",

	// Flags LOCAL to `serve`, not persistent flags of the root command. The
	// resolver's flag tier is the root's persistent set; reaching a
	// subcommand's own flags means a per-command resolution, which is a
	// structural change and not this pass's. Their env reads keep the old
	// "environment overrides the flag" convention, which DIVERGES from
	// ADR-0029 and is recorded here rather than left unremarked.
	"WADJET_CATALOG_SNAPSHOT_PREFIX":   "local flag of `serve` (--catalog-snapshot-s3-prefix); env still overrides the flag",
	"WADJET_CATALOG_SNAPSHOT_INTERVAL": "local flag of `serve` (--catalog-snapshot-interval); env still overrides the flag",
}

var literalEnvRead = regexp.MustCompile(`os\.(?:Getenv|LookupEnv)\("(WADJET_[A-Z0-9_]+)"\)`)

// TestEveryHandRolledEnvReadIsAccountedFor: every literal WADJET_* read in
// cmd/wadjet is either a registry key (resolved by the loader) or an
// exemption with a reason.
func TestEveryHandRolledEnvReadIsAccountedFor(t *testing.T) {
	inRegistry := map[string]bool{}
	for _, name := range config.EnvNames() {
		inRegistry[name] = true
	}

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	found := map[string][]string{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(filepath.Clean(name))
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range literalEnvRead.FindAllStringSubmatch(string(src), -1) {
			found[m[1]] = append(found[m[1]], name)
		}
	}
	if len(found) == 0 {
		t.Fatal("no WADJET_* reads found at all; the scan is broken, not the code")
	}

	for v, files := range found {
		switch {
		case inRegistry[v]:
			t.Errorf("%s is a registry key AND is read by hand in %v — two tiers, two conventions, "+
				"which is exactly #808. Delete the hand-rolled read; the loader resolves it.",
				v, files)
		case handRolledEnvExemptions[v] != "":
			// Accounted for.
		default:
			t.Errorf("%s is read by hand in %v and is neither a registry key nor an exemption.\n"+
				"Either add it to the configuration registry (internal/config/registry.go), so it "+
				"resolves as explicit flag > env > file > default like everything else, or add it to "+
				"handRolledEnvExemptions with the reason it is not configuration (ADR-0029).",
				v, files)
		}
	}

	// The other direction: an exemption whose read is gone is stale.
	var stale []string
	for v := range handRolledEnvExemptions {
		if len(found[v]) == 0 {
			stale = append(stale, v)
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Errorf("these exemptions name variables nothing reads any more: %v — delete them", stale)
	}
}

// TestEnableAlertsResolvesLikeEverythingElse: WADJET_ENABLE_ALERTS used to
// beat the flag unconditionally, which is the opposite of every other tier.
// It is a registry key now, so an explicit --enable-alerts=false wins over
// the environment and `alerts:` in the config file works.
func TestEnableAlertsResolvesLikeEverythingElse(t *testing.T) {
	t.Run("env beats the flag default", func(t *testing.T) {
		t.Setenv("WADJET_ENABLE_ALERTS", "1")
		res, err := resolveThroughTheRealCommand(t, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !res.Config().Alerts.Enabled || !enableAlerts {
			t.Fatalf("alerts.enabled = %v (variable %v), want true",
				res.Config().Alerts.Enabled, enableAlerts)
		}
	})

	t.Run("an explicit flag beats the env", func(t *testing.T) {
		t.Setenv("WADJET_ENABLE_ALERTS", "1")
		res, err := resolveThroughTheRealCommand(t, []string{"--enable-alerts=false"})
		if err != nil {
			t.Fatal(err)
		}
		if res.Config().Alerts.Enabled || enableAlerts {
			t.Fatal("--enable-alerts=false lost to WADJET_ENABLE_ALERTS=1")
		}
		if got := res.Source("alerts.enabled"); got != config.SourceFlag {
			t.Fatalf("source = %q, want %q", got, config.SourceFlag)
		}
	})

	t.Run("the config file reaches it", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "wadjet.yaml")
		if err := os.WriteFile(path, []byte("alerts:\n  enabled: true\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		res, err := resolveThroughTheRealCommand(t, []string{"--config", path})
		if err != nil {
			t.Fatal(err)
		}
		if !res.Config().Alerts.Enabled || !enableAlerts {
			t.Fatal("`alerts: enabled: true` did not reach the run functions")
		}
	})
}
