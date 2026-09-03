package config

import (
	"fmt"
	"os"
)

// Source names the tier a resolved value came from.
type Source string

const (
	// SourceDefault is the built-in default — the flag's registered default
	// where the key has a flag, DefaultConfig()'s value otherwise.
	SourceDefault Source = "default"
	// SourceFile is the YAML configuration file.
	SourceFile Source = "file"
	// SourceEnv is a WADJET_* environment variable.
	SourceEnv Source = "env"
	// SourceFlag is a command-line flag the operator actually typed.
	SourceFlag Source = "flag"
	// SourceAdmin is a runtime change through the admin API.
	SourceAdmin Source = "admin"
)

// FlagValues is the resolver's view of a parsed command line.
//
// Value returns the variable the flag is bound to, which holds the flag's
// DEFAULT until the operator sets it — so a single accessor serves both the
// default tier and the explicit-flag tier, and Changed is what separates
// them. That is the whole of ADR-0029's "a flag counts only if Changed".
type FlagValues interface {
	// Changed reports whether the flag was set on the command line.
	Changed(flag string) bool
	// Value returns the flag's current value (its default when not
	// Changed), or ok=false when the resolver's flag name is unknown.
	Value(flag string) (any, bool)
	// Default returns the flag's registered default, snapshotted before the
	// command line was parsed. It is the DEFAULT TIER for every key that
	// has a flag: the binary runs on the flag's default today, and
	// DefaultConfig()'s value is not always the same one (DefaultConfig
	// sets storage.access_key to "minioadmin" where --access-key defaults
	// to "" and means "auto-detect from env/IAM").
	Default(flag string) (any, bool)
}

// Inputs are the tiers a resolution draws from.
type Inputs struct {
	// File is the config file merged over DefaultConfig() — i.e. Load()'s
	// result — or nil when no --config was given. Sections outside the
	// registry (auth, abac_policies, per-role query limits) are carried
	// through from here unchanged.
	File *Config
	// FileKeys names the registry keys the file actually set. Use
	// LoadWithKeys to build it; a key absent here never wins its tier even
	// when File carries a merged default for it.
	FileKeys map[string]bool
	// Lookup reads an environment variable. nil means os.LookupEnv.
	Lookup func(string) (string, bool)
	// Flags is the parsed command line. nil means no flag tier, in which
	// case DefaultConfig() supplies the default tier.
	Flags FlagValues
}

// Resolution is a fully resolved configuration plus the tier each key came
// from.
type Resolution struct {
	cfg     *Config
	sources map[string]Source
}

// Config returns the resolved configuration.
func (r *Resolution) Config() *Config { return r.cfg }

// Source returns the tier key came from, or SourceDefault for an unknown key.
func (r *Resolution) Source(key string) Source {
	if s, ok := r.sources[key]; ok {
		return s
	}
	return SourceDefault
}

// Sources returns a copy of the per-key source map.
func (r *Resolution) Sources() map[string]Source {
	out := make(map[string]Source, len(r.sources))
	for k, v := range r.sources {
		out[k] = v
	}
	return out
}

// withConfig returns a resolution over cfg carrying r's sources, marking
// every key whose value changed as coming from src.
func (r *Resolution) withConfig(cfg *Config, src Source) *Resolution {
	out := &Resolution{cfg: cfg, sources: r.Sources()}
	for _, k := range keys {
		if !equalValues(k, k.Get(r.cfg), k.Get(cfg)) {
			out.sources[k.Name] = src
		}
	}
	return out
}

func equalValues(k Key, a, b any) bool {
	if k.Kind == KindStringSlice {
		as, bs := a.([]string), b.([]string)
		if len(as) != len(bs) {
			return false
		}
		for i := range as {
			if as[i] != bs[i] {
				return false
			}
		}
		return true
	}
	return a == b
}

// Resolve applies the precedence order settled in ADR-0029:
//
//	explicit flag > environment variable > config file > default
//
// A flag counts only when FlagValues reports it Changed; a flag's DEFAULT
// never beats an environment variable or a config-file value. An empty
// string never overrides a lower tier, on any tier — that is how the
// environment layer has always behaved, and applying it to flags too means
// `--nats-tls-cert=""` reads as "unset" rather than as "explicitly blank",
// which is what every string flag here means by an empty value.
func Resolve(in Inputs) *Resolution {
	lookup := in.Lookup
	if lookup == nil {
		lookup = os.LookupEnv
	}

	var out Config
	if in.File != nil {
		out = *in.File
	} else {
		out = DefaultConfig()
	}

	hasFlags := in.Flags != nil
	sources := make(map[string]Source, len(keys))
	for _, k := range keys {
		// Tier 4 — default.
		val := defaultValue(k)
		src := SourceDefault
		if k.Flag != "" && hasFlags {
			if d, ok := in.Flags.Default(k.Flag); ok {
				val = d
			}
		}

		// Tier 1 — a flag the operator actually typed.
		if k.Flag != "" && hasFlags && in.Flags.Changed(k.Flag) {
			if v, ok := in.Flags.Value(k.Flag); ok && !isBlankString(k, v) {
				val, src = v, SourceFlag
			}
		}

		if src != SourceFlag {
			// Tier 2 — environment.
			if v, ok := envValue(k, lookup); ok {
				val, src = v, SourceEnv
			} else if in.File != nil && in.FileKeys[k.Name] {
				// Tier 3 — config file.
				if fv := k.Get(in.File); !isBlankString(k, fv) {
					val, src = fv, SourceFile
				}
			}
		}

		k.Set(&out, val)
		sources[k.Name] = src
	}

	return &Resolution{cfg: &out, sources: sources}
}

// DeferredKeysSet returns every DEFERRED key this resolution took from a
// tier other than the default — that is, every key an operator configured
// that no runtime consumer reads.
//
// Rule 11's "never left half-live" is what this serves. Parsing such a
// section silently and refusing it only on the admin WRITE path leaves the
// operator's startup configuration accepted and inert, which is the exact
// defect #808 was filed for; the caller turns this into a refusal that
// names the keys.
func (r *Resolution) DeferredKeysSet() []string {
	var out []string
	for _, k := range keys {
		if k.Deferred && r.Source(k.Name) != SourceDefault {
			out = append(out, k.Name)
		}
	}
	return out
}

// DeferredReason returns the recorded mechanism for a deferred key.
func DeferredReason(name string) string {
	if k, ok := KeyByName(name); ok {
		return k.DeferredWhy
	}
	return ""
}

// isBlankString reports whether a string-kind key's value is empty. Empty
// strings never override a lower tier (see Resolve's doc comment).
func isBlankString(k Key, v any) bool {
	return k.Kind == KindString && v.(string) == ""
}

func defaultValue(k Key) any {
	d := DefaultConfig()
	return k.Get(&d)
}

func envValue(k Key, lookup func(string) (string, bool)) (any, bool) {
	if k.Env == "" {
		return nil, false
	}
	s, ok := lookup(k.Env)
	if !ok || s == "" {
		return nil, false
	}
	return k.ParseEnv(s)
}

// LoadWithKeys reads a YAML config file and returns the config merged over
// the defaults (exactly Load's result) together with the set of registry
// keys the file actually set.
//
// Presence is decided by unmarshalling a SECOND time into a zero Config and
// asking whether the key came out non-zero. A key written into the file with
// its type's zero value is therefore indistinguishable from an absent one —
// the same convention the environment tier has always used for an empty
// string, and the reason Resolve treats both alike.
func LoadWithKeys(path string) (*Config, map[string]bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("reading config file: %w", err)
	}

	merged := DefaultConfig()
	if err := strictUnmarshal(data, &merged); err != nil {
		return nil, nil, err
	}

	var bare Config
	if err := strictUnmarshal(data, &bare); err != nil {
		return nil, nil, err
	}

	set := make(map[string]bool, len(keys))
	for _, k := range keys {
		if !k.IsZero(k.Get(&bare)) {
			set[k.Name] = true
		}
	}
	return &merged, set, nil
}
