package main

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"

	"github.com/derekmwright/wadjet/internal/config"
)

// The configuration loader.
//
// ADR-0029 settles the order — explicit flag > env > file > default — and a
// flag counts only when the operator actually typed it. Before this arc the
// real order was "flag, even at its default, beats a config file that
// reached three of eleven sections, with the environment nowhere": the
// measured census had the environment layer winning 0 of 24 cells, and 12 of
// 24 were broken outright (#808).
//
// The repair is a two-pass load in the root command's PersistentPreRunE
// rather than a Changed() guard at each of the twenty-four binding points.
// One function scales, revives the eight dead file sections in the same
// motion, and — because it writes the resolved values BACK into the
// variables the flags are bound to — leaves no consumer able to read a value
// the resolution disagrees with. TestEveryConfigKeyReachesItsFlagVariable
// asserts that write-back is total over the registry.

// configFlagBinding reads and writes the package-level variable a persistent
// flag is bound to. get supplies the resolver's flag tier; set is the
// write-back.
type configFlagBinding struct {
	get func() any
	set func(any)
}

// configFlagBindings maps a registry key's flag name to its bound variable.
// TestConfigFlagBindingsMatchTheRegistry asserts, in both directions, that
// this table covers exactly the registry keys that name a flag and that
// every flag named here is a real persistent flag of the root command.
var configFlagBindings = map[string]configFlagBinding{
	"mode":         {func() any { return mode }, func(v any) { mode = v.(string) }},
	"storage-type": {func() any { return storageType }, func(v any) { storageType = v.(string) }},
	"data-dir":     {func() any { return dataDir }, func(v any) { dataDir = v.(string) }},
	"endpoint":     {func() any { return endpoint }, func(v any) { endpoint = v.(string) }},
	"access-key":   {func() any { return accessKey }, func(v any) { accessKey = v.(string) }},
	"secret-key":   {func() any { return secretKey }, func(v any) { secretKey = v.(string) }},
	"bucket":       {func() any { return bucket }, func(v any) { bucket = v.(string) }},
	"ssl":          {func() any { return useSSL }, func(v any) { useSSL = v.(bool) }},
	"region":       {func() any { return s3Region }, func(v any) { s3Region = v.(string) }},

	"storage-circuit-threshold": {func() any { return circuitThreshold }, func(v any) { circuitThreshold = v.(int) }},
	"storage-circuit-reset":     {func() any { return circuitResetTimeout }, func(v any) { circuitResetTimeout = v.(time.Duration) }},
	"storage-circuit-request-timeout": {
		func() any { return circuitRequestTimeout },
		func(v any) { circuitRequestTimeout = v.(time.Duration) },
	},

	"nats-port":      {func() any { return natsPort }, func(v any) { natsPort = v.(int) }},
	"nats-url":       {func() any { return natsURL }, func(v any) { natsURL = v.(string) }},
	"nats-store-dir": {func() any { return natsStoreDir }, func(v any) { natsStoreDir = v.(string) }},
	"cluster-id":     {func() any { return clusterID }, func(v any) { clusterID = v.(string) }},
	"leaf-remote":    {func() any { return leafRemotes }, func(v any) { leafRemotes = v.([]string) }},
	"nats-tls-cert":  {func() any { return natsTLSCert }, func(v any) { natsTLSCert = v.(string) }},
	"nats-tls-key":   {func() any { return natsTLSKey }, func(v any) { natsTLSKey = v.(string) }},
	"nats-tls-ca":    {func() any { return natsTLSCA }, func(v any) { natsTLSCA = v.(string) }},

	"http-addr": {func() any { return httpAddr }, func(v any) { httpAddr = v.(string) }},
	"grpc-addr": {func() any { return grpcAddr }, func(v any) { grpcAddr = v.(string) }},

	"max-concurrent": {func() any { return maxConcurrent }, func(v any) { maxConcurrent = v.(int) }},
	"cache-bytes":    {func() any { return cacheBytes }, func(v any) { cacheBytes = v.(int64) }},
	"memory-budget":  {func() any { return memoryBudget }, func(v any) { memoryBudget = v.(int64) }},
	"spill-dir":      {func() any { return spillDir }, func(v any) { spillDir = v.(string) }},
	"result-store":   {func() any { return resultStoreBytes }, func(v any) { resultStoreBytes = v.(int64) }},

	"enable-alerts": {func() any { return enableAlerts }, func(v any) { enableAlerts = v.(bool) }},

	"geoip-city": {func() any { return geoipCityDB }, func(v any) { geoipCityDB = v.(string) }},
	"geoip-asn":  {func() any { return geoipASNDB }, func(v any) { geoipASNDB = v.(string) }},

	"query-intermediate-ttl":   {func() any { return queryIntermediateTTL }, func(v any) { queryIntermediateTTL = v.(time.Duration) }},
	"query-intermediate-sweep": {func() any { return queryIntermediateGC }, func(v any) { queryIntermediateGC = v.(time.Duration) }},

	"otel-endpoint": {func() any { return otelEndpoint }, func(v any) { otelEndpoint = v.(string) }},
	"otel-insecure": {func() any { return otelInsecure }, func(v any) { otelInsecure = v.(bool) }},
}

// configFlagDefaults is the flag defaults snapshotted at registration time,
// before any parsing.
var configFlagDefaults map[string]any

func snapshotConfigFlagDefaults() {
	configFlagDefaults = make(map[string]any, len(configFlagBindings))
	for name, b := range configFlagBindings {
		configFlagDefaults[name] = b.get()
	}
}

// cobraFlagValues presents the root command's persistent flags to the
// resolver.
type cobraFlagValues struct {
	cmd *cobra.Command
}

func (c cobraFlagValues) Changed(flag string) bool {
	if c.cmd == nil {
		return false
	}
	f := c.cmd.Root().PersistentFlags().Lookup(flag)
	return f != nil && f.Changed
}

func (c cobraFlagValues) Value(flag string) (any, bool) {
	b, ok := configFlagBindings[flag]
	if !ok {
		return nil, false
	}
	return b.get(), true
}

func (c cobraFlagValues) Default(flag string) (any, bool) {
	v, ok := configFlagDefaults[flag]
	return v, ok
}

// resolvedConfig holds the resolution the whole process runs on.
var resolvedConfig atomic.Pointer[config.Resolution]

// resolveConfiguration is the root command's PersistentPreRunE body: load
// the config file (propagating a parse failure), resolve every registry key
// against ADR-0029's order, publish the resolution, and write the resolved
// values back into the variables the flags are bound to.
//
// A config file that does not parse is a startup ERROR on every command.
// #802 settled that doctrine for the auth block ("an unreadable config file
// silently started a server with NO authentication at all — that now stops
// the process with the reason") and #827 extended it to the NATS TLS tier;
// it holds for the whole file, because every section now reaches runtime and
// a dropped parse error is a process running on values the operator did not
// write.
func resolveConfiguration(cmd *cobra.Command) error {
	var fileCfg *config.Config
	var fileKeys map[string]bool
	if configFile != "" {
		var err error
		fileCfg, fileKeys, err = config.LoadWithKeys(configFile)
		if err != nil {
			return fmt.Errorf("loading config file %q: %w", configFile, err)
		}
	}

	res := config.Resolve(config.Inputs{
		File:     fileCfg,
		FileKeys: fileKeys,
		Flags:    cobraFlagValues{cmd: cmd},
	})

	// A DEFERRED key the operator actually set is a startup error naming it.
	// Accepting a `parquet:` block at startup and refusing it only on the
	// admin write path would leave the operator's configuration accepted and
	// inert — the silent-inert shape #808 was filed for, and the thing Rule
	// 11's "never left half-live" forbids.
	if set := res.DeferredKeysSet(); len(set) > 0 {
		return fmt.Errorf(
			"configuration sets %s, which no part of the running process reads (%s). "+
				"Remove the setting; it is reported by GET /v1/admin/config with "+
				"\"reaches_runtime\": false and refused by every write path, and "+
				"accepting it here would be the silent no-op this refusal exists to "+
				"prevent",
			strings.Join(set, ", "), config.DeferredReason(set[0]))
	}

	publishResolvedConfig(res)
	return nil
}

// publishResolvedConfig stores the resolution and syncs every bound flag
// variable to it, so a consumer that still reads a flag variable reads the
// resolved value.
func publishResolvedConfig(res *config.Resolution) {
	resolvedConfig.Store(res)
	cfg := res.Config()
	for _, k := range config.Keys() {
		if k.Flag == "" {
			continue
		}
		if b, ok := configFlagBindings[k.Flag]; ok {
			b.set(k.Get(cfg))
		}
	}
}

// effectiveResolution returns the resolution the process is running on.
//
// Callers that never went through the root command (embedded and test
// callers invoking a RunE directly) get a DEFAULT-AND-ENVIRONMENT
// resolution: the fallback passes a cobraFlagValues with no command, so
// Changed is always false, Resolve never reads a flag value, and the result
// is the registered flag defaults (or config.DefaultConfig() where a key has
// no flag) with the environment applied over them. No config file and no
// flag tier, because neither has been parsed. It never returns nil, so a
// consumer never has to ask whether configuration happened.
func effectiveResolution() *config.Resolution {
	if res := resolvedConfig.Load(); res != nil {
		return res
	}
	return config.Resolve(config.Inputs{Flags: cobraFlagValues{}})
}

// effectiveConfig returns the configuration the process is running on.
func effectiveConfig() *config.Config { return effectiveResolution().Config() }

// serveMode is the run mode `serve` dispatches on. It reads the RESOLVED
// mode: before #808's fix the switch read the flag variable, so a `mode:` in
// the config file or a WADJET_MODE in the environment was parsed, validated,
// reported by GET /v1/admin/config — and never dispatched on.
func serveMode() string { return effectiveConfig().Mode }
