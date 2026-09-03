# ADR-0029 — Configuration precedence: explicit flag > env > file > default

- **Status**: Accepted (2026-09-03)
- **Supersedes**: nothing. Settles the question #808 raised and #827/#828 are faces of.
- **Related**: ADR-0028 (operational invariants — the breaker keys this ADR makes configurable), #802 (an unreadable config file is a startup error), #827 (the NATS TLS file tier), #828 (the admin config endpoint).

## Context

Wadjet had four configuration tiers and no order.

The measured census on `1f0974d5` — the real binary, run in all eight
presence cells of (default × file × env × flag) for twenty-four settings —
found:

- The environment layer won **0 of 24** cells. Twenty-two of the twenty-eight
  documented `WADJET_*` variables were dead: `applyEnvOverrides` was correct
  and complete, and nothing on the `serve`, `query`, `shell` or `mcp` path
  called it. `docs/configuration.md` carried a warning saying so.
- Six variables *were* live, by a separate hand-rolled `os.Getenv` path (NATS
  TLS, OTEL), using the **opposite** convention: a flag won only when
  non-empty, and the config file was consulted last or not at all.
- The config **file** was dead for most sections. `mode`, `storage.*`,
  `nats.*`, `http.*`, `grpc.*`, `worker.*`, `parquet.*` and `telemetry.*`
  were parsed, validated (`manager.go`), reported (`GET /v1/admin/config`) —
  and consumed by nothing. `switch mode` dispatched on the flag variable.
- Because the flag variable was what every consumer read, a flag's
  **default** silently beat every other tier.

The three consequences are one defect wearing three hats: a deployment
configured through the environment ran on something else (#808); mTLS
material in the YAML produced a plaintext NATS connection with no warning
(#827); and an operator read a configuration that was not the running one,
changed it, and was told it was applied (#828).

## Decision

**Explicit flag > environment variable > config file > built-in default.**

1. **A flag counts only when the operator typed it** (`pflag.Flag.Changed`).
   A flag's default is the *default tier*, not the flag tier, and never beats
   an environment variable or a config-file value.

2. **The default tier is the FLAG's registered default** wherever a key has a
   flag, and `config.DefaultConfig()`'s value otherwise. The two are not
   always the same, and the flag's is what the binary has always run on:
   `DefaultConfig()` sets `storage.access_key` to `minioadmin`, while
   `--access-key` defaults to `""` and means "auto-detect from env/IAM".
   Taking `DefaultConfig()` as the base would have been a silent behaviour
   change of its own.

3. **An empty value never overrides a lower tier, on any tier.** That has
   always been the environment's convention (`if v := os.Getenv(x); v != ""`)
   and it now holds for flags and file keys too, so `--nats-tls-cert=""`
   reads as *unset*. A config-file key written with its type's zero value is
   likewise indistinguishable from absent.

3a. **A key the schema does not define is a startup error naming it**, and so
   is a key with no runtime consumer (decision 8). Silence about a key the
   operator wrote is the defect this ADR exists to remove; it does not
   matter whether the key was misspelled or merely unimplemented. The cost is
   forward compatibility — an older binary refuses a newer file — and it is
   accepted, on the same reasoning PostgreSQL refuses an unrecognised
   `postgresql.conf` parameter.

4. **One loader, in the root command's `PersistentPreRunE`**, not a
   `Changed()` guard at each of the twenty-four binding points. It resolves
   every key once, publishes the resolution, and writes the resolved values
   back into the variables the flags are bound to — so no consumer can read a
   value the resolution disagrees with, and adding a key does not mean
   editing a consumer.

5. **A configuration registry is the single source of truth.** Key name, env
   var, flag name, type, secrecy and deferral live in one table
   (`internal/config/registry.go`). The resolver, `applyEnvOverrides`, the
   admin endpoint's report and the documentation gate all read it, so a
   setting cannot exist on one path and be missing from another.

6. **An unreadable or unparseable `--config` is a startup error, on every
   command.** #802 settled this for the auth block and #827 for the NATS TLS
   tier; it generalises, because every section reaches runtime now and a
   dropped parse error is a process running on values nobody wrote.

7. **A key is hot-reloadable only if a subscriber consumes it.** The admin
   API refuses a write to any other key with HTTP 409 naming it, and the
   config manager preserves such keys across a reload — the reported
   configuration is the running configuration or it is nothing.

8. **A key with no runtime consumer is DEFERRED explicitly, never left
   half-live** (Rule 11, `docs/design/correctness-fix-protocol.md`). It
   stays in the schema, carries the structural change that would make it
   live, and declares `"reaches_runtime": false` — and setting it is an
   ERROR, at startup and on every write path alike. Parsing it silently at
   startup and refusing it only through the admin API would leave the
   operator's own configuration accepted and inert, which is the shape #808
   was filed for.

## Why PostgreSQL's order

Wadjet ships the PostgreSQL wire protocol and ADR-0012 already makes
PostgreSQL the authority on what a client should expect. PostgreSQL resolves
configuration the same way — `postgresql.conf` loses to the server's command
line, which loses to `PGOPTIONS`, which loses to a session `SET` — and the
principle underneath is that **the more specific and more recent statement of
intent wins**. A flag typed at the command line is the most specific thing an
operator can say; a flag's default is not a statement of intent at all.

The alternative orders were considered and rejected:

- **File > env > flag** (Kubernetes-ish, "the declarative artifact is the
  truth"): makes a one-off `--bucket` on an operator's command line
  unusable, which is the main reason flags exist here.
- **Env > flag** (Twelve-Factor purism): the same objection, and it surprises
  everyone who has typed `--port` at a shell.
- **Keep the current order but wire the missing tiers**: the current order is
  "a flag default beats everything", which is not an order anyone chose. It
  exists because consumers read flag variables.

## Blast radius (accepted)

Ten settings have a flag whose default is not the zero value AND a lower tier
that existed before this change, so a deployment that sets the same setting
two ways changes behaviour: `--mode`, `--http-addr`, `--grpc-addr`,
`--storage-type`, `--endpoint`, `--bucket`, `--nats-port`, `--cluster-id`,
`--max-concurrent` (the nine the maintainer accepted on #808), plus
`--result-store`, which shadows the config-file-only key
`worker.result_store_bytes` and was found while building the registry.

A unit exporting `WADJET_STORAGE_BUCKET=prod` without `--bucket` moves from
reading bucket `wadjet` to reading bucket `prod`. That is the repair and the
risk in the same sentence. All ten are documented, so operators plausibly set
them; `docs/configuration.md` names every one with a before/after, and
`TestEveryNonZeroDefaultFlagIsAccountedFor` asserts the list in both
directions so a new non-zero-default flag cannot join the set unnamed.

Fifteen further settings have zero-value flag defaults: nothing changes for
them except that the environment variable and the config file start working.

## Gates

- **The precedence census, two arms.** Every registry key × all eight
  presence combinations of (file, env, flag), asserting the winning value AND
  the reported source. `internal/config.TestPrecedenceCensus` is a
  loader-chain arm; `cmd/wadjet.TestPrecedenceCensusOnTheRealCommand` builds
  the real `*cobra.Command` with the real flag registrations and runs the
  real `PersistentPreRunE` through `Execute`. The second arm exists because
  #808 *is* the failure of a correct model nothing called.
- `TestAFlagLeftAtItsDefaultLosesToTheEnvironment` — one named subtest per
  changed-behaviour setting.
- `TestEveryConfigKeyReachesItsFlagVariable` — the write-back is total over
  the registry, which is what makes consumers safe without editing them.
- `TestConfigFileSectionsReachRuntime` — for each formerly dead section, an
  assertion through the expression the run path itself uses (`serveMode()`,
  `newStore()`, `natsServerConfig()`, the cost guard).
- `TestTheCostGuardReachesTheRunPathFromTheEnvironmentAlone` — the cost
  guard is wired outside the `--config` block, so an env-only deployment is
  actually guarded. Its mirror asserts an unconfigured one stays unlimited.
- `TestAnUnknownConfigKeyIsAStartupError` and `TestAWellFormedConfigStillLoads`
  — the strict decode, in both directions.
- `TestADeferredSectionIsRefusedAtStartup` and its mirror — decision 8's
  startup half.
- `TestTelemetryReachesEveryModeThatHasAConsumer` — every run mode with an
  OTLP consumer calls the initializer.
- `TestEnvironmentVariableNamesAgreeEverywhere` — the registry, the
  `applyEnvOverrides` doc comment and `docs/configuration.md`'s tables name
  one set.
- `TestNATSTLSAgreesWithTheResolvedConfig` — the one remaining hand-rolled
  walk (#827's security control, which keeps its own gates) cannot disagree
  with the loader in any cell.
- `internal/server` — GET reports a source per key and redacts secrets; PUT
  of a key with no subscriber is refused by name; PUT of a key with one is
  applied and observed.

## Consequences

- Adding a setting means adding a registry row. Everything else follows.
- **A key whose default is non-zero cannot be turned off from the config
  file.** File-tier presence is decided by "non-zero after a bare
  unmarshal", so `use_ssl: false` reads as absent. Every bool in the
  registry defaults to `false` today, so nothing can need turning off and
  the limitation is inert — but the first `true`-default bool added to the
  registry would be silently unsettable from the file, and needs explicit
  presence detection (a `*bool`, or `yaml.Node`-level parsing) at that
  point. The same holds for a non-zero-default int.
- **The admin config surface is tied to `--config`**: `AdminAPI` is built
  beside the auth provider, which the file defines, so a process configured
  purely by flags and environment has no `GET /v1/admin/config` to read its
  own resolution from. That endpoint is the diagnostic the strict decode and
  the deferred-key refusal would otherwise lean on, which is part of why
  both of those are startup ERRORS rather than warnings.
- `internal/config` now depends on nothing new; the flag tier reaches it
  through a three-method interface the command layer implements.
- `parquet.compression`, `parquet.row_group_size` and
  `parquet.page_buffer_size` are deferred under decision 8: every ingest
  writer is built from `ingest.DefaultConfig()` at seven call sites across
  three packages, and `ingest.Config` carries only `RowGroupSize`. Making
  them live is `wadjet.Config` → `ingest.Config` → `parquet.WriterConfig`
  plumbing and is its own piece of work. Until then the section is refused,
  not accepted — the YAML sample in `docs/configuration.md` carries it
  commented out for that reason.
