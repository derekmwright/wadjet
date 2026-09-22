# ADR-0041: An embedded program's catalog is a directory, opened the way `serve` opens it

Status: Accepted (2026-09-22, #1255, arc EC)

Related: ADR-0037 (the license seam: everything `wadjet.Open` reaches stays
MIT), ADR-0029 (configuration precedence — `derivedCatalogStoreDir` is the
file-tier rule this ADR makes the embedded API's rule too), ADR-0030 (a DML
statement commits against the manifest it read — the CAS the catalog store
must keep), ADR-0020 (DROP TABLE reclaim is opt-in — the same "an orphan is
bytes, not rows" position this ADR's crash window leans on).

## Context

`wadjet.Config.MetaKV` is typed `catalog.MetaKV`, an interface under
`internal/`, with no public constructor. An out-of-tree program's only choice
was nil, and nil was an in-memory catalog: every table it created was
process-local and gone at exit, while the Parquet objects the table had
flushed to a durable store stayed behind, referenced by nothing (#1255). The
getting-started guide documented this as the way things were. For the line's
goal — an engine an embedded, small-workload user can adopt — a catalog that
does not survive a restart is not a limitation, it is the absence of the
feature.

The CLI had already solved the same problem for itself. Since #842 every
`wadjet` command and `wadjet serve` under `--storage-type=file --data-dir=D`
keep the catalog at `D/_catalog`: an embedded nats-server with a JetStream
file store, an advisory `flock` on `D/_catalog/wadjet.lock` that binds every
door, and the holder's pid and client URL published into the lock file so a
short-lived command that loses the race reaches the holder's catalog rather
than whatever answers a well-known port. That mechanism lived in
`internal/cli`, where `wadjet.Open` cannot reach it.

## Decision

1. **A persistent catalog is a DIRECTORY, not a type.** `Config.DataDir`
   names one local directory for the data (a file store, under `Bucket`,
   default `wadjet`) and the catalog (`<DataDir>/_catalog`).
   `Config.CatalogDir` names the catalog directory alone, beside any `Store`.
   Neither names an internal type; `MetaKV` stays for the in-repo servers
   that already hold a KV, and a `Config` naming two stores or two catalogs
   is refused before anything is opened.

2. **The directory is opened the way `serve` opens it — by the same code.**
   `internal/catalogdir` (MIT) holds the lock, the embedded JetStream store,
   the published holder and the KV bucket, and `wadjet.Open`, the CLI
   commands and `wadjet serve` all call it. The layout, the bucket, the KV
   bucket name and the cluster id (`local`) are identical, so a program, the
   CLI and a server over one directory hold ONE set of tables. This is the
   property the alternative of a small file-backed KV could not have: a
   second catalog FORMAT is a second catalog, and a program and a `serve`
   over one directory would each have held one the other could not see —
   the silent divergence this ADR exists to rule out.

3. **One holder at a time; a second opener is refused, never a second
   writer.** A held directory is refused with `wadjet.ErrCatalogHeld`,
   naming the directory and the holder's pid — from the same process or
   another, including a `serve`. `Open` does not dial the holder the way a
   short-lived command does: a long-lived program whose catalog vanished
   when an unrelated process exited would be a worse contract than a
   refusal at `Open`. The CLI commands still dial, so `wadjet tables
   --data-dir=D` beside a running program lists its tables live. `Close`
   releases the directory; a killed process leaves the flock to the kernel
   and the JetStream store to its own recovery.

4. **The in-memory catalog stays, explicitly.** A `Config` naming no
   directory gets the in-memory catalog it always got — it is what every
   test over `NewMemStore` wants. Over a `*FileStore` it logs one warning
   naming the two fields, because that shape is the #1255 trap: rows on
   disk, catalog gone.

5. **The crash window is object-then-manifest, and it stays that way.** The
   ingester writes the Parquet object before it commits the manifest by CAS,
   so a process killed between the two leaves an orphan object and no entry;
   killed after, the table with its rows. The catalog never names a file
   that is not there. This is gated in both directions
   (`TestAKillBetweenFlushAndCatalogWriteNeverLeavesADanglingEntry`), and it
   is a process-kill guarantee: across a power loss neither the object write
   nor the catalog write is fsynced, which is a filing, not this ADR.

## Alternatives considered

- **A small file-backed `MetaKV`** (a JSON file per key, rename + fsync, a
  revision counter). Lighter — the embedded nats-server adds about 14 MB to
  an out-of-tree binary (31.5 MB → 45.5 MB, `test/embed` at base vs tip) —
  and faster to open. Rejected on decision 2: a second format is a second
  catalog, and making `serve` switch formats too would have taken away the
  live CLI-beside-`serve` flow, because a file KV has no holder a second
  process can dial.
- **A public `NewNATSKV`-style constructor** (the direction #1255 sketches).
  Exports a transport (`jetstream.JetStream`) in the public API and leaves
  the embedder to run a server, pick a store directory and — the part nobody
  knows to do — take the lock that keeps two processes off one JetStream
  store. Rejected: the API names a directory, not a transport.
- **Keep the in-memory default and document it.** The status quo. Rejected.

## Consequences

- `wadjet.Open(ctx, wadjet.Config{DataDir: dir})` is the zero-configuration
  persistent database, and `Open` on a directory costs milliseconds (7–24 ms
  empty, 6–13 ms with two tables and a `ListTables`, against microseconds
  for the in-memory catalog): an embedded nats-server starts on an ephemeral
  port on every `Open`. A program that opens and closes a `DB` per request
  is holding it wrong; one `DB` per process is the contract.
- An out-of-tree binary now links nats-server. Every package `wadjet`
  reaches is MIT; `tools/licensecheck` holds that.
- `internal/cli.LockCatalogStoreDir`, `CatalogLock` and `CatalogLockPath`
  are the same names over `internal/catalogdir`; `internal/clid` (AGPL)
  keeps calling them.
- The getting-started program runs twice against one directory and answers
  both runs' rows; `test/embed` is that restart from a separate module.
- Two open positions carry forward as filings: fsync on the object write
  and the catalog store (power-loss ordering), and an orphan-object sweep.
