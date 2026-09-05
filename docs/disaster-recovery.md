# Disaster Recovery

This runbook documents how to back up, verify, and restore Wadjet catalog metadata.

## How Snapshots Work

Catalog snapshots are **opt-in**. Start the coordinator (or standalone) with
`--catalog-snapshot-s3-prefix=s3://<bucket>/<path>/` and it writes every
`<cluster>.*` NATS KV key to object storage on a timer. Unset, nothing is
written.

- **Enabled by**: `--catalog-snapshot-s3-prefix` (or `WADJET_CATALOG_SNAPSHOT_PREFIX`)
- **Interval**: `--catalog-snapshot-interval`, default 5m (or `WADJET_CATALOG_SNAPSHOT_INTERVAL`). `0` disables the timer; an explicit `CREATE SNAPSHOT` still works, but only through the gRPC `Query` RPC against a coordinator — `psql` and `POST /v1/queries` both refuse the statement `0A000` (see [SQL reference](sql-reference.md#parsed-and-not-executed))
- **Retention**: the GC after each tick keeps the 10 newest snapshots plus every snapshot younger than 24h. Not configurable
- **Layout**: one directory per snapshot, not one file — `<prefix>snapshots/<ts>/manifest.json` is the index, `<prefix>snapshots/<ts>/<kind>/<name>.json` is one file per KV key, and `<prefix>latest` holds the newest `<ts>`
- **Timestamp format**: `20060102T150405Z` (e.g. `20260327T063000Z`), UTC
- **Leader-only**: In distributed mode, only the elected leader takes snapshots (prevents duplicates)

## Verifying Snapshots

### List Available Snapshots

There is no `wadjet catalog list-snapshots` command. List the prefix directly:

```bash
aws s3 ls s3://wadjet/catalog/snapshots/
aws s3 cp s3://wadjet/catalog/latest -   # newest timestamp
```

Each snapshot is a directory named with a UTC timestamp:
`catalog/snapshots/20260327T063000Z/`

### Verify Snapshot Contents

Download and inspect a snapshot manifest:

```bash
aws s3 cp s3://wadjet/catalog/snapshots/20260327T063000Z/manifest.json /tmp/snap.json
jq '.key_count' /tmp/snap.json          # number of catalog keys
jq -r '.keys[].kv_key' /tmp/snap.json   # list KV keys
jq -r '.keys[].s3_path' /tmp/snap.json  # list the per-key object paths
```

A healthy snapshot contains keys for:
- Cluster metadata (`<cluster>.meta` -> `meta.json`)
- Table schemas (`<cluster>.table.<name>` -> `table/<name>.json`)
- File manifests (`<cluster>.manifest.<name>` -> `manifest_data/<name>.json`)
- Alerts, if configured (`<cluster>.alert.<name>` -> `alert/<name>.json`)

## Restore Procedures

### Scenario 1: NATS KV Corrupted (Data Files Intact)

If the NATS KV store is corrupted or lost but S3 data files are intact:

```bash
# 1. Stop the coordinator
# 2. Delete the NATS store directory
rm -rf ~/.wadjet/nats

# 3. Restart the coordinator with the restore flag — it restores the newest
#    snapshot before serving.
wadjet serve --mode=coordinator \
  --catalog-snapshot-s3-prefix=s3://wadjet/catalog/ \
  --force-restore-catalog=latest ...
```

Without `--force-restore-catalog`, the coordinator restores automatically only
when its catalog holds no tables.

### Scenario 2: Restore to a Specific Point in Time

```bash
# 1. List snapshot timestamps
aws s3 ls s3://wadjet/catalog/snapshots/

# 2. Restore that timestamp
wadjet serve --mode=coordinator \
  --catalog-snapshot-s3-prefix=s3://wadjet/catalog/ \
  --force-restore-catalog=20260327T060000Z ...
```

### Scenario 3: Full Cluster Recovery (New Infrastructure)

1. Deploy new coordinator + workers with the same S3 bucket configuration
2. Start the coordinator with `--catalog-snapshot-s3-prefix=... --force-restore-catalog=latest`
3. To take a snapshot on demand against a running coordinator:
   ```bash
   wadjet catalog snapshot --coord-addr=<coord-host>:5433
   ```
4. Verify tables are accessible. `tables` reads the catalog of the running
   coordinator, so point it at that coordinator's NATS:
   ```bash
   wadjet tables --bucket wadjet --endpoint s3.us-east-2.amazonaws.com --ssl --region us-east-2 \
     --nats-url nats://<coord-host>:4222
   ```
5. Start workers and verify query execution

### Scenario 4: S3 Data Loss

If S3 data files are lost, snapshots alone cannot recover the data — they only contain metadata (schemas, file paths, stats). Data recovery requires:

1. Restore from S3 versioning (if enabled on the bucket)
2. Re-ingest from source systems

**Recommendation**: Enable S3 versioning and cross-region replication on the data bucket for production deployments.

## Monitoring Snapshot Health

### Logs and S3

Wadjet exposes **no** snapshot-specific Prometheus metric. Monitor via logs and S3:

- The coordinator logs a warning on every failed tick (`catalog snapshot tick error`, `catalog snapshot GC error`)
- Watch the `latest` pointer and the snapshot prefix for recency

### Manual Health Check

```bash
# Newest snapshot timestamp
aws s3 cp s3://wadjet/catalog/latest -

# Verify its manifest is parseable and non-empty
TS=$(aws s3 cp s3://wadjet/catalog/latest - | tr -d '\n')
aws s3 cp s3://wadjet/catalog/snapshots/$TS/manifest.json - | jq '.key_count'
```

## Configuration Reference

Catalog snapshots have **no config-file section**. They are configured by flag
or environment variable only:

| Flag | Env | Default | Meaning |
|---|---|---|---|
| `--catalog-snapshot-s3-prefix` | `WADJET_CATALOG_SNAPSHOT_PREFIX` | unset (disabled) | `s3://bucket/path/` target |
| `--catalog-snapshot-interval` | `WADJET_CATALOG_SNAPSHOT_INTERVAL` | `5m` | periodic cadence; `0` = explicit-only |
| `--force-restore-catalog` | — | unset | `latest`, or a specific `20060102T150405Z` timestamp |

Retention (10 newest plus everything under 24h) is fixed in code and has no
knob. There is no mutation-triggered snapshot: the only triggers are the
interval ticker and an explicit `CREATE SNAPSHOT` over gRPC.

## Recovery Time Objectives

| Scenario | RTO | RPO |
|----------|-----|-----|
| NATS KV corruption | ~2 min (restore + restart) | ≤5 min (snapshot interval, once `--catalog-snapshot-s3-prefix` is set) |
| Full cluster rebuild | ~10 min (deploy + restore) | ≤5 min |
| S3 data loss | Depends on re-ingestion | Data since last S3 backup |
