# Disaster Recovery

This runbook documents how to back up, verify, and restore Wadjet catalog metadata.

## How Snapshots Work

Wadjet automatically snapshots all catalog metadata (table schemas, partition maps, file entries, column stats) to object storage every 5 minutes. Snapshots are JSON files stored under `_catalog/snapshots/` in the data bucket.

- **Interval**: 5 minutes (configurable via `catalog_snapshot.interval` or `WADJET_CATALOG_SNAPSHOT_INTERVAL`)
- **Retention**: 48 snapshots (~4 hours of history, configurable)
- **Storage**: Same S3 bucket as table data, under `_catalog/snapshots/`
- **Format**: JSON containing all NATS KV entries for the cluster
- **Leader-only**: In distributed mode, only the elected leader takes snapshots (prevents duplicates)

## Verifying Snapshots

### List Available Snapshots

```bash
# Via CLI
wadjet catalog list-snapshots --bucket wadjet --endpoint s3.us-east-2.amazonaws.com --ssl --region us-east-2

# Via S3 directly
aws s3 ls s3://wadjet/_catalog/snapshots/
```

Each snapshot file is named with a timestamp: `_catalog/snapshots/2026-03-27T06:30:00Z.json`

### Verify Snapshot Contents

Download and inspect a snapshot:

```bash
aws s3 cp s3://wadjet/_catalog/snapshots/2026-03-27T06:30:00Z.json /tmp/snap.json
cat /tmp/snap.json | jq '.entries | length'  # number of catalog entries
cat /tmp/snap.json | jq '.entries[].key' | head  # list entry keys
```

A healthy snapshot contains entries for:
- Table schemas (`<cluster>.tables.<name>.schema`)
- Partition metadata (`<cluster>.tables.<name>.partitions.<partition>`)
- File entries within each partition

## Restore Procedures

### Scenario 1: NATS KV Corrupted (Data Files Intact)

If the NATS KV store is corrupted or lost but S3 data files are intact:

```bash
# 1. Stop the coordinator
# 2. Delete the NATS store directory
rm -rf ~/.wadjet/nats

# 3. Restart the coordinator (empty KV)
wadjet serve --mode=coordinator ...

# 4. Restore from latest snapshot
wadjet catalog restore --bucket wadjet --endpoint s3.us-east-2.amazonaws.com --ssl --region us-east-2
```

### Scenario 2: Restore to a Specific Point in Time

```bash
# 1. List available snapshots
wadjet catalog list-snapshots ...

# 2. Restore from a specific snapshot
wadjet catalog restore --snapshot-key "_catalog/snapshots/2026-03-27T06:00:00Z.json" --bucket wadjet ...
```

### Scenario 3: Full Cluster Recovery (New Infrastructure)

1. Deploy new coordinator + workers with the same S3 bucket configuration
2. Start the coordinator (it will create an empty NATS KV)
3. Restore the catalog:
   ```bash
   wadjet catalog restore --bucket wadjet --endpoint s3.us-east-2.amazonaws.com --ssl --region us-east-2
   ```
4. Verify tables are accessible:
   ```bash
   wadjet tables --bucket wadjet --endpoint s3.us-east-2.amazonaws.com --ssl --region us-east-2
   ```
5. Start workers and verify query execution

### Scenario 4: S3 Data Loss

If S3 data files are lost, snapshots alone cannot recover the data — they only contain metadata (schemas, file paths, stats). Data recovery requires:

1. Restore from S3 versioning (if enabled on the bucket)
2. Re-ingest from source systems

**Recommendation**: Enable S3 versioning and cross-region replication on the data bucket for production deployments.

## Monitoring Snapshot Health

### Prometheus Metrics

Wadjet exposes snapshot metrics at `/metrics`:

- Check the snapshot timestamp in the coordinator logs for recency
- Monitor the `_catalog/snapshots/` prefix in S3 for expected file count

### Manual Health Check

```bash
# Verify snapshots are being created (should see files within last 10 minutes)
aws s3 ls s3://wadjet/_catalog/snapshots/ | tail -5

# Verify snapshot is parseable
aws s3 cp s3://wadjet/_catalog/snapshots/$(aws s3 ls s3://wadjet/_catalog/snapshots/ | tail -1 | awk '{print $4}') - | jq '.entries | length'
```

## Configuration Reference

```yaml
catalog_snapshot:
  enabled: true           # default: true
  interval: "5m"          # snapshot frequency
  retention: 48           # max snapshots to keep
  prefix: "_catalog/snapshots"  # S3 key prefix
  debounce: "30s"         # min time between mutation-triggered snapshots
  leader_only: true       # only leader snapshots in distributed mode
```

Environment variables: `WADJET_CATALOG_SNAPSHOT_ENABLED`, `WADJET_CATALOG_SNAPSHOT_INTERVAL`, `WADJET_CATALOG_SNAPSHOT_RETENTION`, `WADJET_CATALOG_SNAPSHOT_PREFIX`, `WADJET_CATALOG_SNAPSHOT_DEBOUNCE`, `WADJET_CATALOG_SNAPSHOT_LEADER_ONLY`

## Recovery Time Objectives

| Scenario | RTO | RPO |
|----------|-----|-----|
| NATS KV corruption | ~2 min (restore + restart) | ≤5 min (snapshot interval) |
| Full cluster rebuild | ~10 min (deploy + restore) | ≤5 min |
| S3 data loss | Depends on re-ingestion | Data since last S3 backup |
