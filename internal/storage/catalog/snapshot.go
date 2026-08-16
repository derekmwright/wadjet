package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// SnapshotOptions configures where catalog snapshots are written.
type SnapshotOptions struct {
	Store  objstore.Store
	Bucket string
	Prefix string // path within bucket, e.g. "wadjet/catalog/". Must end in "/".
}

// SnapshotManifest is the JSON body of <ts>/manifest.json. Lists every
// KV key included in the snapshot and its SHA256 for integrity.
type SnapshotManifest struct {
	Version   int                `json:"version"`
	Timestamp string             `json:"timestamp"`
	ClusterID string             `json:"cluster_id"`
	KeyCount  int                `json:"key_count"`
	Keys      []SnapshotKeyEntry `json:"keys"`
}

// SnapshotKeyEntry records one KV key's location and integrity hash.
type SnapshotKeyEntry struct {
	KVKey  string `json:"kv_key"`  // e.g. "local.table.orders"
	S3Path string `json:"s3_path"` // relative to <prefix>/snapshots/<ts>/, e.g. "table/orders.json"
	SHA256 string `json:"sha256"`  // hex-encoded
}

// Snapshot writes every <clusterID>.* key to <prefix>/snapshots/<ts>/ and
// atomically updates <prefix>/latest. Returns the timestamp written.
func (c *Catalog) Snapshot(ctx context.Context, opts SnapshotOptions) (string, error) {
	if opts.Store == nil || opts.Bucket == "" || opts.Prefix == "" {
		return "", fmt.Errorf("Snapshot: Store, Bucket, Prefix are required")
	}
	if !strings.HasSuffix(opts.Prefix, "/") {
		opts.Prefix += "/"
	}

	ts := time.Now().UTC().Format("20060102T150405Z")
	snapRoot := opts.Prefix + "snapshots/" + ts + "/"

	// List every key under <clusterID>.
	clusterPrefix := c.clusterID + "."
	keys, err := c.kv.List(clusterPrefix)
	if err != nil {
		return "", fmt.Errorf("listing KV keys: %w", err)
	}
	sort.Strings(keys) // deterministic manifest ordering

	manifest := SnapshotManifest{
		Version:   1,
		Timestamp: ts,
		ClusterID: c.clusterID,
	}

	for _, kvKey := range keys {
		val, _, err := c.kv.Get(kvKey)
		if err != nil {
			return "", fmt.Errorf("reading key %q: %w", kvKey, err)
		}
		s3Path, err := kvKeyToS3Path(c.clusterID, kvKey)
		if err != nil {
			return "", err
		}
		sum := sha256.Sum256(val)
		manifest.Keys = append(manifest.Keys, SnapshotKeyEntry{
			KVKey:  kvKey,
			S3Path: s3Path,
			SHA256: hex.EncodeToString(sum[:]),
		})
		if _, err := opts.Store.Put(ctx, opts.Bucket, snapRoot+s3Path, bytes.NewReader(val), int64(len(val)), "application/json"); err != nil {
			return "", fmt.Errorf("writing snapshot key %q: %w", s3Path, err)
		}
	}
	manifest.KeyCount = len(manifest.Keys)

	// Write manifest.json after all keys are uploaded.
	mfBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return "", err
	}
	if _, err := opts.Store.Put(ctx, opts.Bucket, snapRoot+"manifest.json", bytes.NewReader(mfBytes), int64(len(mfBytes)), "application/json"); err != nil {
		return "", fmt.Errorf("writing manifest: %w", err)
	}

	// Atomically update latest pointer (single PUT, no torn write).
	latestBody := []byte(ts + "\n")
	if _, err := opts.Store.Put(ctx, opts.Bucket, opts.Prefix+"latest", bytes.NewReader(latestBody), int64(len(latestBody)), "text/plain"); err != nil {
		return "", fmt.Errorf("updating latest pointer: %w", err)
	}

	return ts, nil
}

// kvKeyToS3Path maps a fully qualified KV key ("<clusterID>.<segment>.<rest>")
// to a relative S3 path under snapshots/<ts>/. Examples:
//
//	<cid>.meta                 -> meta.json
//	<cid>.table.orders         -> table/orders.json
//	<cid>.manifest.orders      -> manifest_data/orders.json
//	<cid>.alert.failed_logins  -> alert/failed_logins.json
//	<cid>.<anything>.<rest>    -> <anything>/<rest>.json  (catch-all)
func kvKeyToS3Path(clusterID, kvKey string) (string, error) {
	prefix := clusterID + "."
	if !strings.HasPrefix(kvKey, prefix) {
		return "", fmt.Errorf("key %q is not in cluster %q", kvKey, clusterID)
	}
	suffix := kvKey[len(prefix):]
	// Top-level "meta" has no rest.
	if suffix == "meta" {
		return "meta.json", nil
	}
	// Split into segment + rest on the first '.'.
	dot := strings.Index(suffix, ".")
	if dot < 0 {
		// Unknown flat key — store at root.
		return suffix + ".json", nil
	}
	segment, rest := suffix[:dot], suffix[dot+1:]
	// "manifest" is a reserved filename (manifest.json at the root); use
	// manifest_data/ subdirectory for the per-table manifest KV keys.
	if segment == "manifest" {
		return "manifest_data/" + rest + ".json", nil
	}
	return segment + "/" + rest + ".json", nil
}

// s3PathToKVKey reverses kvKeyToS3Path. Used by Restore (Task 4).
func s3PathToKVKey(clusterID, s3Path string) (string, error) {
	if !strings.HasSuffix(s3Path, ".json") {
		return "", fmt.Errorf("s3 path %q has no .json suffix", s3Path)
	}
	trimmed := strings.TrimSuffix(s3Path, ".json")
	if trimmed == "meta" {
		return clusterID + ".meta", nil
	}
	// "manifest_data/<rest>" -> "<cid>.manifest.<rest>"
	if rest, ok := strings.CutPrefix(trimmed, "manifest_data/"); ok {
		return clusterID + ".manifest." + rest, nil
	}
	// Generic "<segment>/<rest>" -> "<cid>.<segment>.<rest>"
	slash := strings.Index(trimmed, "/")
	if slash < 0 {
		// Flat top-level (rare; future-proof).
		return clusterID + "." + trimmed, nil
	}
	segment, rest := trimmed[:slash], trimmed[slash+1:]
	return clusterID + "." + segment + "." + rest, nil
}

// RestoreOptions configures where to read a snapshot from.
type RestoreOptions struct {
	SnapshotOptions
	// ForceTS, when non-empty, overrides the latest pointer. Use "latest"
	// as a sentinel to mean "read the pointer" (equivalent to empty).
	ForceTS string
}

// Restore reads a snapshot from S3 and populates every KV key it contains.
// Returns the timestamp restored, or "" if there was nothing to restore
// (latest pointer absent).
//
// Restore does NOT check whether the KV is already populated — that is the
// caller's responsibility (see coordinator's startup hook).
func (c *Catalog) Restore(ctx context.Context, opts RestoreOptions) (string, error) {
	if opts.Store == nil || opts.Bucket == "" || opts.Prefix == "" {
		return "", fmt.Errorf("Restore: Store, Bucket, Prefix are required")
	}
	if !strings.HasSuffix(opts.Prefix, "/") {
		opts.Prefix += "/"
	}

	ts := opts.ForceTS
	if ts == "" || ts == "latest" {
		latest, err := readLatestTS(ctx, opts.SnapshotOptions)
		if err != nil {
			return "", fmt.Errorf("reading latest pointer: %w", err)
		}
		if latest == "" {
			return "", nil // nothing to restore; fresh cluster semantics
		}
		ts = latest
	}

	snapRoot := opts.Prefix + "snapshots/" + ts + "/"

	// Read manifest.
	r, _, err := opts.Store.Get(ctx, opts.Bucket, snapRoot+"manifest.json")
	if err != nil {
		return "", fmt.Errorf("reading manifest for %q: %w", ts, err)
	}
	mfBody, err := io.ReadAll(r)
	r.Close()
	if err != nil {
		return "", err
	}
	var mf SnapshotManifest
	if err := json.Unmarshal(mfBody, &mf); err != nil {
		return "", fmt.Errorf("parsing manifest: %w", err)
	}
	if mf.ClusterID != c.clusterID {
		return "", fmt.Errorf("snapshot cluster_id %q does not match catalog cluster_id %q", mf.ClusterID, c.clusterID)
	}

	// Fetch each key, verify SHA, write to KV.
	for _, entry := range mf.Keys {
		kr, _, err := opts.Store.Get(ctx, opts.Bucket, snapRoot+entry.S3Path)
		if err != nil {
			return "", fmt.Errorf("reading snapshot key %q: %w", entry.S3Path, err)
		}
		val, err := io.ReadAll(kr)
		kr.Close()
		if err != nil {
			return "", err
		}
		sum := sha256.Sum256(val)
		got := hex.EncodeToString(sum[:])
		if got != entry.SHA256 {
			return "", fmt.Errorf("integrity: sha256 mismatch for key %q (path %q): want %s, got %s",
				entry.KVKey, entry.S3Path, entry.SHA256, got)
		}
		if _, err := c.kv.Put(entry.KVKey, val); err != nil {
			return "", fmt.Errorf("writing KV key %q: %w", entry.KVKey, err)
		}
	}
	return ts, nil
}

// GCSnapshots deletes snapshot timestamps that are older than minAge AND
// not in the `keep` newest. Never deletes the snapshot currently pointed
// at by the latest pointer.
func (c *Catalog) GCSnapshots(ctx context.Context, opts SnapshotOptions, keep int, minAge time.Duration) error {
	if !strings.HasSuffix(opts.Prefix, "/") {
		opts.Prefix += "/"
	}
	snapshotsPrefix := opts.Prefix + "snapshots/"
	objs, err := opts.Store.List(ctx, opts.Bucket, objstore.ListOptions{Prefix: snapshotsPrefix})
	if err != nil {
		return fmt.Errorf("listing snapshots: %w", err)
	}

	// Collect unique timestamps.
	tsSet := make(map[string]struct{})
	for _, obj := range objs {
		rest := strings.TrimPrefix(obj.Key, snapshotsPrefix)
		slash := strings.Index(rest, "/")
		if slash <= 0 {
			continue
		}
		tsSet[rest[:slash]] = struct{}{}
	}
	tsList := make([]string, 0, len(tsSet))
	for ts := range tsSet {
		tsList = append(tsList, ts)
	}
	// Timestamp format is lexicographically sortable; newest last.
	sort.Strings(tsList)

	// Don't delete the latest pointer's target.
	latest, err := readLatestTS(ctx, opts)
	if err != nil {
		return fmt.Errorf("reading latest: %w", err)
	}

	now := time.Now().UTC()
	// Figure out which timestamps are "keep newest" (last `keep` entries).
	keepSet := make(map[string]struct{})
	start := len(tsList) - keep
	if start < 0 {
		start = 0
	}
	for _, ts := range tsList[start:] {
		keepSet[ts] = struct{}{}
	}
	if latest != "" {
		keepSet[latest] = struct{}{}
	}

	for _, ts := range tsList {
		if _, keep := keepSet[ts]; keep {
			continue
		}
		if minAge > 0 {
			t, err := time.Parse("20060102T150405Z", ts)
			if err != nil {
				continue // malformed; leave it alone
			}
			if now.Sub(t) < minAge {
				continue // too young to delete
			}
		}
		// Delete every object under snapshots/<ts>/
		subObjs, err := opts.Store.List(ctx, opts.Bucket, objstore.ListOptions{
			Prefix: snapshotsPrefix + ts + "/",
		})
		if err != nil {
			return fmt.Errorf("listing for delete %s: %w", ts, err)
		}
		for _, obj := range subObjs {
			_ = opts.Store.Delete(ctx, opts.Bucket, obj.Key) // best-effort
		}
	}
	return nil
}

// readLatestTS reads <prefix>/latest and returns the trimmed timestamp.
// Returns ("", nil) if the latest pointer does not exist (fresh cluster).
// Used by Restore (Task 4).
func readLatestTS(ctx context.Context, opts SnapshotOptions) (string, error) {
	r, _, err := opts.Store.Get(ctx, opts.Bucket, opts.Prefix+"latest")
	if err != nil {
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "NoSuchKey") {
			return "", nil
		}
		return "", err
	}
	defer r.Close()
	body, err := io.ReadAll(r)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(body)), nil
}
