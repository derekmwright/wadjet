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

	"github.com/citc-tech/wadjet/internal/storage/objstore"
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
