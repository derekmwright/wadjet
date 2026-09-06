package harness

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"strings"

	"github.com/derekmwright/wadjet/benchmarks/tpch"
	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// compactedObjectPrefix is the basename prefix the background compactor gives
// every file it writes (internal/storage/compaction/compactor.go). A STAGED
// benchmark dataset never contains one: the loaders write chunk_* / ingest
// names. So a compacted_* object under a table prefix can only be the residue
// of an earlier `wadjet serve` that ran its 5-minute compaction sweep against
// this bucket and was killed before its 30-minute deferred delete removed the
// inputs — the run's manifest knew the compacted copy REPLACED the chunks, but
// that manifest was ephemeral and died with the process, so both the copy and
// the originals are still here. Priming from the raw object listing then loads
// BOTH, and the table gains a full extra copy of every row the compaction
// touched. That is exactly how wadjet-bench-sf10-use2 came to hold ~3× its
// lineitem rows (#921; #278 is the same duplication class). The harness now
// disables background compaction on every cluster it spawns
// (cluster.go), so no NEW orphan is created; this guard refuses to load an
// EXISTING one, because a corrupted bucket must never again be silently primed
// as duplicated data.
const compactedObjectPrefix = "compacted_"

// allowCompactedEnv opts out of the guard for the rare legitimate case: a
// dataset whose canonical files were themselves produced by compaction and
// staged deliberately. Off by default; a corrupted bucket is the far more
// likely explanation, and a silent one is what cost us #921.
const allowCompactedEnv = "WADJET_HARNESS_ALLOW_COMPACTED"

// compactedOrphans returns the keys of any compacted_* parquet objects in a
// table's listing — the objects a staged dataset never contains.
func compactedOrphans(objects []objstore.ObjectInfo) []string {
	var orphans []string
	for _, obj := range objects {
		if !strings.HasSuffix(obj.Key, ".parquet") {
			continue
		}
		if strings.HasPrefix(path.Base(obj.Key), compactedObjectPrefix) {
			orphans = append(orphans, obj.Key)
		}
	}
	return orphans
}

// primeS3Catalog lists parquet files under bucket/dataPrefix/<table>/ for each
// of the 8 TPC-H tables, probes the first file for its real schema, and
// registers the files in the NATS KV catalog via CreateTable + AddFiles.
//
// Mirrors cmd/tpch-bench's discoverData pattern but writes to the catalog
// directly through NATS (like loadSampleData) rather than through a
// *wadjet.DB handle — the coordinator is a subprocess, so we'd otherwise
// have to go over pgwire to reach its in-process DB.
//
// This path assumes data is already staged in S3. It does not write sample
// parquet files; use loadSampleData for the FileStore path.
func primeS3Catalog(
	ctx context.Context,
	cluster *Cluster,
	endpoint, region, bucket string,
	ssl bool,
	dataPrefix string,
	logger *slog.Logger,
) error {
	if dataPrefix != "" && !strings.HasSuffix(dataPrefix, "/") {
		dataPrefix += "/"
	}

	store, err := objstore.NewMinIOStore(objstore.MinIOConfig{
		Endpoint: endpoint,
		UseSSL:   ssl,
		Region:   region,
	})
	if err != nil {
		return fmt.Errorf("s3 store: %w", err)
	}

	nc, err := cluster.ConnectNATS()
	if err != nil {
		return fmt.Errorf("connecting to NATS for catalog: %w", err)
	}
	defer nc.Close()

	js, err := distributed.NewJetStream(nc)
	if err != nil {
		return fmt.Errorf("creating JetStream: %w", err)
	}
	kv, err := catalog.NewNATSKV(js)
	if err != nil {
		return fmt.Errorf("creating NATS KV: %w", err)
	}
	cat := catalog.New(kv, store, bucket)
	if err := cat.Init(ctx); err != nil {
		return fmt.Errorf("catalog init: %w", err)
	}

	for name := range tpch.AllTables {
		prefix := dataPrefix + name + "/"
		objects, err := store.List(ctx, bucket, objstore.ListOptions{Prefix: prefix})
		if err != nil {
			return fmt.Errorf("listing s3://%s/%s: %w", bucket, prefix, err)
		}
		// Refuse a bucket carrying background-compaction residue. See
		// compactedObjectPrefix: a compacted_* object under a table prefix is
		// a duplicated-data hazard, not staged data, and priming it silently
		// is how #921 happened. Hard-fail unless the operator opts in.
		if orphans := compactedOrphans(objects); len(orphans) > 0 {
			if os.Getenv(allowCompactedEnv) == "" {
				return fmt.Errorf(
					"s3://%s/%s holds %d compacted_* object(s) (e.g. %s) — these are residue "+
						"from a prior cluster's background compaction, not staged data, and priming "+
						"them alongside the original files loads DUPLICATED rows (#921, #278). "+
						"Remove the compacted_* objects (keep one canonical generation) or set %s=1 "+
						"to prime anyway",
					bucket, prefix, len(orphans), orphans[0], allowCompactedEnv)
			}
			logger.Warn("priming a bucket with background-compaction residue — rows may be DUPLICATED",
				"table", name, "prefix", prefix, "compacted_objects", len(orphans),
				"override", allowCompactedEnv)
		}
		var pqObjects []objstore.ObjectInfo
		for _, obj := range objects {
			if strings.HasSuffix(obj.Key, ".parquet") {
				pqObjects = append(pqObjects, obj)
			}
		}
		if len(pqObjects) == 0 {
			return fmt.Errorf("no parquet files under s3://%s/%s", bucket, prefix)
		}

		// Probe the first file for the real schema. SF10/SF100 Polars-generated
		// parquet uses INT64/DATE rather than the hardcoded INT32/STRING
		// tpch.AllTables schemas; registering the hardcoded schema causes
		// silent type truncation in join keys. See cmd/tpch-bench/main.go
		// discoverData for the original motivation.
		schema, err := probeS3Schema(ctx, store, bucket, pqObjects[0].Key)
		if err != nil {
			logger.Warn("schema probe failed, falling back to hardcoded schema",
				"table", name, "key", pqObjects[0].Key, "err", err)
			schema = tpch.AllTables[name]
		}

		if err := cat.CreateTable(ctx, name, schema, nil); err != nil && !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("create table %s: %w", name, err)
		}

		files := make([]catalog.FileEntry, 0, len(pqObjects))
		var totalRows int64
		for _, obj := range pqObjects {
			numRows, perr := probeS3RowCount(ctx, store, bucket, obj.Key)
			if perr != nil {
				logger.Warn("row-count probe failed", "table", name, "key", obj.Key, "err", perr)
				numRows = 0
			}
			totalRows += numRows
			files = append(files, catalog.FileEntry{
				Path:      obj.Key,
				SizeBytes: obj.Size,
				NumRows:   numRows,
				CreatedAt: obj.LastModified,
			})
		}
		if err := cat.AddFiles(ctx, name, nil, "", files); err != nil {
			return fmt.Errorf("registering files for %s: %w", name, err)
		}
		logger.Info("primed table from S3", "table", name, "files", len(files), "rows", totalRows)
	}
	return nil
}

func probeS3Schema(ctx context.Context, store objstore.Store, bucket, key string) (parquet.Schema, error) {
	if ras, ok := store.(objstore.ReaderAtStore); ok {
		ra, sz, err := ras.GetReaderAt(ctx, bucket, key)
		if err != nil {
			return parquet.Schema{}, fmt.Errorf("opening reader-at: %w", err)
		}
		defer ra.Close()
		r, err := parquet.NewReader(ra, sz)
		if err != nil {
			return parquet.Schema{}, fmt.Errorf("parquet reader: %w", err)
		}
		return r.Schema(), nil
	}
	rc, _, err := store.Get(ctx, bucket, key)
	if err != nil {
		return parquet.Schema{}, fmt.Errorf("get: %w", err)
	}
	defer rc.Close()
	buf, err := io.ReadAll(rc)
	if err != nil {
		return parquet.Schema{}, fmt.Errorf("read: %w", err)
	}
	r, err := parquet.NewReaderFromBytes(buf)
	if err != nil {
		return parquet.Schema{}, fmt.Errorf("parquet reader: %w", err)
	}
	return r.Schema(), nil
}

func probeS3RowCount(ctx context.Context, store objstore.Store, bucket, key string) (int64, error) {
	if ras, ok := store.(objstore.ReaderAtStore); ok {
		ra, sz, err := ras.GetReaderAt(ctx, bucket, key)
		if err != nil {
			return 0, fmt.Errorf("opening reader-at: %w", err)
		}
		defer ra.Close()
		r, err := parquet.NewReader(ra, sz)
		if err != nil {
			return 0, fmt.Errorf("parquet reader: %w", err)
		}
		return r.NumRows(), nil
	}
	rc, _, err := store.Get(ctx, bucket, key)
	if err != nil {
		return 0, fmt.Errorf("get: %w", err)
	}
	defer rc.Close()
	buf, err := io.ReadAll(rc)
	if err != nil {
		return 0, fmt.Errorf("read: %w", err)
	}
	r, err := parquet.NewReaderFromBytes(buf)
	if err != nil {
		return 0, fmt.Errorf("parquet reader: %w", err)
	}
	return r.NumRows(), nil
}
