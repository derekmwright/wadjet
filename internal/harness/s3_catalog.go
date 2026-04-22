package harness

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/citc-tech/wadjet/benchmarks/tpch"
	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

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
