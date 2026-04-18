package worker

import (
	"context"
	"fmt"

	"github.com/citc-tech/wadjet/internal/storage/objstore"
)

// newPartitionShardSource lists all .wshf files at the given S3 prefix and
// returns a streaming source over them. Used by the probe-side worker in a
// shuffle-distributed query: each worker is assigned one or more partition
// prefixes, and instantiates one source per assigned partition to read the
// hash-shard rows that belong to its partition.
//
// Files are read sequentially; each is fully drained before the next one
// starts. This matches the existing cachedFileStreamSource semantics.
func newPartitionShardSource(ctx context.Context, executor *Executor, bucket, prefix string) (*cachedFileStreamSource, error) {
	infos, err := executor.store.List(ctx, bucket, objstore.ListOptions{Prefix: prefix})
	if err != nil {
		return nil, fmt.Errorf("listing partition prefix %q: %w", prefix, err)
	}
	files := make([]string, 0, len(infos))
	for _, info := range infos {
		files = append(files, info.Key)
	}
	return newCachedFileStreamSource(executor, bucket, files), nil
}
