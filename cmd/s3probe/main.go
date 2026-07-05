// s3probe measures raw objstore.MinIOStore GET throughput against a bucket
// prefix at several concurrency levels. Diagnostic tool for the standalone
// cold-S3 scan investigation (2026-07-05): decomposes scan-path wall time
// into store-layer throughput vs scan-machinery overhead.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/citc-tech/wadjet/internal/storage/objstore"
)

func main() {
	var (
		endpoint = flag.String("endpoint", "s3.us-east-2.amazonaws.com", "S3 endpoint")
		bucket   = flag.String("bucket", "wadjet-bench-sf10-use2", "bucket")
		region   = flag.String("region", "us-east-2", "region")
		prefix   = flag.String("prefix", "tables/lineitem/", "key prefix")
		maxFiles = flag.Int("max-files", 300, "max objects per pass")
		levels   = flag.String("levels", "1,4,8,16,32", "comma-separated concurrency levels")
	)
	flag.Parse()

	ctx := context.Background()
	store, err := objstore.NewMinIOStore(objstore.MinIOConfig{
		Endpoint: *endpoint,
		UseSSL:   true,
		Region:   *region,
	})
	if err != nil {
		log.Fatal(err)
	}

	keys, err := listKeys(ctx, store, *bucket, *prefix, *maxFiles)
	if err != nil {
		log.Fatal(err)
	}
	if len(keys) == 0 {
		log.Fatalf("no objects under %s/%s", *bucket, *prefix)
	}
	fmt.Printf("objects: %d under %s\n", len(keys), *prefix)

	var lvls []int
	for _, part := range splitInts(*levels) {
		lvls = append(lvls, part)
	}

	// Per-object latency at c=1 over a small sample first.
	sample := keys
	if len(sample) > 24 {
		sample = keys[:24]
	}
	var lats []time.Duration
	var sampleBytes int64
	for _, k := range sample {
		t0 := time.Now()
		n, err := getAll(ctx, store, *bucket, k)
		if err != nil {
			log.Fatalf("get %s: %v", k, err)
		}
		lats = append(lats, time.Since(t0))
		sampleBytes += n
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	fmt.Printf("c=1 per-object: p50=%v p90=%v max=%v avg_size=%.1fMB\n",
		lats[len(lats)/2], lats[len(lats)*9/10], lats[len(lats)-1],
		float64(sampleBytes)/float64(len(sample))/1e6)

	for _, c := range lvls {
		wall, bytes := pass(ctx, store, *bucket, keys, c)
		mbs := float64(bytes) / 1e6 / wall.Seconds()
		fmt.Printf("c=%-3d wall=%-10v bytes=%.1fGB agg=%.1f MB/s per-lane=%.1f MB/s\n",
			c, wall.Round(time.Millisecond), float64(bytes)/1e9, mbs, mbs/float64(c))
	}
}

func pass(ctx context.Context, store objstore.Store, bucket string, keys []string, conc int) (time.Duration, int64) {
	var idx, total int64
	var wg sync.WaitGroup
	t0 := time.Now()
	wg.Add(conc)
	for w := 0; w < conc; w++ {
		go func() {
			defer wg.Done()
			for {
				i := int(atomic.AddInt64(&idx, 1) - 1)
				if i >= len(keys) {
					return
				}
				n, err := getAll(ctx, store, bucket, keys[i])
				if err != nil {
					log.Printf("get %s: %v", keys[i], err)
					continue
				}
				atomic.AddInt64(&total, n)
			}
		}()
	}
	wg.Wait()
	return time.Since(t0), total
}

func getAll(ctx context.Context, store objstore.Store, bucket, key string) (int64, error) {
	rc, _, err := store.Get(ctx, bucket, key)
	if err != nil {
		return 0, err
	}
	defer rc.Close()
	return io.Copy(io.Discard, rc)
}

func listKeys(ctx context.Context, store objstore.Store, bucket, prefix string, max int) ([]string, error) {
	infos, err := store.List(ctx, bucket, objstore.ListOptions{Prefix: prefix, MaxKeys: max})
	if err != nil {
		return nil, err
	}
	var keys []string
	for _, o := range infos {
		keys = append(keys, o.Key)
		if len(keys) >= max {
			break
		}
	}
	return keys, nil
}

func splitInts(s string) []int {
	var out []int
	cur := 0
	has := false
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			if has {
				out = append(out, cur)
			}
			cur, has = 0, false
			continue
		}
		if s[i] >= '0' && s[i] <= '9' {
			cur = cur*10 + int(s[i]-'0')
			has = true
		}
	}
	return out
}
