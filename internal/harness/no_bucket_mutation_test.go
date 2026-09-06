package harness

import (
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// A benchmark harness must never MUTATE the bucket it reads (#921). The SF10
// fixture bucket grew to ~3× its lineitem rows because a harness STANDALONE
// coordinator ran the default-on background compaction sweep against the shared
// bucket, left the compacted_* outputs behind when it was killed before the
// 30-minute deferred delete, and the next run's primeS3Catalog registered both
// the outputs and the originals. Two guards close it, and this file gates both.

// TestHarnessServeArgsDisableBackgroundCompaction is the load-bearing gate: the
// coordinator (which is where the sweep actually runs) and the workers are
// spawned with --background-compaction=false. Reverting either append in
// cluster.go fails this.
func TestHarnessServeArgsDisableBackgroundCompaction(t *testing.T) {
	c := NewCluster(ClusterConfig{
		WadjetBin:   "/nonexistent/wadjet", // never spawned; we only inspect args
		RunDir:      t.TempDir(),
		NumWorkers:  2,
		PgAddr:      ":15499",
		StorageType: "s3",
		Bucket:      "wadjet-bench-sf10-use2",
		Region:      "us-east-2",
		Endpoint:    "s3.us-east-2.amazonaws.com",
		SSL:         true,
	})
	// coordinatorServeArgs reads the ports; set them so it does not build a
	// nonsense --nats-port. Values are irrelevant to the flag assertion.
	c.natsPort, c.httpPort, c.grpcPort = 14100, 14101, 14102
	c.natsURL = "nats://127.0.0.1:14100"

	const flag = "--background-compaction=false"
	coord := c.coordinatorServeArgs()
	if !contains(coord, flag) {
		t.Fatalf("coordinator serve args do not carry %q — a harness coordinator "+
			"would run the compaction sweep against the read bucket (#921)\n  args: %v", flag, coord)
	}
	// The coordinator is standalone mode: that is the process that starts the
	// sweep (cmd/wadjet/main.go runStandalone), so the flag on it is what
	// actually prevents the mutation.
	if !contains(coord, "--mode=standalone") {
		t.Fatalf("coordinator is not standalone mode — the guard's placement assumes it: %v", coord)
	}
	worker := c.workerServeArgs("worker-0", 14200)
	if !contains(worker, flag) {
		t.Fatalf("worker serve args do not carry %q (defence in depth, #921)\n  args: %v", flag, worker)
	}
}

// TestCompactedOrphansAreDetected gates the SECOND guard: primeS3Catalog
// refuses a listing that contains compacted_* residue. compactedOrphans is the
// detector it calls; it must find every compacted_* parquet object and nothing
// else. A staged dataset never contains one, so a hit is always the duplication
// hazard #921 recorded.
func TestCompactedOrphansAreDetected(t *testing.T) {
	now := time.Now()
	objs := []objstore.ObjectInfo{
		{Key: "tables/lineitem/chunk_0001.parquet", Size: 6_471_277, LastModified: now},
		{Key: "tables/lineitem/chunk_0002.parquet", Size: 6_472_805, LastModified: now},
		// The residue: an engine-written compaction output left in the bucket.
		{Key: "tables/lineitem/compacted_01a07452-55e9-7205-b88c-d20d6c4d0fad.parquet", Size: 324_624_427, LastModified: now},
		// Not parquet — ignored even though the name matches.
		{Key: "tables/lineitem/compacted_notes.txt", Size: 10, LastModified: now},
		// A prefix-only coincidence in the middle of a path segment is NOT a
		// hit: the guard keys on the BASENAME prefix.
		{Key: "tables/lineitem/uncompacted_data.parquet", Size: 100, LastModified: now},
	}
	got := compactedOrphans(objs)
	if len(got) != 1 || got[0] != "tables/lineitem/compacted_01a07452-55e9-7205-b88c-d20d6c4d0fad.parquet" {
		t.Fatalf("compactedOrphans = %v, want exactly the one compacted_*.parquet object", got)
	}

	// A clean listing yields nothing.
	clean := []objstore.ObjectInfo{
		{Key: "tables/orders/chunk_0001.parquet", Size: 100, LastModified: now},
		{Key: "tables/orders/chunk_0002.parquet", Size: 100, LastModified: now},
	}
	if got := compactedOrphans(clean); len(got) != 0 {
		t.Fatalf("compactedOrphans on a clean listing = %v, want none", got)
	}
}

func contains(args []string, want string) bool {
	for _, a := range args {
		if strings.EqualFold(a, want) {
			return true
		}
	}
	return false
}
