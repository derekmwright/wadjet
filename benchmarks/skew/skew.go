// Package skew generates the synthetic hot-key benchmark fixture for the
// adaptive skew-aware shuffle A/B (docs/design/skew-aware-shuffle.md,
// Phase 3). It is the TestSkewSplitParity fixture shape scaled to sizes that
// engage the mechanism at PRODUCTION thresholds — no lowered knobs.
//
// Shape: skew_events (probe side, one hot join key carrying HotPct% of all
// rows) joins skew_dims (build side, uniform keys). The sizes are dialed to
// the mechanism's engagement envelope on a 3-worker cluster:
//
//   - skew_dims manifest bytes must exceed the cluster broadcast threshold
//     (broadcastThresholdFromCluster, capped at 200 MiB) so the planner emits
//     a hash-partitioned join instead of broadcast+probe-split. The wide
//     `pad` column exists only for this: queries never project it, so the
//     shuffled build stays a few tens of MB — far under the skew split's
//     build-replication bound. Wide dimension table, narrow projection: the
//     common shape in the security workloads this mechanism targets.
//   - The hot group's probe bytes must exceed skewSplitMinGroupBytes
//     (256 MiB) while the uniform groups stay below it, so exactly one group
//     splits. Suite queries aggregate over events.v (count(DISTINCT e.v)),
//     which forces the ~256-byte payload through the shuffle — projection
//     pruning would otherwise reduce the probe to its 8-byte key and no
//     realistic row count would cross the threshold.
//
// Generation is deterministic for a given Config (seeded rand, fixed chunk
// boundaries), so two harness runs — the flag-off and flag-on A/B arms —
// see byte-identical logical rows and row-level parity checks are valid.
package skew

import (
	"fmt"
	"math/rand"
	"strconv"

	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// Tables maps table name → schema, for catalog registration and S3 discovery.
var Tables = map[string]parquet.Schema{
	"skew_events": {Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeString},
	}},
	"skew_dims": {Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString},
		{Name: "pad", Type: parquet.TypeString},
	}},
}

// Queries is the fixed suite, keyed by name. Both queries force events.v
// through the shuffle (see package comment) and produce small deterministic
// result sets so cross-arm parity can compare full row checksums.
//
// skew_join_agg: inner join + aggregate over the build side's low-cardinality
// category. The hot key's category count and per-category distinct-payload
// counts surface any row loss or duplication under the split. The
// count(DISTINCT e.v) partial state also makes task memory proportional to
// task probe bytes — the memory-relief signal.
//
// skew_left_join: unmatched probe rows (events keys outside dims' key range)
// must survive a replicated-build split exactly once each; total_rows and
// distinct_payloads equal the generated events row count.
var Queries = map[string]string{
	"skew_join_agg": `SELECT d.name, count(*) AS cnt, count(DISTINCT e.v) AS distinct_payloads
FROM skew_events e JOIN skew_dims d ON e.k = d.k
GROUP BY d.name ORDER BY d.name`,
	"skew_left_join": `SELECT count(*) AS total_rows, count(d.name) AS matched_rows, count(DISTINCT e.v) AS distinct_payloads
FROM skew_events e LEFT JOIN skew_dims d ON e.k = d.k`,
}

// QueryOrder is the canonical execution order of the suite.
var QueryOrder = []string{"skew_join_agg", "skew_left_join"}

// Config parameterizes the generator. All sizes are row counts; byte sizes
// follow from PadBytes (~PadBytes+16 per events row).
type Config struct {
	EventsRows int   // total probe rows
	DimsRows   int   // total build rows; dims keys are [0, DimsRows)
	HotKey     int64 // the hot events key; must be < DimsRows so it matches
	HotPct     int   // percent of events rows on HotKey (0-100)
	KeySpace   int64 // non-hot events keys are uniform in [0, KeySpace); keys >= DimsRows miss
	NameCard   int   // distinct dims.name values (result cardinality of skew_join_agg)
	PadBytes   int   // random payload width for events.v and dims.pad
	ChunkRows  int   // rows per emitted parquet chunk
	Seed       int64
}

// DefaultLocalConfig engages the skew split at production thresholds on a
// 3-worker local harness cluster: dims ≈ 330 MB parquet (over the 200 MiB
// broadcast cap), events ≈ 1.7 GB with 90% on the hot key — hot group
// ≈ 1.6 GB probe (splits at k=3), uniform groups ≈ 60 MB (stay unsplit).
func DefaultLocalConfig() Config {
	return Config{
		EventsRows: 6_500_000,
		DimsRows:   1_200_000,
		HotKey:     7,
		HotPct:     90,
		KeySpace:   1_600_000,
		NameCard:   48,
		PadBytes:   248,
		ChunkRows:  250_000,
		Seed:       42,
	}
}

// DefaultDeployConfig is the SF10-class fixture: events ≈ 8.5 GB with a
// ≈ 7.6 GB hot partition, dims identical to the local config. Uniform groups
// ≈ 220 MB stay under the 256 MiB floor; only the hot group splits.
func DefaultDeployConfig() Config {
	cfg := DefaultLocalConfig()
	cfg.EventsRows = 32_000_000
	cfg.ChunkRows = 500_000
	return cfg
}

// Validate rejects configs the generator or the suite queries would
// misbehave on.
func (c Config) Validate() error {
	switch {
	case c.EventsRows <= 0 || c.DimsRows <= 0 || c.ChunkRows <= 0:
		return fmt.Errorf("skew config: rows and chunk size must be positive: %+v", c)
	case c.HotKey < 0 || c.HotKey >= int64(c.DimsRows):
		return fmt.Errorf("skew config: HotKey %d must be in [0, DimsRows %d) so it matches a dim", c.HotKey, c.DimsRows)
	case c.HotPct < 0 || c.HotPct > 100:
		return fmt.Errorf("skew config: HotPct %d out of [0,100]", c.HotPct)
	case c.KeySpace <= int64(c.DimsRows):
		return fmt.Errorf("skew config: KeySpace %d must exceed DimsRows %d so LEFT JOIN has unmatched rows", c.KeySpace, c.DimsRows)
	case c.NameCard <= 0 || c.PadBytes <= 0:
		return fmt.Errorf("skew config: NameCard and PadBytes must be positive: %+v", c)
	}
	return nil
}

// GenerateChunked streams the fixture through emit in deterministic chunk
// order: all skew_dims chunks, then all skew_events chunks. emit receives at
// most ChunkRows rows per call, mirroring tpch.GenerateChunked's contract.
func GenerateChunked(cfg Config, emit func(table string, rows []map[string]any) error) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	rng := rand.New(rand.NewSource(cfg.Seed))

	for start := 0; start < cfg.DimsRows; start += cfg.ChunkRows {
		n := cfg.ChunkRows
		if start+n > cfg.DimsRows {
			n = cfg.DimsRows - start
		}
		rows := make([]map[string]any, n)
		for i := range rows {
			k := int64(start + i)
			rows[i] = map[string]any{
				"k":    k,
				"name": fmt.Sprintf("cat-%02d", k%int64(cfg.NameCard)),
				"pad":  randPayload(rng, cfg.PadBytes),
			}
		}
		if err := emit("skew_dims", rows); err != nil {
			return fmt.Errorf("emitting skew_dims chunk at %d: %w", start, err)
		}
	}

	row := 0
	for start := 0; start < cfg.EventsRows; start += cfg.ChunkRows {
		n := cfg.ChunkRows
		if start+n > cfg.EventsRows {
			n = cfg.EventsRows - start
		}
		rows := make([]map[string]any, n)
		for i := range rows {
			k := cfg.HotKey
			if row%100 >= cfg.HotPct {
				k = rng.Int63n(cfg.KeySpace)
			}
			// The "-<row>" suffix makes v unique, so count(DISTINCT e.v)
			// equals the joined row count exactly — a parity invariant.
			rows[i] = map[string]any{
				"k": k,
				"v": randPayload(rng, cfg.PadBytes) + "-" + strconv.Itoa(row),
			}
			row++
		}
		if err := emit("skew_events", rows); err != nil {
			return fmt.Errorf("emitting skew_events chunk at %d: %w", start, err)
		}
	}
	return nil
}

// payloadAlphabet has 64 symbols so each output byte carries 6 bits of
// entropy: the payloads stay near-incompressible under parquet's snappy
// pages AND the .wshf shuffle format, keeping manifest-byte estimates and
// per-partition shuffle accounting within ~25% of each other. Compressible
// pads would shrink the two measures differently and silently shift the
// fixture out of the engagement envelope.
const payloadAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

func randPayload(rng *rand.Rand, n int) string {
	buf := make([]byte, n)
	rng.Read(buf)
	for i, b := range buf {
		buf[i] = payloadAlphabet[b&63]
	}
	return string(buf)
}
