package distributed

// Dynamic filter (Trino-style semi-join pushdown) wire types.
//
// Flow: the planner identifies eligible joins (selective build, large probe
// over an integer key) and annotates two stages — a build-side scan with
// DynamicFilterEmit and a probe-side scan with DynamicFilterConsume. At
// runtime the build-scan tasks compute partial {KeyRange, Bloom} stats over
// the join key column, upload them as sideband artifacts, and the
// coordinator unions the partials into a single BuildStats. The unioned
// stats are then injected into the probe-scan stage's OpSpec.DynamicFilters
// so probe-side row groups whose stats don't intersect the bloom are skipped
// before any S3 fetch fires.

// DynamicFilterEmit, attached to a build-scan Stage, instructs each scan task
// to compute a partial bloom + min/max range on KeyColumn (post-filter,
// post-projection). All tasks use the same BloomBits so the coordinator
// unions partials with a trivial bitwise OR.
type DynamicFilterEmit struct {
	FilterID  string `json:"filter_id"`
	KeyColumn string `json:"key_column"`
	KeyType   string `json:"key_type"` // "int32" | "int64" | "date" — integer types only in v1
	BloomBits int    `json:"bloom_bits"`
	// AtOutput places the accumulator over the task's OUTPUT stream (just
	// before the sink) instead of the scan source. Used by the semi/anti
	// build-filter pass, whose bloom must reflect the stage's post-filter /
	// post-join output keys (docs/design/semi-anti-build-dynamic-filters.md).
	AtOutput bool `json:"at_output,omitempty"`
	// StagePartials is the total number of partials the stage will produce
	// for this filter (== the stage's task count), stamped by the
	// coordinator at dispatch. When set, the emitting worker names its
	// partial key with an ".of<N>" suffix so an attach-on-arrival consumer
	// that discovers partials directly (DynamicFilterSpec.PartialPrefix)
	// learns the completeness target from any single partial — the
	// consumer usually dispatches before the emitter stage exists, so it
	// cannot be told the count in its own spec.
	StagePartials int `json:"stage_partials,omitempty"`
	// PartialPrefix is the exact S3 prefix the worker must upload this
	// filter's partials under, stamped by the coordinator alongside
	// StagePartials. The coordinator stamps the SAME value into consumer
	// DynamicFilterSpec.PartialPrefix, making it the single source of
	// truth for the partial key layout — the worker never reconstructs it.
	// Empty (legacy coordinator) ⇒ the worker falls back to the historical
	// queries/<task.QueryID>/dynfilter/<stageID>/ construction.
	PartialPrefix string `json:"partial_prefix,omitempty"`
	// GuardConsumes (guarded re-emit): FilterIDs of the emitting stage's
	// own attach-mode consumes whose blooms must retro-filter this emit's
	// buffered head rows at finalize. See the planner-side field of the
	// same name (physical.DynamicFilterEmit).
	GuardConsumes []string `json:"guard_consumes,omitempty"`
}

// DynamicFilterConsume, attached to a probe-scan Stage, instructs the
// coordinator to inject the matching BuildStats artifact into this stage's
// scan tasks. The stat-dep edge (SourceStageID added to Dependencies)
// guarantees the artifact is available before this stage dispatches.
type DynamicFilterConsume struct {
	FilterID      string `json:"filter_id"`
	SourceStageID string `json:"source_stage_id"`
	TargetColumn  string `json:"target_column"`
	KeyType       string `json:"key_type"`
}

// DynamicFilterPartialRef is a per-task sideband artifact reference returned
// in ResultNotification. Each build-scan task uploads its partial stats to
// the indicated S3 location; the coordinator fetches all partials for a
// FilterID and unions them.
type DynamicFilterPartialRef struct {
	FilterID string `json:"filter_id"`
	Bucket   string `json:"bucket"`
	Key      string `json:"key"`
}

// DynamicFilterSpec is the materialized stat the coordinator injects into a
// probe-scan task's OpSpec. Bloom is carried inline when the unioned bitset
// is small (≤ inlineThresholdBytes); otherwise BloomBucket+BloomKey point
// to the S3-staged artifact and the worker fetches it before scan init.
type DynamicFilterSpec struct {
	FilterID     string `json:"filter_id"`
	TargetColumn string `json:"target_column"`
	KeyType      string `json:"key_type"`

	HasRange bool  `json:"has_range,omitempty"`
	Min      int64 `json:"min,omitempty"`
	Max      int64 `json:"max,omitempty"`

	// Inline bloom (small filters): non-empty Bloom + BloomMask.
	Bloom     []uint64 `json:"bloom,omitempty"`
	BloomMask uint64   `json:"bloom_mask,omitempty"`

	// Staged bloom (large filters): non-empty BloomKey. BloomBits gives
	// the expected uint64 word count for size validation post-fetch.
	BloomBucket string `json:"bloom_bucket,omitempty"`
	BloomKey    string `json:"bloom_key,omitempty"`
	BloomWords  int    `json:"bloom_words,omitempty"`

	// Deferred marks an attach-on-arrival consume: the artifact at
	// BloomBucket/BloomKey may not exist yet when the task starts. The
	// worker begins scanning unfiltered, polls the key, and installs the
	// bloom mid-scan when it lands (drop-only semantics keep results
	// identical). A key that never appears (emitter withheld the filter)
	// degrades to an unfiltered scan, same as a missing filter today.
	Deferred bool `json:"deferred,omitempty"`

	// PartialPrefix (Deferred only) is the S3 prefix where the emitter
	// stage's per-task partials land. When set, the consumer's poll loop
	// additionally lists the prefix and ORs partials as they are uploaded,
	// activating the union the moment the last partial lands — instead of
	// waiting out emitter stage completion + coordinator merge + merged-key
	// staging. Activation still requires FULL coverage (every one of the
	// ".of<N>" partials merged): an incomplete union falsely rejects rows,
	// so partial-coverage filtering is never applied. Empty = merged-key
	// polling only (kill switch WADJET_DF_INCREMENTAL_PARTIALS=0).
	PartialPrefix string `json:"partial_prefix,omitempty"`
}
