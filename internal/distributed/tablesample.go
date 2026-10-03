// SPDX-License-Identifier: AGPL-3.0-only

package distributed

// TableSampleSpec is a scan's TABLESAMPLE as a scan fragment carries it
// (#1411): the method and the argument's VALUE — read as PostgreSQL's real
// once, when the coordinator planned the statement (physical.
// TablesampleArgument), never re-read from text on the worker. Percent is
// that float4 value widened to float64, which JSON carries exactly; the
// coordinator refuses a NULL, NaN or out-of-range argument (2202H) before it
// dispatches the scan, so the value that arrives is in [0, 100].
//
// The worker applies the engine's sampler (exec.NewSampledSource) to the
// scan's source, over the rows the scan selects (a DELETE's markers) — the
// code the single-process scan runs.
type TableSampleSpec struct {
	Method  string  `json:"method"`  // "BERNOULLI" or "SYSTEM"
	Percent float64 `json:"percent"` // 0..100, as real
	Null    bool    `json:"null,omitempty"`
}
