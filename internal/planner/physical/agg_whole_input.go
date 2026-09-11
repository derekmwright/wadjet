package physical

import "strings"

// aggNeedsWholeInput identifies aggregates that must consume each group's
// raw rows exactly once in one place, as a RawInputAggregate final (#291).
// Median/percentile/mode and min_by/max_by cannot merge their output as input;
// string_agg is also gated (concatenation preserves values, but not order).
// These states have no bounded merge summary; the full input crosses the
// exchange. Ungrouped input is Singleton with whole-column peak memory;
// grouped input clusters by keys across workers, bounded by the largest group.
// A mode fixture may agree by luck; the planner gate must prove engagement.
// See docs/internals/whole-input-aggregates.md for the design.
func aggNeedsWholeInput(fn string) bool {
	switch strings.ToLower(strings.TrimSpace(fn)) {
	case "median", "percentile_cont", "percentile_disc",
		"quantile_cont", "quantile_disc", "mode",
		"min_by", "max_by", "string_agg":
		return true
	}
	return false
}

// anyAggNeedsWholeInput reports whether any spec in the list is gated.
func anyAggNeedsWholeInput(specs []AggSpec) bool {
	for _, s := range specs {
		if aggNeedsWholeInput(s.Func) {
			return true
		}
	}
	return false
}
