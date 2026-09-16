// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

// preservesBuildSide reports whether a canonical join kind emits build-side
// rows that found no probe partner — the rows a RIGHT or FULL join exists to
// preserve, produced after probing by HashJoinProbe.FlushUnmatchedRows.
//
// Every distributed layout that REPLICATES the build side across tasks is
// unsound for these: each task holds the whole build and sees only its slice
// of the probe, so each would emit the same unmatched rows. Broadcast
// (walkStages) and skew-split (coordinator.planSkewSplitTasks) both gate on
// this; the hash-shuffle layout is sound because a partition's build and
// probe rows land on the same task.
func preservesBuildSide(jt string) bool {
	return jt == "right" || jt == "full"
}
