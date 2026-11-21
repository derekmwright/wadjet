// SPDX-License-Identifier: MIT

//go:build race

package physical

// raceEnabled reports whether the race detector is compiled in; allocation
// totals are not stable under it, so allocation gates keep only their
// deterministic half there.
const raceEnabled = true
