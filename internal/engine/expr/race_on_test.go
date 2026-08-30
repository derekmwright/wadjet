//go:build race

package expr

// raceEnabled reports whether the race detector is compiled in; allocation
// counts are not stable under it, so alloc gates keep only their
// correctness half there.
const raceEnabled = true
