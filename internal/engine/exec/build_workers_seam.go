// SPDX-License-Identifier: MIT

package exec

import "sync/atomic"

// forceSerialJoinBuild, when set, makes HashJoin.Build take the routes a
// one-CPU host takes: the SERIAL key-only build (which records the build keys'
// min/max for the dynamic range filter) instead of the parallel one (which does
// not). CI and the gate run on many cores, so without it the serial route —
// and with it the dynamic range filter on semi/anti joins — is exercised only
// by `taskset -c 0` runs (#1677).
var forceSerialJoinBuild atomic.Bool

// ForceSerialJoinBuildForTest makes every hash-join build take the one-CPU
// routes until restore is called. TEST ONLY.
func ForceSerialJoinBuildForTest() (restore func()) {
	prev := forceSerialJoinBuild.Swap(true)
	return func() { forceSerialJoinBuild.Store(prev) }
}

// joinBuildWorkers is the worker count HashJoin.Build routes by: the host's
// CPUs, or one under ForceSerialJoinBuildForTest.
func joinBuildWorkers(cpus int) int {
	if forceSerialJoinBuild.Load() {
		return 1
	}
	return cpus
}
