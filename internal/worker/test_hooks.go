// SPDX-License-Identifier: AGPL-3.0-only

package worker

import (
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/distributed"
)

// Test-only seams for the #1709 redelivery regressions, which run real
// workers against a real JetStream and must order two executions of one
// task deterministically. Both are nil in production; the cost on the hot
// path is one atomic load.

// TaskDeliveryHook, when set, runs for every task delivery after the poison
// defense and before execution. delivery is the JetStream delivery count (1
// on the gRPC data plane) and inProgress extends the delivery's AckWait, so
// a test can hold one delivery unacked until the server redelivers it.
type TaskDeliveryHook func(task *distributed.Task, delivery uint64, workerID string, inProgress func())

// GatherFinalizeHook, when set, runs as a gather sink starts its terminal,
// after every batch has been sent.
type GatherFinalizeHook func(task distributed.Task)

// ResultPublishHook, when set, runs before each attempt to deliver a task's
// result notification; a non-nil error stands in for that attempt failing,
// so a test can drive the worker's own Nak after three failed attempts.
type ResultPublishHook func(task distributed.Task, attempt int) error

var (
	taskDeliveryHook   atomic.Pointer[TaskDeliveryHook]
	gatherFinalizeHook atomic.Pointer[GatherFinalizeHook]
	resultPublishHook  atomic.Pointer[ResultPublishHook]
)

// SetResultPublishHookForTest installs h (nil removes it).
func SetResultPublishHookForTest(h ResultPublishHook) {
	if h == nil {
		resultPublishHook.Store(nil)
		return
	}
	resultPublishHook.Store(&h)
}

// SetTaskDeliveryHookForTest installs h (nil removes it).
func SetTaskDeliveryHookForTest(h TaskDeliveryHook) {
	if h == nil {
		taskDeliveryHook.Store(nil)
		return
	}
	taskDeliveryHook.Store(&h)
}

// SetGatherFinalizeHookForTest installs h (nil removes it).
func SetGatherFinalizeHookForTest(h GatherFinalizeHook) {
	if h == nil {
		gatherFinalizeHook.Store(nil)
		return
	}
	gatherFinalizeHook.Store(&h)
}
