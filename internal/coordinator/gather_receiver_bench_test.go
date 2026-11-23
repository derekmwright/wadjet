// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"

	dpv1 "github.com/derekmwright/wadjet/gen/dataplane/v1"
	"github.com/derekmwright/wadjet/internal/distributed"
)

// BenchmarkGatherHandleParsed is the receiver's per-message cost: one batch
// message through the acceptance rule and the decode/append, for a 1-row and
// a full 2,048-row payload. The receiver is replaced every 1,024 messages
// (outside the timer) so the accumulated result stays bounded.
func BenchmarkGatherHandleParsed(b *testing.B) {
	for _, rows := range []int{1, 2048} {
		b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
			payload := wshfInt64Payload(b, rows, 0)
			var r *gatherReceiver
			var gs *testGatherStream
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if i%1024 == 0 {
					b.StopTimer()
					r, gs = newTestGatherReceiver(0)
					b.StartTimer()
				}
				r.handleParsed(gs.batch(payload, rows))
			}
		})
	}
}

// BenchmarkGatherExpectTasks is the receiver's per-statement registration
// state: B/op for registering a fused stage's 24 tasks, and 1,024.
func BenchmarkGatherExpectTasks(b *testing.B) {
	for _, n := range []int{24, 1024} {
		ids := make([]string, n)
		for i := range ids {
			ids[i] = fmt.Sprintf("%08x", i)
		}
		b.Run(fmt.Sprintf("tasks=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				r := &gatherReceiver{done: make(chan struct{}, 1)}
				r.expectTasks(ids)
			}
		})
	}
}

// TestGatherWireBytesPerMessage reports the wire size of a gather batch and
// terminal on both transports, with the identity a real execution stamps
// (an 8-character task id, a 16-character execution id). Measurement, not
// an assertion: the 1709a note records base against tip.
func TestGatherWireBytesPerMessage(t *testing.T) {
	for _, rows := range []int{0, 1, 2048} {
		m := distributed.GatherBatchMsg{WorkerID: "worker-0", TaskID: "0b84691f", ExecutionID: "28fa5b6adf1af6ec"}
		if rows == 0 {
			m.Terminal, m.BatchCount = true, 3
		} else {
			m.Payload, m.RowCount, m.Seq = wshfInt64Payload(t, rows, 0), int32(rows), 3
		}
		js, err := distributed.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		pb := proto.Size(&dpv1.ResultBatch{
			QueryId: "375347dc-c112-473b-8ff9-edd67f312b8c", WorkerId: m.WorkerID, Terminal: m.Terminal,
			RowCount: m.RowCount, Payload: m.Payload, TaskId: m.TaskID, ExecutionId: m.ExecutionID,
			Seq: m.Seq, BatchCount: m.BatchCount,
		})
		t.Logf("rows=%d terminal=%v: nats_json=%d bytes, grpc_proto=%d bytes", rows, m.Terminal, len(js), pb)
	}
}
