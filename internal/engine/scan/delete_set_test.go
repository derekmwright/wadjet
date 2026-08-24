package scan

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// deleteSetShapes are the marker shapes a DELETE actually produces, plus the
// degenerate ones a corrupt or hand-written manifest can.
func deleteSetShapes() map[string][]int64 {
	scattered := make([]int64, 0, 1000)
	for i := 0; i < 1000; i++ {
		scattered = append(scattered, int64(i)*997)
	}
	contiguous := make([]int64, 0, 100000)
	for i := 0; i < 100000; i++ {
		contiguous = append(contiguous, int64(i))
	}
	return map[string][]int64{
		"empty":          nil,
		"single":         {0},
		"single-high":    {1 << 40},
		"contiguous":     contiguous,
		"scattered":      scattered,
		"two-runs":       {5, 6, 7, 100, 101},
		"unsorted":       {9, 1, 5, 2, 0},
		"duplicated":     {3, 3, 3, 4, 4},
		"negative-mixed": {-1, 2, 3},
	}
}

func TestDeleteSetRoundTripsEveryShape(t *testing.T) {
	for name, rows := range deleteSetShapes() {
		t.Run(name, func(t *testing.T) {
			want := NewDeleteSet(rows)
			got, err := DecodeDeleteSet(want.Encode())
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if want.Rows() != got.Rows() || want.Runs() != got.Runs() {
				t.Fatalf("round trip: rows %d/%d runs %d/%d",
					want.Rows(), got.Rows(), want.Runs(), got.Runs())
			}
			// Membership is what the scan actually asks, so compare that
			// rather than the internal slices — over a window that covers
			// every index in the set plus its neighbours.
			for _, r := range rows {
				if r < 0 {
					if got.Contains(r) {
						t.Fatalf("negative index %d is in the set", r)
					}
					continue
				}
				for _, probe := range []int64{r - 1, r, r + 1} {
					if want.Contains(probe) != got.Contains(probe) {
						t.Fatalf("Contains(%d): %v != %v", probe, want.Contains(probe), got.Contains(probe))
					}
				}
			}
		})
	}
}

// The set has to answer membership the way a linear scan of the raw indices
// would — the property every read path depends on.
func TestDeleteSetMembershipMatchesTheRawIndices(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for trial := 0; trial < 200; trial++ {
		n := rng.Intn(40)
		raw := make([]int64, 0, n)
		naive := make(map[int64]bool, n)
		for i := 0; i < n; i++ {
			v := int64(rng.Intn(64))
			raw = append(raw, v)
			naive[v] = true
		}
		d := NewDeleteSet(raw)
		for probe := int64(0); probe < 70; probe++ {
			if d.Contains(probe) != naive[probe] {
				t.Fatalf("trial %d: Contains(%d)=%v, want %v (raw=%v)", trial, probe, d.Contains(probe), naive[probe], raw)
			}
		}
		// Overlaps must agree with Contains over the same window.
		for lo := int64(0); lo < 70; lo += 7 {
			var any bool
			for r := lo; r < lo+7; r++ {
				any = any || naive[r]
			}
			if d.Overlaps(lo, 7) != any {
				t.Fatalf("trial %d: Overlaps(%d,7)=%v, want %v (raw=%v)", trial, lo, d.Overlaps(lo, 7), any, raw)
			}
		}
	}
}

// A truncated or hostile payload must be a returned error, never a panic and
// never a silently short set: a scan that cannot read its markers has to fail
// the task rather than answer with the deleted rows still in it.
func TestDecodeDeleteSetRejectsMalformedPayloads(t *testing.T) {
	full := NewDeleteSet([]int64{1, 2, 3, 900, 901}).Encode()
	for i := 1; i < len(full); i++ {
		if _, err := DecodeDeleteSet(full[:i]); err == nil {
			// A prefix that happens to be a whole number of complete pairs
			// is a legal (shorter) set — that is the only acceptable
			// non-error, and it must still be well-formed.
			set, _ := DecodeDeleteSet(full[:i])
			if set.Runs() == 0 && i > 0 {
				t.Fatalf("truncation at %d decoded to an empty set without erroring", i)
			}
		}
	}
	if _, err := DecodeDeleteSet([]byte{0x02, 0x00}); err == nil {
		t.Fatal("a zero-length run must be rejected")
	}
	if _, err := DecodeDeleteSet([]byte{0xff}); err == nil {
		t.Fatal("a truncated varint must be rejected")
	}
}

func FuzzDecodeDeleteSet(f *testing.F) {
	for _, rows := range deleteSetShapes() {
		f.Add(NewDeleteSet(rows).Encode())
	}
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
	f.Fuzz(func(t *testing.T, b []byte) {
		set, err := DecodeDeleteSet(b)
		if err != nil {
			return
		}
		// A decoded set must be internally consistent: re-encoding it and
		// decoding again yields the same membership.
		again, err := DecodeDeleteSet(set.Encode())
		if err != nil {
			t.Fatalf("re-decode of a self-encoded set failed: %v", err)
		}
		if again.Rows() != set.Rows() || again.Runs() != set.Runs() {
			t.Fatalf("not idempotent: rows %d/%d runs %d/%d", set.Rows(), again.Rows(), set.Runs(), again.Runs())
		}
		for probe := int64(0); probe < 128; probe++ {
			if set.Contains(probe) != again.Contains(probe) {
				t.Fatalf("membership drift at %d", probe)
			}
		}
	})
}

// ApplyDeleteMarkers works in FILE-ABSOLUTE coordinates and must intersect an
// existing selection rather than overwrite it — a scan-level filter that
// already dropped rows must not have them resurrected.
func TestApplyDeleteMarkersIsFileAbsoluteAndIntersects(t *testing.T) {
	newBatch := func(n int) *batch.RecordBatch {
		b := batch.NewRecordBatch([]pqt.Column{{Name: "v", Type: pqt.TypeInt64}}, n)
		for i := 0; i < n; i++ {
			b.Columns[0].Int64Data[i] = int64(i)
		}
		b.Len = n
		return b
	}
	active := func(b *batch.RecordBatch) []int64 {
		var out []int64
		if b.Sel == nil {
			for i := 0; i < b.Len; i++ {
				out = append(out, b.Columns[0].Int64Data[i])
			}
			return out
		}
		for _, i := range b.Sel {
			out = append(out, b.Columns[0].Int64Data[i])
		}
		return out
	}

	// Rows 10 and 12 of the FILE are deleted; this batch starts at file row
	// 10, so its local rows 0 and 2 go.
	b := newBatch(4)
	if !ApplyDeleteMarkers(b, 10, NewDeleteSet([]int64{10, 12})) {
		t.Fatal("batch dropped entirely")
	}
	if got := active(b); len(got) != 2 || got[0] != 1 || got[1] != 3 {
		t.Fatalf("file-absolute offset ignored: active=%v, want [1 3]", got)
	}

	// The same markers against a batch at offset 0 must touch nothing —
	// proof the offset is used and not the local index.
	b = newBatch(4)
	if !ApplyDeleteMarkers(b, 0, NewDeleteSet([]int64{10, 12})) {
		t.Fatal("batch dropped entirely")
	}
	if b.Sel != nil {
		t.Fatalf("out-of-range markers narrowed the batch: sel=%v", b.Sel)
	}

	// Intersection with an existing selection.
	b = newBatch(4)
	b.Sel = []uint32{1, 2, 3}
	if !ApplyDeleteMarkers(b, 0, NewDeleteSet([]int64{2})) {
		t.Fatal("batch dropped entirely")
	}
	if got := active(b); len(got) != 2 || got[0] != 1 || got[1] != 3 {
		t.Fatalf("existing selection not intersected: active=%v, want [1 3]", got)
	}

	// Everything deleted: the caller is told to drop the batch, not handed
	// an empty selection.
	b = newBatch(4)
	if ApplyDeleteMarkers(b, 0, NewDeleteSet([]int64{0, 1, 2, 3})) {
		t.Fatal("a fully-deleted batch must report false")
	}
}

// The wire encoding exists to keep a task spec under the NATS payload cap.
// This is the measurement that claim rests on: for every shape, the encoded
// form against the SAME markers as the catalog manifest holds them (a JSON
// array of decimal indices). It is a documented number, not a threshold to
// tune — the assertion is only that the wire form never LOSES to the
// manifest, which is what makes "any marker set the catalog can hold, the
// wire can carry" true.
func TestDeleteRunEncodingNeverLosesToTheManifestJSON(t *testing.T) {
	for name, rows := range deleteSetShapes() {
		if len(rows) == 0 {
			continue
		}
		wire := len(EncodeDeleteRuns(rows))
		raw, err := json.Marshal(rows)
		if err != nil {
			t.Fatalf("%s: marshal: %v", name, err)
		}
		t.Logf("%-14s rows=%-7d runs=%-6d wire=%-8d manifest_json=%-9d ratio=%.3f",
			name, len(rows), NewDeleteSet(rows).Runs(), wire, len(raw),
			float64(wire)/float64(len(raw)))
		if wire > len(raw) {
			t.Errorf("%s: wire encoding %d B exceeds the manifest's own %d B", name, wire, len(raw))
		}
	}
}

// The spec-size question #491 asks directly: a 1000-file table with sparse
// deletes. The per-task payload is what matters — a scan task reads a slice
// of the files — but even the whole-table union has to be nowhere near the
// 8 MB NATS cap.
func TestSparseDeletesOverAThousandFilesStayTiny(t *testing.T) {
	const files = 1000
	rng := rand.New(rand.NewSource(11))
	var total int
	for f := 0; f < files; f++ {
		rows := make([]int64, 0, 8)
		for i := 0; i < 8; i++ {
			rows = append(rows, int64(rng.Intn(1_000_000)))
		}
		key := fmt.Sprintf("tables/t/chunk_%08x.parquet", f)
		total += len(key) + len(EncodeDeleteRuns(rows))
	}
	t.Logf("1000 files x 8 sparse deletes: %d B of marker payload for the whole table", total)
	if total > 1<<20 {
		t.Fatalf("sparse markers over %d files cost %d B; expected well under 1 MiB", files, total)
	}
}
