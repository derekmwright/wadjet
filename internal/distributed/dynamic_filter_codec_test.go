package distributed

import (
	"bytes"
	"testing"
)

func TestDynamicFilterArtifactRoundTrip(t *testing.T) {
	orig := &DynamicFilterArtifact{
		KeyType:   "int64",
		HasRange:  true,
		Min:       17,
		Max:       9999,
		RowCount:  1234,
		Bloom:     []uint64{0x1, 0xdeadbeef, 0xffffffffffffffff, 0x0, 0x55},
		BloomMask: 4,
	}
	var buf bytes.Buffer
	if err := EncodeDynamicFilterArtifact(&buf, orig); err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := DecodeDynamicFilterArtifact(&buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.KeyType != orig.KeyType {
		t.Errorf("KeyType = %q, want %q", got.KeyType, orig.KeyType)
	}
	if got.HasRange != orig.HasRange || got.Min != orig.Min || got.Max != orig.Max {
		t.Errorf("range mismatch: got (%v, %d, %d), want (%v, %d, %d)",
			got.HasRange, got.Min, got.Max, orig.HasRange, orig.Min, orig.Max)
	}
	if got.RowCount != orig.RowCount {
		t.Errorf("RowCount = %d, want %d", got.RowCount, orig.RowCount)
	}
	if len(got.Bloom) != len(orig.Bloom) {
		t.Fatalf("Bloom len = %d, want %d", len(got.Bloom), len(orig.Bloom))
	}
	for i, w := range orig.Bloom {
		if got.Bloom[i] != w {
			t.Errorf("Bloom[%d] = %#x, want %#x", i, got.Bloom[i], w)
		}
	}
	if got.BloomMask != orig.BloomMask {
		t.Errorf("BloomMask = %d, want %d", got.BloomMask, orig.BloomMask)
	}
}

func TestDynamicFilterArtifactBadMagic(t *testing.T) {
	data := make([]byte, dfHeaderSize)
	copy(data[0:4], "ZZZZ")
	_, err := DecodeDynamicFilterArtifact(bytes.NewReader(data))
	if err == nil {
		t.Fatal("expected magic mismatch error")
	}
}

func TestDynamicFilterArtifactEmptyBloom(t *testing.T) {
	orig := &DynamicFilterArtifact{
		KeyType: "int32",
		// RowCount=0, no bloom; coordinator should treat as no-op.
	}
	var buf bytes.Buffer
	if err := EncodeDynamicFilterArtifact(&buf, orig); err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := DecodeDynamicFilterArtifact(&buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.RowCount != 0 || len(got.Bloom) != 0 {
		t.Errorf("expected empty artifact, got RowCount=%d len(Bloom)=%d", got.RowCount, len(got.Bloom))
	}
}
