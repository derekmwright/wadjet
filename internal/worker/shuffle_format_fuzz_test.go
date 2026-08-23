package worker

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/wshf"
)

// touchDecodedBatch reads every value out of a decoded batch. Decoding
// without panicking is only half the contract: a byte column whose offsets
// were accepted unchecked decodes fine and then panics in whichever
// operator first calls Value(), so the fuzz target must consume what it
// decoded to see that class at all.
func touchDecodedBatch(b *batch.RecordBatch) {
	for _, v := range b.Columns {
		if v == nil {
			continue
		}
		for i := 0; i < b.Len; i++ {
			if v.Nulls.IsNull(i) {
				continue
			}
			switch v.Type {
			case parquet.TypeBool:
				_ = v.BoolData[i]
			case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol, parquet.TypeDate:
				_ = v.Int32Data[i]
			case parquet.TypeInt64, parquet.TypeTimestamp, parquet.TypeIPv4, parquet.TypeMAC, parquet.TypeDuration:
				_ = v.Int64Data[i]
			case parquet.TypeFloat32:
				_ = v.Float32Data[i]
			case parquet.TypeFloat64:
				_ = v.Float64Data[i]
			case parquet.TypeString, parquet.TypeBytes, parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeUUID:
				_ = v.BytesData.Value(i)
			case parquet.TypeDecimal:
				if v.DecimalData.Data != nil {
					_ = v.DecimalData.Data[i]
				}
			}
		}
	}
}

// FuzzWSHFDecode is #422's gate: a WSHF payload arrives from the network
// (a worker's inline result on NATS, a peer's gRPC stream) or from a file
// that may have been truncated, and every count and length the decoder
// acts on comes out of those same untrusted bytes. Header, schema and
// chunk mutations must produce an error or a usable batch — never a panic,
// because the coordinator decodes inline results in a goroutine nothing
// above recovers.
//
// The seed corpus is every prefix of a small valid payload, which walks the
// truncation boundary across each field of the header, the schema and the
// first chunk's column segments, plus wider payloads covering every column
// class (the container types included) and the compressed envelopes.
func FuzzWSHFDecode(f *testing.F) {
	small := makeWshfBytesForFuzz()
	for i := 0; i <= len(small); i++ {
		f.Add(small[:i])
	}
	f.Add(buildMultiTypeWSHF(f, 3, 2, 33))
	f.Add(writeContainerWSHF(f, buildContainerBatch(f), []uint32{0, 2}))
	f.Add(CompressShuffleData(small))
	f.Add([]byte("WSHC\xff\xff\xff\xff"))
	f.Add([]byte("WSHZ\xff\xff\xff\xff"))
	// A header that promises far more than the payload can hold: the row
	// count is the one field the decoder allocates against.
	huge := append([]byte(nil), small...)
	binary.LittleEndian.PutUint32(huge[4:], 1)
	f.Add(huge)

	f.Fuzz(func(t *testing.T, data []byte) {
		// The coordinator's inline path: envelope sniff, then whole-payload
		// decode.
		if raw, err := wshf.Decompress(data); err == nil {
			if batches, err := wshf.DecodeBatches(raw); err == nil {
				for _, b := range batches {
					touchDecodedBatch(b)
				}
			}
		}

		// The worker's mmap path: one batch at a time, and the position
		// the drop-behind walk trusts must never run backwards or past the
		// payload.
		r, err := wshf.NewChunkReader(data)
		if err != nil {
			return
		}
		prev := 0
		for i := 0; i < 1<<12; i++ {
			b, err := r.Next()
			if pos := r.Pos(); pos < prev || pos > len(data) {
				t.Fatalf("chunk reader position %d left [%d, %d]", pos, prev, len(data))
			} else {
				prev = pos
			}
			if err != nil || b == nil {
				return
			}
			touchDecodedBatch(b)
		}
	})
}

// TestWSHFTruncationErrorsEverywhere is the #422 regression in table form:
// every prefix of a valid multi-type payload must come back as an error
// from both readers, never as a panic and never as a short answer that
// claims success. The fuzz target covers mutation; this pins truncation
// specifically, and runs in the ordinary suite.
func TestWSHFTruncationErrorsEverywhere(t *testing.T) {
	full := buildMultiTypeWSHF(t, 11, 2, 16)
	if _, err := wshf.DecodeBatches(full); err != nil {
		t.Fatalf("the intact payload must decode: %v", err)
	}
	for cut := 0; cut < len(full); cut++ {
		data := full[:cut]
		batches, err := wshf.DecodeBatches(data)
		if err == nil {
			// A prefix can only be legitimate if it holds every chunk the
			// header promises, which no strict prefix of this payload does.
			t.Fatalf("cut at %d/%d: DecodeBatches accepted a truncated payload (%d batches)",
				cut, len(full), len(batches))
		}
		r, rerr := wshf.NewChunkReader(data)
		if rerr != nil {
			continue
		}
		for {
			b, err := r.Next()
			if err != nil {
				break
			}
			if b == nil {
				t.Fatalf("cut at %d/%d: ChunkReader reported a clean EOF on a truncated payload",
					cut, len(full))
			}
		}
	}
}

// TestWSHFHostileLengthFieldsAreRejected pins the fields that are not
// merely short but implausible: a chunk that claims more rows than the
// payload could hold must be refused BEFORE the batch is allocated
// against that number.
func TestWSHFHostileLengthFieldsAreRejected(t *testing.T) {
	schema := []parquet.Column{{Name: "v", Type: parquet.TypeInt64}}
	var buf bytes.Buffer
	sw := newShuffleWriter(&buf, schema)
	if err := sw.writeHeader(); err != nil {
		t.Fatal(err)
	}
	b := batch.NewRecordBatch(schema, 4)
	for i := 0; i < 4; i++ {
		b.Columns[0].Nulls.SetValid(i)
		b.Columns[0].Int64Data[i] = int64(i)
	}
	if err := sw.writeChunk(b.Columns, nil, 4); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()
	binary.LittleEndian.PutUint32(data[4:], sw.numChunks)
	headerEnd := len(data) - (4 + 4 + 8 + 4 + 4*8) // row count + bitmap + data segment

	for _, tc := range []struct {
		name string
		mut  func([]byte)
	}{
		{"row count beyond the payload", func(d []byte) {
			binary.LittleEndian.PutUint32(d[headerEnd:], 1<<24)
		}},
		{"row count beyond the ceiling", func(d []byte) {
			binary.LittleEndian.PutUint32(d[headerEnd:], 1<<30)
		}},
		{"bitmap word count beyond the row count", func(d []byte) {
			binary.LittleEndian.PutUint32(d[headerEnd+4:], 1<<20)
		}},
		{"data length disagrees with the row count", func(d []byte) {
			binary.LittleEndian.PutUint32(d[headerEnd+4+4+8:], 7)
		}},
		{"column count beyond the ceiling", func(d []byte) {
			binary.LittleEndian.PutUint16(d[8:], 0xFFFF)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := append([]byte(nil), data...)
			tc.mut(d)
			if _, err := wshf.DecodeBatches(d); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}
