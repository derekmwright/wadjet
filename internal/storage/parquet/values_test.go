package parquet

import (
	"encoding/binary"
	"math"
	"testing"
)

// --- Round-trip tests: construct → access → verify ---

func TestPlainInt32Values(t *testing.T) {
	tests := []struct {
		name string
		vals []int32
	}{
		{"empty", nil},
		{"single", []int32{42}},
		{"multiple", []int32{1, -1, 0, math.MaxInt32, math.MinInt32}},
		{"zeros", []int32{0, 0, 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := PlainInt32Values(tt.vals)
			if got := v.Count(); got != len(tt.vals) {
				t.Fatalf("Count() = %d, want %d", got, len(tt.vals))
			}
			got := v.Int32()
			if len(tt.vals) == 0 {
				if got != nil {
					t.Fatalf("Int32() = %v, want nil for empty", got)
				}
				return
			}
			if len(got) != len(tt.vals) {
				t.Fatalf("Int32() len = %d, want %d", len(got), len(tt.vals))
			}
			for i, want := range tt.vals {
				if got[i] != want {
					t.Errorf("Int32()[%d] = %d, want %d", i, got[i], want)
				}
			}
		})
	}
}

func TestPlainInt64Values(t *testing.T) {
	tests := []struct {
		name string
		vals []int64
	}{
		{"empty", nil},
		{"single", []int64{42}},
		{"extremes", []int64{math.MaxInt64, math.MinInt64, 0, -1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := PlainInt64Values(tt.vals)
			if got := v.Count(); got != len(tt.vals) {
				t.Fatalf("Count() = %d, want %d", got, len(tt.vals))
			}
			got := v.Int64()
			if len(tt.vals) == 0 {
				if got != nil {
					t.Fatalf("Int64() = %v, want nil for empty", got)
				}
				return
			}
			for i, want := range tt.vals {
				if got[i] != want {
					t.Errorf("Int64()[%d] = %d, want %d", i, got[i], want)
				}
			}
		})
	}
}

func TestPlainFloat32Values(t *testing.T) {
	tests := []struct {
		name string
		vals []float32
	}{
		{"empty", nil},
		{"basic", []float32{1.5, -0.0, 3.14}},
		{"special", []float32{float32(math.Inf(1)), float32(math.Inf(-1)), float32(math.NaN()), 0}},
		{"extremes", []float32{math.MaxFloat32, math.SmallestNonzeroFloat32}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := PlainFloat32Values(tt.vals)
			if got := v.Count(); got != len(tt.vals) {
				t.Fatalf("Count() = %d, want %d", got, len(tt.vals))
			}
			got := v.Float()
			if len(tt.vals) == 0 {
				if got != nil {
					t.Fatalf("Float() = %v, want nil for empty", got)
				}
				return
			}
			for i, want := range tt.vals {
				if math.IsNaN(float64(want)) {
					if !math.IsNaN(float64(got[i])) {
						t.Errorf("Float()[%d] = %v, want NaN", i, got[i])
					}
				} else if got[i] != want {
					t.Errorf("Float()[%d] = %v, want %v", i, got[i], want)
				}
			}
		})
	}
}

func TestPlainFloat64Values(t *testing.T) {
	tests := []struct {
		name string
		vals []float64
	}{
		{"empty", nil},
		{"basic", []float64{1.5, -0.0, 3.14159265358979}},
		{"special", []float64{math.Inf(1), math.Inf(-1), math.NaN()}},
		{"extremes", []float64{math.MaxFloat64, math.SmallestNonzeroFloat64}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := PlainFloat64Values(tt.vals)
			if got := v.Count(); got != len(tt.vals) {
				t.Fatalf("Count() = %d, want %d", got, len(tt.vals))
			}
			got := v.Double()
			if len(tt.vals) == 0 {
				if got != nil {
					t.Fatalf("Double() = %v, want nil for empty", got)
				}
				return
			}
			for i, want := range tt.vals {
				if math.IsNaN(want) {
					if !math.IsNaN(got[i]) {
						t.Errorf("Double()[%d] = %v, want NaN", i, got[i])
					}
				} else if got[i] != want {
					t.Errorf("Double()[%d] = %v, want %v", i, got[i], want)
				}
			}
		})
	}
}

func TestByteArrayValues(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		offsets []uint32
		count   int
	}{
		{"empty_nil", nil, nil, 0},
		{"empty_offsets", nil, []uint32{0}, 0},
		{"single", []byte("hello"), []uint32{0, 5}, 1},
		{"multiple", []byte("helloworld"), []uint32{0, 5, 10}, 2},
		{"with_empty_string", []byte("helloworld"), []uint32{0, 5, 5, 10}, 3},
		{"all_empty", nil, []uint32{0, 0, 0}, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := ByteArrayValues(tt.data, tt.offsets)
			if got := v.Count(); got != tt.count {
				t.Fatalf("Count() = %d, want %d", got, tt.count)
			}
			gotData, gotOffsets := v.ByteArray()
			if len(gotData) != len(tt.data) {
				t.Errorf("data len = %d, want %d", len(gotData), len(tt.data))
			}
			if len(gotOffsets) != len(tt.offsets) {
				t.Errorf("offsets len = %d, want %d", len(gotOffsets), len(tt.offsets))
			}
		})
	}
}

// --- Index accessor tests (used for statistics) ---

func TestInt32At(t *testing.T) {
	vals := []int32{100, -200, math.MaxInt32, math.MinInt32}
	v := PlainInt32Values(vals)
	for i, want := range vals {
		if got := v.Int32At(i); got != want {
			t.Errorf("Int32At(%d) = %d, want %d", i, got, want)
		}
	}
}

func TestInt64At(t *testing.T) {
	vals := []int64{100, -200, math.MaxInt64, math.MinInt64}
	v := PlainInt64Values(vals)
	for i, want := range vals {
		if got := v.Int64At(i); got != want {
			t.Errorf("Int64At(%d) = %d, want %d", i, got, want)
		}
	}
}

func TestFloatAt(t *testing.T) {
	vals := []float32{1.5, -3.14, math.MaxFloat32}
	v := PlainFloat32Values(vals)
	for i, want := range vals {
		if got := v.FloatAt(i); got != want {
			t.Errorf("FloatAt(%d) = %v, want %v", i, got, want)
		}
	}
}

func TestDoubleAt(t *testing.T) {
	vals := []float64{1.5, -3.14159, math.MaxFloat64}
	v := PlainFloat64Values(vals)
	for i, want := range vals {
		if got := v.DoubleAt(i); got != want {
			t.Errorf("DoubleAt(%d) = %v, want %v", i, got, want)
		}
	}
}

// --- PLAIN decoding tests (simulating raw Parquet page data) ---

func TestDecodePlainInt32(t *testing.T) {
	want := []int32{1, -1, 0, 42, math.MaxInt32}
	data := make([]byte, len(want)*4)
	for i, val := range want {
		binary.LittleEndian.PutUint32(data[i*4:], uint32(val))
	}
	v, err := DecodePlainInt32(data, len(want))
	if err != nil {
		t.Fatalf("DecodePlainInt32: %v", err)
	}
	if v.Count() != len(want) {
		t.Fatalf("Count() = %d, want %d", v.Count(), len(want))
	}
	got := v.Int32()
	for i, w := range want {
		if got[i] != w {
			t.Errorf("Int32()[%d] = %d, want %d", i, got[i], w)
		}
	}
}

func TestDecodePlainInt64(t *testing.T) {
	want := []int64{1, -1, math.MaxInt64, math.MinInt64}
	data := make([]byte, len(want)*8)
	for i, val := range want {
		binary.LittleEndian.PutUint64(data[i*8:], uint64(val))
	}
	v, err := DecodePlainInt64(data, len(want))
	if err != nil {
		t.Fatalf("DecodePlainInt64: %v", err)
	}
	if v.Count() != len(want) {
		t.Fatalf("Count() = %d, want %d", v.Count(), len(want))
	}
	got := v.Int64()
	for i, w := range want {
		if got[i] != w {
			t.Errorf("Int64()[%d] = %d, want %d", i, got[i], w)
		}
	}
}

func TestDecodePlainFloat(t *testing.T) {
	want := []float32{1.5, -0.0, math.MaxFloat32}
	data := make([]byte, len(want)*4)
	for i, val := range want {
		binary.LittleEndian.PutUint32(data[i*4:], math.Float32bits(val))
	}
	v, err := DecodePlainFloat(data, len(want))
	if err != nil {
		t.Fatalf("DecodePlainFloat: %v", err)
	}
	got := v.Float()
	for i, w := range want {
		if got[i] != w {
			t.Errorf("Float()[%d] = %v, want %v", i, got[i], w)
		}
	}
}

func TestDecodePlainDouble(t *testing.T) {
	want := []float64{1.5, -0.0, math.MaxFloat64, math.SmallestNonzeroFloat64}
	data := make([]byte, len(want)*8)
	for i, val := range want {
		binary.LittleEndian.PutUint64(data[i*8:], math.Float64bits(val))
	}
	v, err := DecodePlainDouble(data, len(want))
	if err != nil {
		t.Fatalf("DecodePlainDouble: %v", err)
	}
	got := v.Double()
	for i, w := range want {
		if got[i] != w {
			t.Errorf("Double()[%d] = %v, want %v", i, got[i], w)
		}
	}
}

func TestDecodePlainBoolean(t *testing.T) {
	tests := []struct {
		name string
		n    int
		bits []byte
		want []bool
	}{
		{"8_bools", 8, []byte{0b10110010}, []bool{false, true, false, false, true, true, false, true}},
		{"3_bools", 3, []byte{0b101}, []bool{true, false, true}},
		{"empty", 0, nil, nil},
		{"16_bools", 16, []byte{0xFF, 0x00},
			[]bool{true, true, true, true, true, true, true, true,
				false, false, false, false, false, false, false, false}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, err := DecodePlainBoolean(tt.bits, tt.n)
			if err != nil {
				t.Fatalf("DecodePlainBoolean: %v", err)
			}
			if v.Count() != tt.n {
				t.Fatalf("Count() = %d, want %d", v.Count(), tt.n)
			}
			raw := v.Boolean()
			for i, want := range tt.want {
				byteIdx := i / 8
				bitIdx := uint(i % 8)
				got := raw[byteIdx]&(1<<bitIdx) != 0
				if got != want {
					t.Errorf("Boolean bit %d = %v, want %v", i, got, want)
				}
			}
		})
	}
}

func TestDecodePlainByteArray(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		// Build PLAIN-encoded BYTE_ARRAY: [4-byte LE length][data]...
		strings := []string{"hello", "world", "", "x"}
		var buf []byte
		for _, s := range strings {
			length := make([]byte, 4)
			binary.LittleEndian.PutUint32(length, uint32(len(s)))
			buf = append(buf, length...)
			buf = append(buf, s...)
		}

		v, err := DecodePlainByteArray(buf, len(strings))
		if err != nil {
			t.Fatalf("DecodePlainByteArray: %v", err)
		}
		if v.Count() != len(strings) {
			t.Fatalf("Count() = %d, want %d", v.Count(), len(strings))
		}
		data, offsets := v.ByteArray()
		for i, want := range strings {
			got := string(data[offsets[i]:offsets[i+1]])
			if got != want {
				t.Errorf("value[%d] = %q, want %q", i, got, want)
			}
		}
	})

	t.Run("empty_input", func(t *testing.T) {
		v, err := DecodePlainByteArray(nil, 0)
		if err != nil {
			t.Fatalf("DecodePlainByteArray: %v", err)
		}
		if v.Count() != 0 {
			t.Fatalf("Count() = %d, want 0", v.Count())
		}
	})

	t.Run("all_empty_strings", func(t *testing.T) {
		// Three zero-length byte arrays.
		buf := make([]byte, 12) // 3 × 4-byte zero-length prefixes
		v, err := DecodePlainByteArray(buf, 3)
		if err != nil {
			t.Fatalf("DecodePlainByteArray: %v", err)
		}
		if v.Count() != 3 {
			t.Fatalf("Count() = %d, want 3", v.Count())
		}
		data, offsets := v.ByteArray()
		for i := 0; i < 3; i++ {
			got := string(data[offsets[i]:offsets[i+1]])
			if got != "" {
				t.Errorf("value[%d] = %q, want empty", i, got)
			}
		}
	})

	t.Run("truncated_data_is_an_error", func(t *testing.T) {
		// Data truncated mid-length-prefix. A partial decode used to be
		// returned as a success, which reads as "the rest of the column was
		// absent"; the page is corrupt and says so now.
		buf := []byte{5, 0, 0, 0, 'h', 'e', 'l', 'l', 'o', 3, 0}
		if _, err := DecodePlainByteArray(buf, 2); err == nil {
			t.Fatal("a page truncated mid-length-prefix decoded without error")
		}
	})

	t.Run("length_prefix_past_the_end_is_an_error", func(t *testing.T) {
		// One value whose length prefix claims far more than the page holds.
		// The pass-one bounds test only checked room for the PREFIX, so the
		// pass-two copy sliced past the end of the page and panicked.
		buf := []byte{0xFF, 0xFF, 0xFF, 0x7F, 'x'}
		if _, err := DecodePlainByteArray(buf, 1); err == nil {
			t.Fatal("a 2 GiB length prefix over a 5-byte page decoded without error")
		}
	})
}

func TestDecodePlainFixedLenByteArray(t *testing.T) {
	tests := []struct {
		name       string
		typeLength int
		n          int
		data       []byte
	}{
		{"uuid_16byte", 16, 2, make([]byte, 32)},
		{"decimal_4byte", 4, 3, make([]byte, 12)},
		{"single", 8, 1, []byte{1, 2, 3, 4, 5, 6, 7, 8}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Fill with identifiable pattern.
			for i := range tt.data {
				tt.data[i] = byte(i)
			}
			v, err := DecodePlainFixedLenByteArray(tt.data, tt.n, tt.typeLength)
			if err != nil {
				t.Fatalf("DecodePlainFixedLenByteArray: %v", err)
			}
			if v.Count() != tt.n {
				t.Fatalf("Count() = %d, want %d", v.Count(), tt.n)
			}
			data, offsets := v.ByteArray()
			for i := 0; i < tt.n; i++ {
				got := data[offsets[i]:offsets[i+1]]
				if len(got) != tt.typeLength {
					t.Errorf("value[%d] length = %d, want %d", i, len(got), tt.typeLength)
				}
				// Verify first byte of each value matches expected pattern.
				expectedFirst := byte(i * tt.typeLength)
				if got[0] != expectedFirst {
					t.Errorf("value[%d][0] = %d, want %d", i, got[0], expectedFirst)
				}
			}
		})
	}
}

// --- RawValues test ---

func TestRawValues(t *testing.T) {
	data := make([]byte, 20) // 5 × 4 bytes
	for i := 0; i < 5; i++ {
		binary.LittleEndian.PutUint32(data[i*4:], uint32(i*10))
	}
	v := RawValues(PhysicalInt32, data, 5)
	if v.Count() != 5 {
		t.Fatalf("Count() = %d, want 5", v.Count())
	}
	got := v.Int32()
	for i, want := range []int32{0, 10, 20, 30, 40} {
		if got[i] != want {
			t.Errorf("Int32()[%d] = %d, want %d", i, got[i], want)
		}
	}
}

// --- Format type tests ---

func TestPhysicalTypeString(t *testing.T) {
	tests := []struct {
		t    PhysicalType
		want string
	}{
		{PhysicalBoolean, "BOOLEAN"},
		{PhysicalInt32, "INT32"},
		{PhysicalInt64, "INT64"},
		{PhysicalInt96, "INT96"},
		{PhysicalFloat, "FLOAT"},
		{PhysicalDouble, "DOUBLE"},
		{PhysicalByteArray, "BYTE_ARRAY"},
		{PhysicalFixedLenByteArray, "FIXED_LEN_BYTE_ARRAY"},
		{PhysicalType(99), "UNKNOWN"},
	}
	for _, tt := range tests {
		if got := tt.t.String(); got != tt.want {
			t.Errorf("%d.String() = %q, want %q", tt.t, got, tt.want)
		}
	}
}

func TestPhysicalTypeByteWidth(t *testing.T) {
	tests := []struct {
		t    PhysicalType
		want int
	}{
		{PhysicalBoolean, 0},
		{PhysicalInt32, 4},
		{PhysicalFloat, 4},
		{PhysicalInt64, 8},
		{PhysicalDouble, 8},
		{PhysicalInt96, 12},
		{PhysicalByteArray, 0},
		{PhysicalFixedLenByteArray, 0},
	}
	for _, tt := range tests {
		if got := tt.t.ByteWidth(); got != tt.want {
			t.Errorf("%s.ByteWidth() = %d, want %d", tt.t, got, tt.want)
		}
	}
}

func TestEncodingString(t *testing.T) {
	tests := []struct {
		e    Encoding
		want string
	}{
		{EncodingPlain, "PLAIN"},
		{EncodingRLE, "RLE"},
		{EncodingDeltaBinaryPacked, "DELTA_BINARY_PACKED"},
		{EncodingRLEDictionary, "RLE_DICTIONARY"},
		{EncodingByteStreamSplit, "BYTE_STREAM_SPLIT"},
		{Encoding(99), "UNKNOWN"},
	}
	for _, tt := range tests {
		if got := tt.e.String(); got != tt.want {
			t.Errorf("%d.String() = %q, want %q", tt.e, got, tt.want)
		}
	}
}

func TestCompressionCodecString(t *testing.T) {
	tests := []struct {
		c    CompressionCodec
		want string
	}{
		{CodecNone, "UNCOMPRESSED"},
		{CodecSnappy, "SNAPPY"},
		{CodecGzip, "GZIP"},
		{CodecLZ4, "LZ4"},
		{CodecZstd, "ZSTD"},
		{CodecLZ4Raw, "LZ4_RAW"},
		{CompressionCodec(99), "UNKNOWN"},
	}
	for _, tt := range tests {
		if got := tt.c.String(); got != tt.want {
			t.Errorf("%d.String() = %q, want %q", tt.c, got, tt.want)
		}
	}
}

// --- accessor guards ---
//
// Every fixed-width accessor is an unsafe.Slice over the page's raw bytes.
// Two things can make that cast run off the end of its backing array, and
// both reach this package from outside it: a physical type that is not the
// one the caller is reading (the row reader decodes a column as the type the
// CATALOG names — see retypeFromCatalog), and a value count that comes from
// a page header in an untrusted file. A mismatch answers nil; every caller
// loops under len(src), so nil degrades to "nothing decoded" and the layer
// above reports it, instead of returning adjacent heap as query results.

func TestValuesAccessorsRefuseWrongPhysicalType(t *testing.T) {
	// 16 bytes: four int32s, two int64s, four float32s, two float64s — so
	// every wrong-width cast is a plausible-looking read rather than an
	// obviously empty one.
	data := make([]byte, 16)
	for i := range data {
		data[i] = byte(i + 1)
	}
	physTypes := []PhysicalType{
		PhysicalBoolean, PhysicalInt32, PhysicalInt64,
		PhysicalFloat, PhysicalDouble, PhysicalByteArray,
	}
	accessors := []struct {
		name string
		want PhysicalType
		n    func(Values) int
	}{
		{"Int32", PhysicalInt32, func(v Values) int { return len(v.Int32()) }},
		{"Int64", PhysicalInt64, func(v Values) int { return len(v.Int64()) }},
		{"Float", PhysicalFloat, func(v Values) int { return len(v.Float()) }},
		{"Double", PhysicalDouble, func(v Values) int { return len(v.Double()) }},
		{"Boolean", PhysicalBoolean, func(v Values) int { return len(v.Boolean()) }},
	}
	for _, pt := range physTypes {
		for _, a := range accessors {
			v := RawValues(pt, data, 2)
			got := a.n(v)
			if pt == a.want {
				if got == 0 {
					t.Errorf("%s() on %s values returned nothing", a.name, pt)
				}
				continue
			}
			if got != 0 {
				t.Errorf("%s() on %s values returned %d elements, want none", a.name, pt, got)
			}
		}
	}
}

// TestValuesAccessorsRefuseACountTheBytesCannotBack: count is a page-header
// claim. A count larger than the bytes can back must not widen the
// unsafe.Slice — and it must not quietly SHRINK it either. A truncated slice
// answers a read of N values with fewer, which every unpack loop accepts as
// "the rest were absent": the same silent-wrong-answer the physical-type
// refusal exists to prevent. Refusal is the answer; the read sites turn it
// into a named error.
func TestValuesAccessorsRefuseACountTheBytesCannotBack(t *testing.T) {
	data := make([]byte, 16)
	cases := []struct {
		name string
		got  int
	}{
		{"Int32", len(RawValues(PhysicalInt32, data, 1<<20).Int32())},
		{"Int64", len(RawValues(PhysicalInt64, data, 1<<20).Int64())},
		{"Float", len(RawValues(PhysicalFloat, data, 1<<20).Float())},
		{"Double", len(RawValues(PhysicalDouble, data, 1<<20).Double())},
	}
	for _, tc := range cases {
		if tc.got != 0 {
			t.Errorf("%s() over 16 bytes with a count of 2^20 returned %d elements, want none",
				tc.name, tc.got)
		}
	}
	// A count the bytes DO back still reads.
	if n := len(RawValues(PhysicalInt64, data, 2).Int64()); n != 2 {
		t.Errorf("Int64() over 16 bytes with a count of 2 returned %d elements, want 2", n)
	}
}

// TestDecodePlainRefusesAnInflatedValueCount is the page-header side of the
// same claim, at the decoder that used to slice data[:n*width] before any
// Values existed to check: a header saying 1,048,576 values over a
// 1000-value body panicked with a slice-bounds error inside the scan
// errgroup.
func TestDecodePlainRefusesAnInflatedValueCount(t *testing.T) {
	body := make([]byte, 1000*8)
	const inflated = 1 << 20
	cases := []struct {
		name string
		err  error
	}{
		{"Int32", errOf(func() error { _, e := DecodePlainInt32(body, inflated); return e })},
		{"Int64", errOf(func() error { _, e := DecodePlainInt64(body, inflated); return e })},
		{"Float", errOf(func() error { _, e := DecodePlainFloat(body, inflated); return e })},
		{"Double", errOf(func() error { _, e := DecodePlainDouble(body, inflated); return e })},
		{"Boolean", errOf(func() error { _, e := DecodePlainBoolean(body, inflated); return e })},
		{"ByteArray", errOf(func() error { _, e := DecodePlainByteArray(body, inflated); return e })},
		{"FLBA", errOf(func() error { _, e := DecodePlainFixedLenByteArray(body, inflated, 16); return e })},
	}
	for _, tc := range cases {
		if tc.err == nil {
			t.Errorf("DecodePlain%s with an inflated value count returned no error", tc.name)
		}
	}
	// Negative counts are the same class of untrusted header field.
	if _, err := DecodePlainInt64(body, -1); err == nil {
		t.Error("DecodePlainInt64 with a negative value count returned no error")
	}
}

func errOf(f func() error) error { return f() }

// TestValuesAtAccessorsBoundsChecked: the statistics path indexes single
// elements out of the same untrusted bytes.
func TestValuesAtAccessorsBoundsChecked(t *testing.T) {
	v := RawValues(PhysicalInt64, make([]byte, 8), 1)
	for _, tc := range []struct {
		name string
		got  float64
	}{
		{"Int32At", float64(v.Int32At(1 << 20))},
		{"Int64At", float64(v.Int64At(1 << 20))},
		{"FloatAt", float64(v.FloatAt(1 << 20))},
		{"DoubleAt", v.DoubleAt(1 << 20)},
		{"Int64At(-1)", float64(v.Int64At(-1))},
	} {
		if tc.got != 0 {
			t.Errorf("%s past the end returned %v, want 0", tc.name, tc.got)
		}
	}
}
