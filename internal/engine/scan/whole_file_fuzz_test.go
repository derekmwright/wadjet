package scan

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"testing"

	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// FuzzWholeFileMutation is the whole-artifact target: a REAL parquet file,
// carrying every type the engine has, with random bytes changed anywhere in
// it — footer, page headers, page bodies. Both read paths then get it.
//
// The unit fuzz targets each drive one decoder with one hand-made input, and
// that is exactly what let this class survive: nothing in a page-values fuzz
// can produce a footer whose data_page_offset is negative, because the
// footer is somebody else's input. The six crashers seeded below were all
// found this way and none of them was reachable from the existing targets —
// five were footer offsets used as slice indices, one was a page loop
// walking past the row group's own row count.
//
// The property is total and deliberately weak: an error, or data. Never a
// fault. A mutated file has no correct contents, so there is nothing else to
// assert — and nothing weaker to hide behind either, since a panic here is a
// worker process in production.
func FuzzWholeFileMutation(f *testing.F) {
	// The crashers, in the (seed, nmut) coordinates of the mutation scheme
	// below. They are kept as seeds rather than as separate tests because
	// what they pin is the SHAPE — a footer offset, a page count — and the
	// fuzzer re-derives neighbours of each on every run.
	for _, c := range [][2]int{
		{221642, 5}, {126704, -47}, {126704, -49},
		{142690, 7}, {126538, -65}, {-10, 1},
	} {
		f.Add(c[0], c[1], fixtureAllTypes)
	}
	// Found by this target on the tree that had already fixed the six above:
	// a row group whose num_rows went NEGATIVE, which
	// batch.NewRecordBatch turned into "makeslice: len out of range" before
	// a single page was read. The footer is validated on open now.
	f.Add(-19, 62, fixtureAllTypes)
	// A few plain shapes so the corpus starts with both regions covered.
	f.Add(1, 1, fixtureAllTypes)
	f.Add(2, -1, fixtureAllTypes)
	f.Add(3, 200, fixtureAllTypes)
	f.Add(4, 0, fixtureAllTypes)

	// The dictionary arm. wadjet's own writer never emits a dictionary page,
	// so the all-types fixture cannot reach the dictionary gathers AT ALL —
	// which is why 60s of this target never found #433, a vacuous bounds
	// check that panicked on an empty dictionary page with a live index
	// stream. The second fixture is a dictionary-encoded BYTE_ARRAY chunk,
	// where a mutated dictionary page header is one byte away from that
	// shape and a mutated index stream is one byte away from every other
	// out-of-range gather.
	f.Add(1, 1, fixtureDictString)
	f.Add(2, -1, fixtureDictString)
	f.Add(5, 3, fixtureDictString)
	f.Add(7, -9, fixtureDictString)
	f.Add(11, 40, fixtureDictString)

	f.Fuzz(func(t *testing.T, seed, nmut, fixture int) {
		fx := fuzzFixture(t, fixture)
		readMutatedBothPaths(t, mutateFile(fx.raw, seed, nmut), fx.native, fx.row)
	})
}

// The fixtures this target mutates. A negative or unknown selector folds onto
// the all-types file rather than skipping, so the fuzzer never spends inputs
// on a no-op.
const (
	fixtureAllTypes = iota
	fixtureDictString
	numFuzzFixtures
)

type mutationFixture struct {
	raw    []byte
	native []pqt.Column
	row    []pqt.Column
}

func fuzzFixture(t testing.TB, which int) mutationFixture {
	t.Helper()
	which = ((which % numFuzzFixtures) + numFuzzFixtures) % numFuzzFixtures
	if which == fixtureDictString {
		col := colFor(pqt.TypeString)
		return mutationFixture{
			raw:    dictStringFile(t),
			native: []pqt.Column{col},
			row:    []pqt.Column{col},
		}
	}
	return mutationFixture{
		raw:    allTypesFile(),
		native: nativeAllTypesSchema(),
		row:    allTypesSchema().Columns,
	}
}

// dictStringFile is a single-column BYTE_ARRAY chunk rewritten as a
// dictionary page plus a live RLE_DICTIONARY index stream — the shape no
// wadjet-written file has. Built once: the rewrite parses and re-emits a
// footer, which is not work to repeat per fuzz input.
var (
	dictStringOnce sync.Once
	dictStringRaw  []byte
)

func dictStringFile(t testing.TB) []byte {
	t.Helper()
	dictStringOnce.Do(func() {
		dictStringRaw = dictEncodeOneColumnFile(t, writeMatrixFile(t, pqt.TypeString))
	})
	return dictStringRaw
}

// TestWholeFileMutationCrashers runs the six known crashers as an ordinary
// test, so they are checked by `go test` and not only under -fuzz.
func TestWholeFileMutationCrashers(t *testing.T) {
	for _, c := range [][2]int{
		{221642, 5}, {126704, -47}, {126704, -49},
		{142690, 7}, {126538, -65}, {-10, 1},
		{-19, 62},
	} {
		t.Run(fmt.Sprintf("seed%d_nmut%d", c[0], c[1]), func(t *testing.T) {
			fx := fuzzFixture(t, fixtureAllTypes)
			readMutatedBothPaths(t, mutateFile(fx.raw, c[0], c[1]), fx.native, fx.row)
		})
	}
}

// TestAllTypesFixtureReadsBack pins the fixture itself: UNMUTATED, the row
// path must hand back exactly what was written, the containers-inside-
// containers included. A fixture the reader cannot read whole is a fuzz
// target that tests the error path and nothing else — and these three
// columns are the only ones in the corpus that make the assembler recurse.
func TestAllTypesFixtureReadsBack(t *testing.T) {
	r, err := pqt.NewReaderFromBytes(allTypesFile())
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	rows, err := r.ReadRowsAs(allTypesSchema().Columns, nil)
	if err != nil {
		t.Fatalf("ReadRowsAs: %v", err)
	}
	if len(rows) != 24 {
		t.Fatalf("read %d rows, want 24", len(rows))
	}
	nested := map[string]func(i int) any{
		"c_map_arr": func(i int) any {
			return map[string]any{"k": []any{int64(i), int64(i + 1)}}
		},
		"c_arr_arr": func(i int) any {
			return []any{[]any{int64(i)}, []any{int64(i + 1), int64(i + 2)}}
		},
		"c_row_map": func(i int) any {
			return map[string]any{"f_int": int64(i), "f_map": map[string]any{"k": int64(i)}}
		},
	}
	// The fixture nulls column j on row i when (i+j)%7 == 0; a NULL column
	// is an absent key.
	colIdx := map[string]int{}
	for j, c := range allTypesSchema().Columns {
		colIdx[c.Name] = j
	}
	for name, want := range nested {
		j := colIdx[name]
		for i, row := range rows {
			got, present := row[name]
			if (i+j)%7 == 0 {
				if present {
					t.Errorf("row %d %s: %#v, want absent", i, name, got)
				}
				continue
			}
			if !reflect.DeepEqual(got, want(i)) {
				t.Errorf("row %d %s:\n   got %#v\n  want %#v", i, name, got, want(i))
			}
		}
	}
}

// readMutatedBothPaths is the property itself: whatever the bytes say, both
// readers either refuse the file or hand back values that can be touched.
func readMutatedBothPaths(t *testing.T, raw []byte, nativeSchema, rowSchema []pqt.Column) {
	t.Helper()
	// No recover: a panic in a decode goroutine is not recoverable by the
	// caller in production either (the scan fans out per column under an
	// errgroup), so catching it here would test something the engine does
	// not get to do.
	if fr, err := pqt.OpenFileReaderFromBytes(raw); err == nil {
		batches, err := ReadFileBatchesNative(fr, nativeSchema, nil)
		if err == nil {
			for _, b := range batches {
				if b == nil {
					continue
				}
				for _, col := range b.Columns {
					for i := 0; i < b.Len; i++ {
						_ = col.GetValue(i)
					}
				}
			}
		}
	}
	if r, err := pqt.NewReaderFromBytes(raw); err == nil {
		_, _ = r.ReadRowsAs(rowSchema, nil)
	}
}

// --- the fixture ---

var (
	allTypesOnce sync.Once
	allTypesRaw  []byte
)

// allTypesSchema is one column of every TypeID the engine has, nested types
// included. The nested ones matter: they are what routes a table to the ROW
// reader, so a file that carries them exercises both paths from one fixture.
func allTypesSchema() pqt.Schema {
	return pqt.Schema{Columns: []pqt.Column{
		{Name: "c_bool", Type: pqt.TypeBool, Nullable: true},
		{Name: "c_int32", Type: pqt.TypeInt32, Nullable: true},
		{Name: "c_int64", Type: pqt.TypeInt64, Nullable: true},
		{Name: "c_float32", Type: pqt.TypeFloat32, Nullable: true},
		{Name: "c_float64", Type: pqt.TypeFloat64, Nullable: true},
		{Name: "c_string", Type: pqt.TypeString, Nullable: true},
		{Name: "c_bytes", Type: pqt.TypeBytes, Nullable: true},
		{Name: "c_timestamp", Type: pqt.TypeTimestamp, Nullable: true},
		{Name: "c_ipv4", Type: pqt.TypeIPv4, Nullable: true},
		{Name: "c_ipv6", Type: pqt.TypeIPv6, Nullable: true},
		{Name: "c_cidr", Type: pqt.TypeCIDR, Nullable: true},
		{Name: "c_mac", Type: pqt.TypeMAC, Nullable: true},
		{Name: "c_port", Type: pqt.TypePort, Nullable: true},
		{Name: "c_protocol", Type: pqt.TypeProtocol, Nullable: true},
		{Name: "c_duration", Type: pqt.TypeDuration, Nullable: true},
		{Name: "c_uuid", Type: pqt.TypeUUID, Nullable: true},
		{Name: "c_date", Type: pqt.TypeDate, Nullable: true},
		{Name: "c_decimal", Type: pqt.TypeDecimal, Precision: 18, Scale: 2, Nullable: true},
		{Name: "c_vector", Type: pqt.TypeVector, Dimension: 4, Nullable: true},
		{Name: "c_array", Type: pqt.TypeArray, Nullable: true,
			ElementType: &pqt.Column{Name: "element", Type: pqt.TypeInt64, Nullable: true}},
		{Name: "c_row", Type: pqt.TypeRow, Nullable: true, Fields: []pqt.Column{
			{Name: "f_int", Type: pqt.TypeInt64, Nullable: true},
			{Name: "f_str", Type: pqt.TypeString, Nullable: true},
		}},
		{Name: "c_map", Type: pqt.TypeMap, Nullable: true,
			ElementType: &pqt.Column{Name: "kv", Type: pqt.TypeRow, Fields: []pqt.Column{
				{Name: "key", Type: pqt.TypeString},
				{Name: "value", Type: pqt.TypeInt64, Nullable: true},
			}}},
		// A container inside a container, which single-level columns do not
		// reach: the recursive assembler (#409) only descends past one level
		// for these shapes, and the levels it descends by are the ones a
		// mutated page header desynchronises. Without them the target ran
		// every mutation through an assembler that never recursed.
		{Name: "c_map_arr", Type: pqt.TypeMap, Nullable: true,
			ElementType: &pqt.Column{Name: "kv", Type: pqt.TypeRow, Fields: []pqt.Column{
				{Name: "key", Type: pqt.TypeString},
				{Name: "value", Type: pqt.TypeArray, Nullable: true,
					ElementType: &pqt.Column{Name: "element", Type: pqt.TypeInt64, Nullable: true}},
			}}},
		{Name: "c_arr_arr", Type: pqt.TypeArray, Nullable: true,
			ElementType: &pqt.Column{Name: "element", Type: pqt.TypeArray, Nullable: true,
				ElementType: &pqt.Column{Name: "element", Type: pqt.TypeInt64, Nullable: true}}},
		{Name: "c_row_map", Type: pqt.TypeRow, Nullable: true, Fields: []pqt.Column{
			{Name: "f_int", Type: pqt.TypeInt64, Nullable: true},
			{Name: "f_map", Type: pqt.TypeMap, Nullable: true,
				ElementType: &pqt.Column{Name: "kv", Type: pqt.TypeRow, Fields: []pqt.Column{
					{Name: "key", Type: pqt.TypeString},
					{Name: "value", Type: pqt.TypeInt64, Nullable: true},
				}}},
		}},
	}}
}

// nativeAllTypesSchema is the same list without ARRAY and MAP, which the
// native reader refuses by design (their leaves do not resolve by column
// name; such tables go to the row reader) — and without a ROW carrying one,
// for the same reason one level down: the native path reads a ROW field as a
// leaf at col.field, and a field that is a container is a GROUP.
func nativeAllTypesSchema() []pqt.Column {
	var out []pqt.Column
	for _, c := range allTypesSchema().Columns {
		if c.Type == pqt.TypeArray || c.Type == pqt.TypeMap {
			continue
		}
		nestedField := false
		for _, f := range c.Fields {
			if f.Type == pqt.TypeArray || f.Type == pqt.TypeMap || f.Type == pqt.TypeRow {
				nestedField = true
			}
		}
		if nestedField {
			continue
		}
		out = append(out, c)
	}
	return out
}

func allTypesFile() []byte {
	allTypesOnce.Do(func() {
		schema := allTypesSchema()
		var buf bytes.Buffer
		w, err := pqt.NewWriter(&buf, schema, pqt.DefaultWriterConfig())
		if err != nil {
			panic(fmt.Sprintf("all-types fixture writer: %v", err))
		}
		rows := make([]map[string]any, 24)
		for i := range rows {
			rows[i] = map[string]any{
				"c_bool":      i%2 == 0,
				"c_int32":     int64(i),
				"c_int64":     int64(i) * 1_000_000,
				"c_float32":   float64(i) + 0.5,
				"c_float64":   float64(i) * 1.25,
				"c_string":    fmt.Sprintf("row-%02d", i),
				"c_bytes":     []byte{byte(i), byte(i + 1), byte(i + 2)},
				"c_timestamp": int64(1_600_000_000_000 + i),
				"c_ipv4":      fmt.Sprintf("10.0.0.%d", i%256),
				"c_ipv6":      fmt.Sprintf("2001:db8::%x", i),
				"c_cidr":      "10.0.0.0/8",
				"c_mac":       "00:11:22:33:44:55",
				"c_port":      int64(1024 + i),
				"c_protocol":  int64(6),
				"c_duration":  int64(i) * 1_000,
				"c_uuid":      fmt.Sprintf("00000000-0000-4000-8000-%012x", i),
				"c_date":      "2021-03-04",
				"c_decimal":   float64(i) + 0.25,
				"c_vector":    []float32{float32(i), 1, 2, 3},
				"c_array":     []any{int64(i), int64(i + 1)},
				"c_row":       map[string]any{"f_int": int64(i), "f_str": "nested"},
				"c_map":       map[string]any{"k": int64(i)},
				"c_map_arr":   map[string]any{"k": []any{int64(i), int64(i + 1)}},
				"c_arr_arr":   []any{[]any{int64(i)}, []any{int64(i + 1), int64(i + 2)}},
				"c_row_map": map[string]any{
					"f_int": int64(i),
					"f_map": map[string]any{"k": int64(i)},
				},
			}
			// Nulls in every column, staggered so no column is all-present.
			for j, col := range schema.Columns {
				if (i+j)%7 == 0 {
					rows[i][col.Name] = nil
				}
			}
		}
		if err := w.WriteRows(rows); err != nil {
			panic(fmt.Sprintf("all-types fixture write: %v", err))
		}
		if err := w.Close(); err != nil {
			panic(fmt.Sprintf("all-types fixture close: %v", err))
		}
		allTypesRaw = buf.Bytes()
	})
	return allTypesRaw
}

// mutateFile applies |nmut| byte mutations to a copy of a fixture.
//
// The scheme is deterministic in (seed, nmut) so a crasher is a pair of
// integers and nothing else has to be stored:
//
//   - the PRNG is seeded from `seed` alone, so the same seed picks the same
//     positions and operations whatever nmut is;
//   - |nmut| is the number of mutations, capped so one input cannot rewrite
//     the whole file (a file with no structure left tests nothing);
//   - the SIGN selects the region. Positive mutates anywhere in the file;
//     negative mutates only the FOOTER — the last footer_length+8 bytes plus
//     the length itself — which is where the offsets and counts live and
//     where every silent-wrong-answer shape comes from. Random bytes over a
//     whole file land in page payloads most of the time and rarely reach the
//     metadata; the negative arm is how the footer gets exercised at all.
//
// Each mutation flips one bit, replaces the byte outright, or sets it to
// 0xFF — the last because saturated bytes are what turn a small varint into
// a huge one, which is the shape behind both the inflated counts and the
// negative offsets.
func mutateFile(orig []byte, seed, nmut int) []byte {
	raw := make([]byte, len(orig))
	copy(raw, orig)

	count := nmut
	if count < 0 {
		count = -count
	}
	if count > 256 {
		count = 256
	}
	if count == 0 || len(raw) < 16 {
		return raw
	}

	lo, hi := 0, len(raw)
	if nmut < 0 {
		// The footer: its length prefix says how far back it starts, and
		// the eight trailing bytes (length + "PAR1") are in range too.
		footerLen := int(uint32(raw[len(raw)-8]) | uint32(raw[len(raw)-7])<<8 |
			uint32(raw[len(raw)-6])<<16 | uint32(raw[len(raw)-5])<<24)
		lo = len(raw) - 8 - footerLen
		if lo < 4 || lo >= len(raw) {
			lo = len(raw) / 2
		}
	}

	rng := rand.New(rand.NewSource(int64(seed)))
	span := hi - lo
	for i := 0; i < count; i++ {
		pos := lo + rng.Intn(span)
		switch rng.Intn(3) {
		case 0:
			raw[pos] ^= 1 << uint(rng.Intn(8))
		case 1:
			raw[pos] = byte(rng.Intn(256))
		default:
			raw[pos] = 0xFF
		}
	}
	return raw
}
