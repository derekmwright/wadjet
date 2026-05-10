package exec

import (
	"bufio"
	"container/heap"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/exec/kernel"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// External-merge spill format for HashAggregate.
//
// Background: the legacy spill path wrote raw INPUT rows under memory pressure
// and re-aggregated them all in Finalize. At SF100+ that re-read 100GB of input
// to produce a 6GB hash table — pathological. The partial-state path spills
// already-aggregated group state (key + accumulators) per spill event, then
// Finalize k-way merges the runs, combining accumulators on equal keys.
//
// File format (little-endian):
//
//   magic       "WAGS\x01"             (5 bytes)
//   numGroupCols   uint32
//   for each group col:
//     nameLen     uint16
//     name        bytes
//     typeID      uint16
//   numAggs        uint32
//   for each agg:
//     func        uint32   (AggFunc)
//     isFloat     byte
//     isDecimal   byte
//     decScale    int32
//   numGroups      uint32                (groups in this run, exact)
//   for each group, sorted by sortKey:
//     rowMarker   byte (0x01)
//     sortKeyLen  uint32
//     sortKey     bytes
//     for each group col:    typed value (tagged like raw-row spill)
//     for each agg:           accumulator (variable layout — see emitAcc)
//   endMarker     byte (0x00)
//
// The sortKey is the binary GROUP BY key (same encoding as appendColumnValue
// uses for the in-memory hash table). We sort & merge by sortKey rather than
// by display values because the key is already canonical and equal-comparable
// across runs.

const partialSpillMagic = "WAGS\x01"

const (
	partialRowMarker byte = 0x01
	partialEndMarker byte = 0x00
)

// Reuse the type tags used by the legacy raw-row spill for display values.
// These match memory.spillTag* but we re-declare here to avoid an import
// cycle through the exec package's spill helpers.
const (
	partialTagNull      byte = 0
	partialTagBoolFalse byte = 1
	partialTagBoolTrue  byte = 2
	partialTagInt64     byte = 3
	partialTagFloat64   byte = 4
	partialTagString    byte = 5
	partialTagInt32     byte = 6
	partialTagFloat32   byte = 7
	partialTagBytes     byte = 8
	partialTagInt128    byte = 9
)

// partialSpillSeq is a per-process counter for unique partial-spill filenames.
// Distinct from spillFileSeq (raw-row spills) so the filenames are easy to
// distinguish in /tmp listings during debugging.
var partialSpillSeq atomic.Int64

// partialAggSpec captures the static, per-aggregate metadata needed to encode
// and decode an Accumulator in a partial-spill file. The (Func, IsFloat,
// IsDecimal) tuple determines which Accumulator fields are populated by the
// SoA flat-arrays path; emitAcc/readAcc only encode/decode those fields, which
// keeps the on-disk size proportional to the aggregate's actual state.
type partialAggSpec struct {
	Func      AggFunc
	IsFloat   bool
	IsDecimal bool
	DecScale  int32
}

// partialGroupCol is one GROUP BY column in the spill header — name + type so
// the reader can decode display values without recomputing types from a batch.
type partialGroupCol struct {
	Name string
	Type parquet.TypeID
}

// partialSpillHeader is the deserialized header — used by both writer and
// reader. The on-disk layout is the variable byte sequence above; this struct
// is the in-memory shape after parsing.
type partialSpillHeader struct {
	GroupCols []partialGroupCol
	Aggs      []partialAggSpec
	NumGroups uint32
}

// partialGroup is one decoded group emitted by the reader and consumed by the
// k-way merger. SortKey drives merge ordering; KeyVals carry display values
// for the eventual Next() output; Accs are the aggregate state to merge.
type partialGroup struct {
	SortKey []byte
	KeyVals []partialKeyValue
	Accs    []kernel.Accumulator
}

// partialKeyValue carries one typed group-by display value without `any`
// boxing. Tag drives which typed field is valid (matches the partialTag*
// constants the on-disk format uses, so writer / reader can dispatch on
// the same byte). At SF100 Q17 (20M groups × ~2 group cols) the prior
// `any` representation cost ~640 MB of transient box allocations per
// drain task; the unboxed struct cuts that to zero.
type partialKeyValue struct {
	Tag   byte         // one of partialTag* (partialTagNull means null)
	I64   int64        // bool / int32 / int64 / timestamp / ipv4 / mac / duration / port / protocol / date
	F64   float64      // float32 / float64
	Bytes []byte       // string / bytes / ipv6 / cidr / uuid (header may alias backing buffer)
	Dec   batch.Int128 // decimal
}

// IsNull reports whether the value is the null sentinel.
func (kv *partialKeyValue) IsNull() bool { return kv.Tag == partialTagNull }

// partialSpillWriter writes a single partial-state run to a file. Caller
// builds groups in memory, sorts them by SortKey, then calls Write per group
// followed by Close.
type partialSpillWriter struct {
	f       *os.File
	w       *bufio.Writer
	header  partialSpillHeader
	count   uint32
	scratch [16]byte
}

func newPartialSpillWriter(dir string, header partialSpillHeader) (*partialSpillWriter, string, error) {
	id := partialSpillSeq.Add(1)
	path := filepath.Join(dir, fmt.Sprintf("agg-spill-%d.%d.bin", os.Getpid(), id))
	f, err := os.Create(path)
	if err != nil {
		return nil, "", fmt.Errorf("creating partial-spill file: %w", err)
	}
	psw := &partialSpillWriter{
		f:      f,
		w:      bufio.NewWriterSize(f, 64*1024),
		header: header,
	}
	if err := psw.writeHeader(); err != nil {
		f.Close()
		os.Remove(path)
		return nil, "", err
	}
	return psw, path, nil
}

func (w *partialSpillWriter) writeHeader() error {
	if _, err := w.w.WriteString(partialSpillMagic); err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(w.scratch[:4], uint32(len(w.header.GroupCols)))
	if _, err := w.w.Write(w.scratch[:4]); err != nil {
		return err
	}
	for _, gc := range w.header.GroupCols {
		binary.LittleEndian.PutUint16(w.scratch[:2], uint16(len(gc.Name)))
		if _, err := w.w.Write(w.scratch[:2]); err != nil {
			return err
		}
		if _, err := w.w.WriteString(gc.Name); err != nil {
			return err
		}
		binary.LittleEndian.PutUint16(w.scratch[:2], uint16(gc.Type))
		if _, err := w.w.Write(w.scratch[:2]); err != nil {
			return err
		}
	}
	binary.LittleEndian.PutUint32(w.scratch[:4], uint32(len(w.header.Aggs)))
	if _, err := w.w.Write(w.scratch[:4]); err != nil {
		return err
	}
	for _, a := range w.header.Aggs {
		binary.LittleEndian.PutUint32(w.scratch[:4], uint32(a.Func))
		if _, err := w.w.Write(w.scratch[:4]); err != nil {
			return err
		}
		if a.IsFloat {
			w.scratch[0] = 1
		} else {
			w.scratch[0] = 0
		}
		if a.IsDecimal {
			w.scratch[1] = 1
		} else {
			w.scratch[1] = 0
		}
		binary.LittleEndian.PutUint32(w.scratch[2:6], uint32(a.DecScale))
		if _, err := w.w.Write(w.scratch[:6]); err != nil {
			return err
		}
	}
	// numGroups placeholder — patched in Close once we know the count.
	binary.LittleEndian.PutUint32(w.scratch[:4], 0)
	if _, err := w.w.Write(w.scratch[:4]); err != nil {
		return err
	}
	return nil
}

// Write serializes one group. Caller is responsible for SortKey ordering —
// this writer does not re-sort.
func (w *partialSpillWriter) Write(g partialGroup) error {
	if err := w.w.WriteByte(partialRowMarker); err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(w.scratch[:4], uint32(len(g.SortKey)))
	if _, err := w.w.Write(w.scratch[:4]); err != nil {
		return err
	}
	if _, err := w.w.Write(g.SortKey); err != nil {
		return err
	}
	for i, gc := range w.header.GroupCols {
		var kv partialKeyValue
		if i < len(g.KeyVals) {
			kv = g.KeyVals[i]
		} else {
			kv.Tag = partialTagNull
		}
		if err := writeKeyValue(w.w, w.scratch[:], &kv, gc.Type); err != nil {
			return err
		}
	}
	for i, spec := range w.header.Aggs {
		if err := emitAcc(w.w, w.scratch[:], spec, &g.Accs[i]); err != nil {
			return err
		}
	}
	w.count++
	return nil
}

// Close flushes the writer, patches the numGroups header field, and closes
// the file. Returns the on-disk size (used for telemetry).
func (w *partialSpillWriter) Close() (int64, error) {
	if err := w.w.WriteByte(partialEndMarker); err != nil {
		w.f.Close()
		return 0, err
	}
	if err := w.w.Flush(); err != nil {
		w.f.Close()
		return 0, err
	}
	// Patch numGroups: locate the offset by re-walking the header layout.
	// Header bytes:
	//   magic (5) + numGroupCols (4) + per-col(name-len(2)+name+type(2)) + numAggs(4) + per-agg(10) + numGroups(4)
	// We compute the offset analytically rather than seeking to a remembered
	// position, so any header refactor produces a deterministic offset.
	offset := int64(len(partialSpillMagic) + 4)
	for _, gc := range w.header.GroupCols {
		offset += int64(2 + len(gc.Name) + 2)
	}
	offset += 4
	offset += int64(len(w.header.Aggs)) * 10
	binary.LittleEndian.PutUint32(w.scratch[:4], w.count)
	if _, err := w.f.WriteAt(w.scratch[:4], offset); err != nil {
		w.f.Close()
		return 0, err
	}
	size, _ := w.f.Seek(0, io.SeekEnd)
	if err := w.f.Close(); err != nil {
		return size, err
	}
	return size, nil
}

// emitAcc writes only the accumulator fields used by this aggregate's func +
// type combo. The reader uses the matching readAcc to deserialize.
//
// scratch must be a slice of at least 16 bytes whose backing storage is
// already heap-allocated (typical: w.scratch[:] from partialSpillWriter).
// The helpers passed `scratch` use it instead of a stack-local [N]byte
// because bufio.Writer.Write triggers an io.Writer interface call on its
// slow path, forcing the compiler to assume any []byte parameter could
// escape; threading scratch through avoids the per-call heap allocation
// the local-array path would incur.
func emitAcc(w *bufio.Writer, scratch []byte, spec partialAggSpec, a *kernel.Accumulator) error {
	switch spec.Func {
	case AggSum, AggAvg:
		if err := writeInt64(w, scratch, a.Count); err != nil {
			return err
		}
		switch {
		case spec.IsDecimal:
			return writeInt128(w, scratch, a.SumDec)
		case spec.IsFloat:
			return writeFloat64(w, scratch, a.SumF64)
		default:
			return writeInt64(w, scratch, a.SumI64)
		}
	case AggCount:
		return writeInt64(w, scratch, a.Count)
	case AggMin:
		if err := writeBool(w, a.HasMin); err != nil {
			return err
		}
		if !a.HasMin {
			return nil
		}
		switch {
		case spec.IsDecimal:
			return writeInt128(w, scratch, a.MinDec)
		case spec.IsFloat:
			return writeFloat64(w, scratch, a.MinF64)
		default:
			return writeInt64(w, scratch, a.MinI64)
		}
	case AggMax:
		if err := writeBool(w, a.HasMax); err != nil {
			return err
		}
		if !a.HasMax {
			return nil
		}
		switch {
		case spec.IsDecimal:
			return writeInt128(w, scratch, a.MaxDec)
		case spec.IsFloat:
			return writeFloat64(w, scratch, a.MaxF64)
		default:
			return writeInt64(w, scratch, a.MaxI64)
		}
	default:
		return fmt.Errorf("partial spill: unsupported agg func %v", spec.Func)
	}
}

func readAcc(r *bufio.Reader, scratch []byte, spec partialAggSpec, a *kernel.Accumulator) error {
	a.IsFloat = spec.IsFloat
	a.IsDecimal = spec.IsDecimal
	a.DecScale = int(spec.DecScale)
	switch spec.Func {
	case AggSum, AggAvg:
		c, err := readInt64(r, scratch)
		if err != nil {
			return err
		}
		a.Count = c
		switch {
		case spec.IsDecimal:
			v, err := readInt128(r, scratch)
			if err != nil {
				return err
			}
			a.SumDec = v
		case spec.IsFloat:
			v, err := readFloat64(r, scratch)
			if err != nil {
				return err
			}
			a.SumF64 = v
		default:
			v, err := readInt64(r, scratch)
			if err != nil {
				return err
			}
			a.SumI64 = v
		}
		return nil
	case AggCount:
		c, err := readInt64(r, scratch)
		if err != nil {
			return err
		}
		a.Count = c
		return nil
	case AggMin:
		hm, err := readBool(r)
		if err != nil {
			return err
		}
		a.HasMin = hm
		if !hm {
			return nil
		}
		switch {
		case spec.IsDecimal:
			v, err := readInt128(r, scratch)
			if err != nil {
				return err
			}
			a.MinDec = v
		case spec.IsFloat:
			v, err := readFloat64(r, scratch)
			if err != nil {
				return err
			}
			a.MinF64 = v
		default:
			v, err := readInt64(r, scratch)
			if err != nil {
				return err
			}
			a.MinI64 = v
		}
		return nil
	case AggMax:
		hm, err := readBool(r)
		if err != nil {
			return err
		}
		a.HasMax = hm
		if !hm {
			return nil
		}
		switch {
		case spec.IsDecimal:
			v, err := readInt128(r, scratch)
			if err != nil {
				return err
			}
			a.MaxDec = v
		case spec.IsFloat:
			v, err := readFloat64(r, scratch)
			if err != nil {
				return err
			}
			a.MaxF64 = v
		default:
			v, err := readInt64(r, scratch)
			if err != nil {
				return err
			}
			a.MaxI64 = v
		}
		return nil
	default:
		return fmt.Errorf("partial spill: unsupported agg func %v", spec.Func)
	}
}

// writeKeyValue writes a typed display value for a group-by column. The type
// is taken from kv.Tag, which mirrors the on-disk tag bytes; output is
// byte-identical to the prior `any`-dispatch path.
//
// scratch must be a slice of at least 16 bytes whose backing storage is
// already heap-allocated (typical: w.scratch[:] from partialSpillWriter).
// See emitAcc for why threading scratch avoids the per-call alloc that a
// stack-local [16]byte would incur via bufio.Writer.Write's escape.
func writeKeyValue(w *bufio.Writer, scratch []byte, kv *partialKeyValue, _ parquet.TypeID) error {
	switch kv.Tag {
	case partialTagNull:
		return w.WriteByte(partialTagNull)
	case partialTagBoolTrue, partialTagBoolFalse:
		// I64 carries the boolean truth value; tag itself encodes it on disk.
		return w.WriteByte(kv.Tag)
	case partialTagInt64:
		if err := w.WriteByte(partialTagInt64); err != nil {
			return err
		}
		binary.LittleEndian.PutUint64(scratch[:8], uint64(kv.I64))
		_, err := w.Write(scratch[:8])
		return err
	case partialTagInt32:
		if err := w.WriteByte(partialTagInt32); err != nil {
			return err
		}
		binary.LittleEndian.PutUint32(scratch[:4], uint32(int32(kv.I64)))
		_, err := w.Write(scratch[:4])
		return err
	case partialTagFloat64:
		if err := w.WriteByte(partialTagFloat64); err != nil {
			return err
		}
		binary.LittleEndian.PutUint64(scratch[:8], math.Float64bits(kv.F64))
		_, err := w.Write(scratch[:8])
		return err
	case partialTagFloat32:
		if err := w.WriteByte(partialTagFloat32); err != nil {
			return err
		}
		binary.LittleEndian.PutUint32(scratch[:4], math.Float32bits(float32(kv.F64)))
		_, err := w.Write(scratch[:4])
		return err
	case partialTagString:
		if err := w.WriteByte(partialTagString); err != nil {
			return err
		}
		binary.LittleEndian.PutUint32(scratch[:4], uint32(len(kv.Bytes)))
		if _, err := w.Write(scratch[:4]); err != nil {
			return err
		}
		_, err := w.Write(kv.Bytes)
		return err
	case partialTagBytes:
		if err := w.WriteByte(partialTagBytes); err != nil {
			return err
		}
		binary.LittleEndian.PutUint32(scratch[:4], uint32(len(kv.Bytes)))
		if _, err := w.Write(scratch[:4]); err != nil {
			return err
		}
		_, err := w.Write(kv.Bytes)
		return err
	case partialTagInt128:
		if err := w.WriteByte(partialTagInt128); err != nil {
			return err
		}
		binary.LittleEndian.PutUint64(scratch[:8], uint64(kv.Dec.Hi))
		binary.LittleEndian.PutUint64(scratch[8:16], kv.Dec.Lo)
		_, err := w.Write(scratch[:16])
		return err
	default:
		return fmt.Errorf("partial spill: unknown kv tag %d", kv.Tag)
	}
}

// setPartialKeyFromAny populates dst with the typed contents of v. Used by
// fallback paths (cursor's AoS branch, MergeSink-migrated state) where the
// canonical KeyVals storage is `[]any` from the consume-time generic path.
// At those call sites, the boxing already happened during consume — we just
// unbox once into the typed slot.
func setPartialKeyFromAny(dst *partialKeyValue, v any) {
	dst.Bytes = nil
	dst.Dec = batch.Int128{}
	dst.I64 = 0
	dst.F64 = 0
	if v == nil {
		dst.Tag = partialTagNull
		return
	}
	switch tv := v.(type) {
	case bool:
		if tv {
			dst.Tag = partialTagBoolTrue
		} else {
			dst.Tag = partialTagBoolFalse
		}
	case int64:
		dst.Tag = partialTagInt64
		dst.I64 = tv
	case int:
		dst.Tag = partialTagInt64
		dst.I64 = int64(tv)
	case int32:
		dst.Tag = partialTagInt32
		dst.I64 = int64(tv)
	case float64:
		dst.Tag = partialTagFloat64
		dst.F64 = tv
	case float32:
		dst.Tag = partialTagFloat32
		dst.F64 = float64(tv)
	case string:
		dst.Tag = partialTagString
		// Aliasing tv's underlying bytes via unsafe is risky; the consume
		// path produces strings whose lifetime exceeds the cursor's, so a
		// view-by-byte conversion is acceptable. The merger copies Bytes
		// when extracting the head into output columns anyway.
		dst.Bytes = []byte(tv)
	case []byte:
		dst.Tag = partialTagBytes
		dst.Bytes = tv
	case batch.Int128:
		dst.Tag = partialTagInt128
		dst.Dec = tv
	default:
		dst.Tag = partialTagString
		dst.Bytes = []byte(fmt.Sprintf("%v", tv))
	}
}

// readKeyValueInto decodes one typed display value from r into dst, reusing
// dst's backing storage where possible. Caller must zero dst before calling
// if it might hold stale data from a prior read (string/bytes Bytes pointer
// is overwritten with a fresh allocation; Dec/I64/F64 are overwritten when
// the new tag's branch sets them).
//
// scratch must be a slice of at least 16 bytes whose backing storage is
// already heap-allocated (typical: r.scratch[:] from partialSpillReader).
// Allocates one fresh []byte per string/bytes read (variable-length data
// must own its bytes since the underlying bufio buffer is reused). Numeric
// types allocate nothing.
func readKeyValueInto(r *bufio.Reader, scratch []byte, dst *partialKeyValue) error {
	tag, err := r.ReadByte()
	if err != nil {
		return err
	}
	dst.Tag = tag
	switch tag {
	case partialTagNull, partialTagBoolFalse, partialTagBoolTrue:
		return nil
	case partialTagInt64:
		if _, err := io.ReadFull(r, scratch[:8]); err != nil {
			return err
		}
		dst.I64 = int64(binary.LittleEndian.Uint64(scratch[:8]))
		return nil
	case partialTagInt32:
		if _, err := io.ReadFull(r, scratch[:4]); err != nil {
			return err
		}
		dst.I64 = int64(int32(binary.LittleEndian.Uint32(scratch[:4])))
		return nil
	case partialTagFloat64:
		if _, err := io.ReadFull(r, scratch[:8]); err != nil {
			return err
		}
		dst.F64 = math.Float64frombits(binary.LittleEndian.Uint64(scratch[:8]))
		return nil
	case partialTagFloat32:
		if _, err := io.ReadFull(r, scratch[:4]); err != nil {
			return err
		}
		dst.F64 = float64(math.Float32frombits(binary.LittleEndian.Uint32(scratch[:4])))
		return nil
	case partialTagString, partialTagBytes:
		if _, err := io.ReadFull(r, scratch[:4]); err != nil {
			return err
		}
		n := int(binary.LittleEndian.Uint32(scratch[:4]))
		// Reuse dst.Bytes capacity when it's large enough; otherwise allocate
		// fresh. The merger and downstream output writer both copy or
		// pass-through Bytes without retaining beyond the next read.
		if cap(dst.Bytes) < n {
			dst.Bytes = make([]byte, n)
		} else {
			dst.Bytes = dst.Bytes[:n]
		}
		if _, err := io.ReadFull(r, dst.Bytes); err != nil {
			return err
		}
		return nil
	case partialTagInt128:
		if _, err := io.ReadFull(r, scratch[:16]); err != nil {
			return err
		}
		dst.Dec.Hi = int64(binary.LittleEndian.Uint64(scratch[:8]))
		dst.Dec.Lo = binary.LittleEndian.Uint64(scratch[8:16])
		return nil
	default:
		return fmt.Errorf("partial spill: unknown tag %d", tag)
	}
}

func writeBool(w *bufio.Writer, v bool) error {
	if v {
		return w.WriteByte(1)
	}
	return w.WriteByte(0)
}

func readBool(r *bufio.Reader) (bool, error) {
	b, err := r.ReadByte()
	if err != nil {
		return false, err
	}
	return b != 0, nil
}

func writeInt64(w *bufio.Writer, scratch []byte, v int64) error {
	binary.LittleEndian.PutUint64(scratch[:8], uint64(v))
	_, err := w.Write(scratch[:8])
	return err
}

func readInt64(r *bufio.Reader, scratch []byte) (int64, error) {
	if _, err := io.ReadFull(r, scratch[:8]); err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(scratch[:8])), nil
}

func writeFloat64(w *bufio.Writer, scratch []byte, v float64) error {
	binary.LittleEndian.PutUint64(scratch[:8], math.Float64bits(v))
	_, err := w.Write(scratch[:8])
	return err
}

func readFloat64(r *bufio.Reader, scratch []byte) (float64, error) {
	if _, err := io.ReadFull(r, scratch[:8]); err != nil {
		return 0, err
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(scratch[:8])), nil
}

func writeInt128(w *bufio.Writer, scratch []byte, v batch.Int128) error {
	binary.LittleEndian.PutUint64(scratch[:8], uint64(v.Hi))
	binary.LittleEndian.PutUint64(scratch[8:16], v.Lo)
	_, err := w.Write(scratch[:16])
	return err
}

func readInt128(r *bufio.Reader, scratch []byte) (batch.Int128, error) {
	if _, err := io.ReadFull(r, scratch[:16]); err != nil {
		return batch.Int128{}, err
	}
	return batch.Int128{
		Hi: int64(binary.LittleEndian.Uint64(scratch[:8])),
		Lo: binary.LittleEndian.Uint64(scratch[8:16]),
	}, nil
}

// partialSpillReader iterates groups in a partial-spill file in stored order
// (which is sorted by sortKey). It is forward-only and not safe for concurrent
// use.
//
// Next returns a *partialGroup whose backing storage (SortKey/KeyVals/Accs)
// is owned by the reader and reused across calls — same contract as
// kWayMerger.Next. Callers must consume (or copy) before the next Next.
// Because the reader feeds into kWayMerger which copies head data on its
// own Next, the in-place reuse is safe end-to-end.
type partialSpillReader struct {
	f       *os.File
	r       *bufio.Reader
	header  partialSpillHeader
	scratch [16]byte

	// Reusable head storage. Returned by Next; valid only until the next
	// Next call. Backing arrays grow once to fit the widest group.
	head        partialGroup
	headSortKey []byte
	headKeyVals []partialKeyValue
	headAccs    []kernel.Accumulator
}

func openPartialSpillReader(path string) (*partialSpillReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening partial-spill file: %w", err)
	}
	r := &partialSpillReader{
		f: f,
		r: bufio.NewReaderSize(f, 64*1024),
	}
	if err := r.readHeader(); err != nil {
		f.Close()
		return nil, err
	}
	return r, nil
}

func (r *partialSpillReader) readHeader() error {
	magic := make([]byte, len(partialSpillMagic))
	if _, err := io.ReadFull(r.r, magic); err != nil {
		return fmt.Errorf("partial spill: reading magic: %w", err)
	}
	if string(magic) != partialSpillMagic {
		return fmt.Errorf("partial spill: bad magic %q", magic)
	}
	if _, err := io.ReadFull(r.r, r.scratch[:4]); err != nil {
		return err
	}
	numGroupCols := int(binary.LittleEndian.Uint32(r.scratch[:4]))
	r.header.GroupCols = make([]partialGroupCol, numGroupCols)
	for i := 0; i < numGroupCols; i++ {
		if _, err := io.ReadFull(r.r, r.scratch[:2]); err != nil {
			return err
		}
		nameLen := int(binary.LittleEndian.Uint16(r.scratch[:2]))
		nameBuf := make([]byte, nameLen)
		if _, err := io.ReadFull(r.r, nameBuf); err != nil {
			return err
		}
		if _, err := io.ReadFull(r.r, r.scratch[:2]); err != nil {
			return err
		}
		typeID := parquet.TypeID(binary.LittleEndian.Uint16(r.scratch[:2]))
		r.header.GroupCols[i] = partialGroupCol{Name: string(nameBuf), Type: typeID}
	}
	if _, err := io.ReadFull(r.r, r.scratch[:4]); err != nil {
		return err
	}
	numAggs := int(binary.LittleEndian.Uint32(r.scratch[:4]))
	r.header.Aggs = make([]partialAggSpec, numAggs)
	for i := 0; i < numAggs; i++ {
		if _, err := io.ReadFull(r.r, r.scratch[:10]); err != nil {
			return err
		}
		r.header.Aggs[i] = partialAggSpec{
			Func:      AggFunc(binary.LittleEndian.Uint32(r.scratch[:4])),
			IsFloat:   r.scratch[4] != 0,
			IsDecimal: r.scratch[5] != 0,
			DecScale:  int32(binary.LittleEndian.Uint32(r.scratch[6:10])),
		}
	}
	if _, err := io.ReadFull(r.r, r.scratch[:4]); err != nil {
		return err
	}
	r.header.NumGroups = binary.LittleEndian.Uint32(r.scratch[:4])
	return nil
}

// Next reads the next group, or returns (nil, io.EOF) when the run is
// exhausted. Returns errors for malformed files (truncated, bad tags, etc.).
//
// The returned *partialGroup aliases reusable buffers on the reader; the
// next Next call invalidates the previously returned slice contents.
func (r *partialSpillReader) Next() (*partialGroup, error) {
	marker, err := r.r.ReadByte()
	if err != nil {
		return nil, err
	}
	if marker == partialEndMarker {
		return nil, io.EOF
	}
	if marker != partialRowMarker {
		return nil, fmt.Errorf("partial spill: bad row marker %d", marker)
	}
	if _, err := io.ReadFull(r.r, r.scratch[:4]); err != nil {
		return nil, err
	}
	keyLen := int(binary.LittleEndian.Uint32(r.scratch[:4]))
	if cap(r.headSortKey) < keyLen {
		r.headSortKey = make([]byte, keyLen)
	} else {
		r.headSortKey = r.headSortKey[:keyLen]
	}
	if _, err := io.ReadFull(r.r, r.headSortKey); err != nil {
		return nil, err
	}
	r.head.SortKey = r.headSortKey

	nGc := len(r.header.GroupCols)
	if cap(r.headKeyVals) < nGc {
		r.headKeyVals = make([]partialKeyValue, nGc)
	} else {
		r.headKeyVals = r.headKeyVals[:nGc]
	}
	for i := range r.headKeyVals {
		// Zero the slot first so a prior group's Bytes / Dec don't leak
		// into a fresh read that doesn't touch those fields.
		r.headKeyVals[i] = partialKeyValue{}
		if err := readKeyValueInto(r.r, r.scratch[:], &r.headKeyVals[i]); err != nil {
			return nil, err
		}
	}
	r.head.KeyVals = r.headKeyVals

	nA := len(r.header.Aggs)
	if cap(r.headAccs) < nA {
		r.headAccs = make([]kernel.Accumulator, nA)
	} else {
		r.headAccs = r.headAccs[:nA]
	}
	// readAcc only sets fields its agg type touches; zero out so a prior
	// group's HasMin/HasMax/etc. don't leak into this read.
	for i := range r.headAccs {
		r.headAccs[i] = kernel.Accumulator{}
	}
	for i := range r.headAccs {
		if err := readAcc(r.r, r.scratch[:], r.header.Aggs[i], &r.headAccs[i]); err != nil {
			return nil, err
		}
	}
	r.head.Accs = r.headAccs

	return &r.head, nil
}

func (r *partialSpillReader) Close() error {
	return r.f.Close()
}

// partialRunSource is the merge input abstraction. memorySource and
// fileSource implement it; the k-way merger only needs Peek/Advance.
type partialRunSource interface {
	// Peek returns the current head group without consuming it. Returns nil
	// when the run is exhausted.
	Peek() *partialGroup
	// Advance discards the current head and loads the next group. Safe to
	// call after exhaustion (no-op).
	Advance() error
	// Close releases underlying resources (file handles, etc.).
	Close() error
}

// memoryRunSource feeds an already-sorted in-memory slice of groups to the
// merger. Used for the final in-memory remainder during Finalize.
type memoryRunSource struct {
	groups []*partialGroup
	pos    int
}

func newMemoryRunSource(groups []*partialGroup) *memoryRunSource {
	return &memoryRunSource{groups: groups}
}

func (m *memoryRunSource) Peek() *partialGroup {
	if m.pos >= len(m.groups) {
		return nil
	}
	return m.groups[m.pos]
}

func (m *memoryRunSource) Advance() error {
	if m.pos < len(m.groups) {
		m.pos++
	}
	return nil
}

func (m *memoryRunSource) Close() error { return nil }

// fileRunSource pulls from a partialSpillReader.
type fileRunSource struct {
	r    *partialSpillReader
	head *partialGroup
}

func newFileRunSource(path string) (*fileRunSource, error) {
	r, err := openPartialSpillReader(path)
	if err != nil {
		return nil, err
	}
	src := &fileRunSource{r: r}
	if err := src.Advance(); err != nil {
		r.Close()
		return nil, err
	}
	return src, nil
}

func (s *fileRunSource) Peek() *partialGroup { return s.head }

func (s *fileRunSource) Advance() error {
	g, err := s.r.Next()
	if err == io.EOF {
		s.head = nil
		return nil
	}
	if err != nil {
		return err
	}
	s.head = g
	return nil
}

func (s *fileRunSource) Close() error { return s.r.Close() }

// runHeap implements heap.Interface — top-of-heap is the smallest sortKey
// across all live runs. We track the source index so Advance() can hit the
// right run after we yield its head.
type runHeapEntry struct {
	source int // index into runs slice
	key    []byte
}

type runHeap []runHeapEntry

func (h runHeap) Len() int { return len(h) }
func (h runHeap) Less(i, j int) bool {
	c := bytesCompare(h[i].key, h[j].key)
	if c != 0 {
		return c < 0
	}
	return h[i].source < h[j].source
}
func (h runHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *runHeap) Push(x any)         { *h = append(*h, x.(runHeapEntry)) }
func (h *runHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// bytesCompare is a local copy of bytes.Compare to avoid the import for one
// call site (and to keep the merge hot path inlinable).
func bytesCompare(a, b []byte) int {
	la, lb := len(a), len(b)
	n := la
	if lb < n {
		n = lb
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	if la < lb {
		return -1
	}
	if la > lb {
		return 1
	}
	return 0
}

// kWayMerger streams groups from N runs in sortKey order, combining
// accumulators on equal keys. Output is monotone non-decreasing in sortKey.
//
// Next returns a *partialGroup whose backing storage (SortKey/KeyVals/Accs
// slices) is owned by the merger and reused across calls. Callers MUST NOT
// retain pointers across Next calls — copy what you need before calling
// Next again. Reusing the head buffers eliminates the per-merged-group
// allocations the older fresh-copy contract required.
type kWayMerger struct {
	runs []partialRunSource
	heap runHeap
	aggs []partialAggSpec

	// Reusable head storage. Returned by Next; valid only until the next
	// Next call. Backing arrays grow once to fit the widest group then
	// stay stable for subsequent reuse.
	head        partialGroup
	headSortKey []byte               // stable backing for head.SortKey
	headKeyVals []partialKeyValue    // stable backing for head.KeyVals
	headAccs    []kernel.Accumulator // stable backing for head.Accs
}

func newKWayMerger(runs []partialRunSource, aggs []partialAggSpec) *kWayMerger {
	m := &kWayMerger{runs: runs, aggs: aggs}
	for i, r := range runs {
		if g := r.Peek(); g != nil {
			m.heap = append(m.heap, runHeapEntry{source: i, key: g.SortKey})
		}
	}
	heap.Init(&m.heap)
	return m
}

// Next returns the next merged group, or (nil, nil) when all runs are
// exhausted. Equal-keyed groups across runs are combined via Accumulator.Merge.
//
// The returned *partialGroup aliases reusable buffers on the merger; the
// next Next call invalidates the previously returned slice contents. The
// caller must consume (or copy) the head before continuing.
func (m *kWayMerger) Next() (*partialGroup, error) {
	if len(m.heap) == 0 {
		return nil, nil
	}
	top := heap.Pop(&m.heap).(runHeapEntry)
	current := m.runs[top.source].Peek()

	// Refill m.head from current using stable backing arrays. SortKey is
	// always copied (the source may reuse its head buffer on Advance, and
	// the heap-top compare below needs a stable reference). KeyVals are
	// typed structs — the slice array is stable; we overwrite slots
	// rather than allocate. Bytes within partialKeyValue still alias the
	// source's underlying storage, but the source guarantees that storage
	// is stable until its own next Advance, which doesn't happen until
	// the corresponding heap entry is popped.
	m.headSortKey = append(m.headSortKey[:0], current.SortKey...)
	m.head.SortKey = m.headSortKey

	if cap(m.headKeyVals) < len(current.KeyVals) {
		m.headKeyVals = make([]partialKeyValue, len(current.KeyVals))
	} else {
		m.headKeyVals = m.headKeyVals[:len(current.KeyVals)]
	}
	copy(m.headKeyVals, current.KeyVals)
	m.head.KeyVals = m.headKeyVals

	if cap(m.headAccs) < len(current.Accs) {
		m.headAccs = make([]kernel.Accumulator, len(current.Accs))
	} else {
		m.headAccs = m.headAccs[:len(current.Accs)]
	}
	copy(m.headAccs, current.Accs)
	m.head.Accs = m.headAccs

	if err := m.advanceSource(top.source); err != nil {
		return nil, err
	}
	// Drain any other runs whose head matches this key and merge their
	// accumulators in. Important: keep going as long as heap top's key
	// equals m.head.SortKey; multiple runs may all carry the same group.
	for len(m.heap) > 0 && bytesCompare(m.heap[0].key, m.head.SortKey) == 0 {
		next := heap.Pop(&m.heap).(runHeapEntry)
		other := m.runs[next.source].Peek()
		for i := range m.head.Accs {
			m.head.Accs[i].Merge(&other.Accs[i])
		}
		if err := m.advanceSource(next.source); err != nil {
			return nil, err
		}
	}
	return &m.head, nil
}

func (m *kWayMerger) advanceSource(idx int) error {
	if err := m.runs[idx].Advance(); err != nil {
		return err
	}
	if g := m.runs[idx].Peek(); g != nil {
		heap.Push(&m.heap, runHeapEntry{source: idx, key: g.SortKey})
	}
	return nil
}

// Close releases all run sources. Safe to call multiple times.
func (m *kWayMerger) Close() error {
	var firstErr error
	for _, r := range m.runs {
		if err := r.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	m.runs = nil
	return firstErr
}

// canUseExternalMerge reports whether HashAggregate's current configuration
// supports the partial-state external-merge spill path. The path requires:
//   - simpleAggs (every agg fits Accumulator: SUM/COUNT/MIN/MAX/AVG)
//   - one of the SoA paths in use (so accs live in intFlatAccs and we can
//     drain them with a uniform key-extraction strategy)
//   - no grouping sets (those route through the generic per-row path)
//
// Other configurations fall back to the legacy raw-row spill behavior.
func (h *HashAggregate) canUseExternalMerge() bool {
	if !h.simpleAggs {
		return false
	}
	if len(h.GroupingSets) > 0 {
		return false
	}
	return h.useIntGroupKey || h.useDualIntGroupKey || h.useCompactGroupKey ||
		h.useStrGroupKey || h.useGenericSoA
}

// buildPartialAggSpecs derives the on-disk aggregate metadata from the
// current intFlatAccs. Called immediately before draining so the IsFloat /
// IsDecimal / DecScale flags reflect the actual scatter layout.
func (h *HashAggregate) buildPartialAggSpecs() []partialAggSpec {
	specs := make([]partialAggSpec, len(h.Aggs))
	for i, agg := range h.Aggs {
		spec := partialAggSpec{Func: agg.Func}
		if i < len(h.intFlatAccs) {
			fa := &h.intFlatAccs[i]
			spec.IsFloat = fa.isFloat
			spec.IsDecimal = fa.isDecimal
			spec.DecScale = int32(fa.decScale)
		}
		specs[i] = spec
	}
	return specs
}

// buildPartialGroupCols constructs the on-disk group-column metadata from the
// resolved column list. Names are GroupByCols verbatim; types come from
// groupColTypes (resolved on first batch). Used both for the spill header
// and to align decoded groups back to output rows.
func (h *HashAggregate) buildPartialGroupCols() []partialGroupCol {
	cols := make([]partialGroupCol, len(h.GroupByCols))
	for i, name := range h.GroupByCols {
		var typ parquet.TypeID
		if i < len(h.groupColTypes) {
			typ = mapBatchTypeToParquet(h.groupColTypes[i])
		}
		cols[i] = partialGroupCol{Name: name, Type: typ}
	}
	return cols
}

// mapBatchTypeToParquet translates the batch.TypeID stored in groupColTypes
// to the parquet.TypeID stored in the spill header. They share most enum
// values but the import boundaries are different. This mapping is best-effort:
// the writer/reader use parquet.TypeID for the header but the actual encoded
// value tag is type-tag-based, so any mismatch only affects the printed type
// name in diagnostics.
func mapBatchTypeToParquet(t batch.TypeID) parquet.TypeID {
	switch t {
	case batch.TypeBool:
		return parquet.TypeBool
	case batch.TypeInt32:
		return parquet.TypeInt32
	case batch.TypeInt64:
		return parquet.TypeInt64
	case batch.TypeFloat32:
		return parquet.TypeFloat32
	case batch.TypeFloat64:
		return parquet.TypeFloat64
	case batch.TypeString:
		return parquet.TypeString
	case batch.TypeBytes:
		return parquet.TypeBytes
	case batch.TypeTimestamp:
		return parquet.TypeTimestamp
	case batch.TypeIPv4:
		return parquet.TypeIPv4
	case batch.TypeIPv6:
		return parquet.TypeIPv6
	case batch.TypeCIDR:
		return parquet.TypeCIDR
	case batch.TypeMAC:
		return parquet.TypeMAC
	case batch.TypePort:
		return parquet.TypePort
	case batch.TypeProtocol:
		return parquet.TypeProtocol
	case batch.TypeDuration:
		return parquet.TypeDuration
	case batch.TypeUUID:
		return parquet.TypeUUID
	case batch.TypeDate:
		return parquet.TypeDate
	case batch.TypeDecimal:
		return parquet.TypeDecimal
	default:
		return parquet.TypeString
	}
}

// spillPartialState drains the current SoA hash state to a partial-spill file
// and resets in-memory structures so subsequent Consume calls start fresh.
// The next Consume call will rebuild flat accumulators from the next batch's
// schema.
//
// Drain is SoA-direct via partialGroupCursor: no materializeFlatAccums copy,
// no []*partialGroup pointer-slice materialization. Peak drain footprint is
// the sort-key arena + sort index + a single reused head, instead of one
// heap-allocated *partialGroup per group. At SF100 Q17 (20M groups) this
// drops ~7 GB per task to ~480 MB.
//
// Caller must hold h.mu.
func (h *HashAggregate) spillPartialState() error {
	if h.Spill == nil {
		return nil
	}
	// Capture specs while h.intFlatAccs still carries the type flags. We
	// no longer materialize, but the buildPartialAggSpecs/buildPartialGroupCols
	// dependencies on h's mode + intFlatAccs are unchanged from the prior path.
	aggSpecs := h.buildPartialAggSpecs()
	groupCols := h.buildPartialGroupCols()

	cursor := newPartialGroupCursor(h)
	if cursor.numGroups == 0 {
		cursor.Close()
		h.resetGroupStateAfterSpill()
		return nil
	}

	header := partialSpillHeader{
		GroupCols: groupCols,
		Aggs:      aggSpecs,
	}
	w, path, err := newPartialSpillWriter(h.Spill.SpillDir(), header)
	if err != nil {
		cursor.Close()
		return err
	}
	for {
		g := cursor.Peek()
		if g == nil {
			break
		}
		if werr := w.Write(*g); werr != nil {
			w.Close()
			cursor.Close()
			return werr
		}
		if aerr := cursor.Advance(); aerr != nil {
			w.Close()
			cursor.Close()
			return aerr
		}
	}
	if _, err := w.Close(); err != nil {
		cursor.Close()
		return err
	}
	cursor.Close()
	h.partialSpillFiles = append(h.partialSpillFiles, path)

	h.resetGroupStateAfterSpill()
	return nil
}

// resetGroupStateAfterSpill clears the in-memory hash table state so the
// next Consume rebuilds it from scratch. Releases tracker bytes for the
// drained group state. Called by spillPartialState; also a useful seam for
// tests that want to reset between spills without writing files.
func (h *HashAggregate) resetGroupStateAfterSpill() {
	if h.Spill != nil && h.trackedGroupMem > 0 {
		h.Spill.ReleaseTracking(h.trackedGroupMem)
		h.trackedGroupMem = 0
	}
	// Reset hash tables. We allocate fresh ones to avoid carrying capacity
	// debt forward — the freshly drained run was likely large, and the next
	// run probably starts smaller.
	if h.intGroupIndex != nil {
		h.intGroupIndex = newIntHashTable(4096)
	}
	if h.strGroupIndex != nil {
		h.strGroupIndex = newStrHashTable(4096)
	}
	h.intGroupStates = nil
	h.strGroupStates = nil
	h.keys = nil
	h.serializedKeys = nil
	h.compactKeys = nil
	h.dualIntKeysA = nil
	h.dualIntKeysB = nil
	h.dualIntNextGroup = nil
	// Drop the group-state pool — its chunks are no longer referenced once
	// the slice headers above are nil. Resetting the pool lets GC reclaim
	// the chunks; without this, the chunk arrays stay live as roots.
	h.gsPool = groupStatePool{}
	// intFlatAccs was nil'd by materializeFlatAccums, but be explicit here
	// for the case where we're called from a test path that bypassed it.
	h.intFlatAccs = nil
	h.groupIndexBuf = nil
}

// finalizeViaPartialMerge sets up a streaming k-way merge over all
// partial-spill runs plus the in-memory remainder. The merger is stored on
// the HashAggregate; Next() drains it incrementally into output batches.
//
// Memory bound: peak in-Next() footprint is one output batch (~2048 rows)
// plus the merger's heap entries (one per run) plus each run's current
// head group. Critically NOT proportional to the merged group count, so
// SF1000+ "output >> memory" cases are tractable.
//
// Spill files are deleted by Close() once the merger is closed, not here —
// the runs need them open during Next() drains.
func (h *HashAggregate) finalizeViaPartialMerge() error {
	// Capture specs while intFlatAccs is still populated.
	specs := h.buildPartialAggSpecs()
	// Capture the output schema while the path-mode flags + groupColTypes
	// reflect the original configuration. Once we collapse to streaming
	// mode below, outputSchema()'s heuristics (which depend on those flags)
	// would emit a different shape.
	h.partialMergerSchema = h.outputSchema()

	// Build a streaming SoA-direct cursor over the in-memory remainder. The
	// cursor owns slice-header references to h.intFlatAccs / *GroupStates /
	// dualIntKeys for its lifetime; the resetGroupStateAfterSpill call below
	// nils h's slice headers but not the underlying arrays, which the cursor
	// keeps live until it's drained and Close()d by closePartialMerger.
	cursor := newPartialGroupCursor(h)

	sources := make([]partialRunSource, 0, len(h.partialSpillFiles)+1)
	if cursor.numGroups > 0 {
		sources = append(sources, cursor)
	} else {
		cursor.Close()
	}
	for _, path := range h.partialSpillFiles {
		src, err := newFileRunSource(path)
		if err != nil {
			// Close any sources we've opened so far.
			for _, s := range sources {
				_ = s.Close()
			}
			return err
		}
		sources = append(sources, src)
	}

	// Reset in-memory state so the existing Next() emission paths can't
	// accidentally drive output from leftover hash table entries — only the
	// merger drives output now. The cursor's slice-header copies keep the
	// SoA arrays + group-state chunks alive until cursor.Close() runs.
	h.resetGroupStateAfterSpill()
	h.useIntGroupKey = false
	h.useDualIntGroupKey = false
	h.useCompactGroupKey = false
	h.useStrGroupKey = false
	h.useGenericSoA = false

	h.partialMerger = newKWayMerger(sources, specs)
	return nil
}

// closePartialMerger closes the active streaming merger (if any), removes
// the spill files it consumed, and releases internal references. Idempotent
// — safe to call from Next() at exhaustion AND from Close() as a backstop.
func (h *HashAggregate) closePartialMerger() {
	if h.partialMerger != nil {
		_ = h.partialMerger.Close()
		h.partialMerger = nil
	}
	for _, path := range h.partialSpillFiles {
		_ = os.Remove(path)
	}
	h.partialSpillFiles = nil
	h.partialMergerSchema = nil
}

// nextFromPartialMerger drains up to batch.DefaultBatchSize groups from the
// streaming merger and emits them as a single output batch matching the
// previously captured output schema. Returns (nil, nil) when the merger is
// exhausted.
//
// Writes columns inline as the merger yields each group, so the only
// transient buffer is the output batch itself — there is no intermediate
// []partialGroup of size DefaultBatchSize. Saves ~144 KB of struct-header
// copies per Next call (2048 × sizeof(partialGroup)) and improves
// locality: each merged group is consumed and dropped before the next one
// is pulled.
//
// Caller must hold h.mu (Next does not lock today, but the merger field is
// only mutated by Finalize and Close, both of which run on the main goroutine
// — concurrent SpillSome only touches pre-finalize state).
func (h *HashAggregate) nextFromPartialMerger() (*batch.RecordBatch, error) {
	if h.partialMerger == nil {
		return nil, nil
	}
	// Pull the first group up-front so the empty case (merger exhausted)
	// returns without allocating an output batch we'd immediately discard.
	first, err := h.partialMerger.Next()
	if err != nil {
		h.closePartialMerger()
		return nil, err
	}
	if first == nil {
		h.closePartialMerger()
		return nil, nil
	}

	out := batch.NewRecordBatch(h.partialMergerSchema, batch.DefaultBatchSize)
	nGroupCols := len(h.GroupByCols)
	h.writeMergedRow(out, 0, first, nGroupCols)
	nRows := 1

	for nRows < batch.DefaultBatchSize {
		g, err := h.partialMerger.Next()
		if err != nil {
			h.closePartialMerger()
			return nil, err
		}
		if g == nil {
			break
		}
		h.writeMergedRow(out, nRows, g, nGroupCols)
		nRows++
	}

	// Trim the output batch to the actual filled row count. The unused
	// slots in each column vector were zero-cleared by NewRecordBatch and
	// are not visible past Len.
	out.Len = nRows
	for _, col := range out.Columns {
		col.Len = nRows
	}
	return out, nil
}

// writeMergedRow writes one merged partialGroup into out at row index i.
// Group-by columns come from g.KeyVals (typed partialKeyValue produced by
// the cursor or file reader); agg columns are finalized via
// finalizeKernelAcc — only SUM/COUNT/MIN/MAX/AVG reach here per the
// canUseExternalMerge gate.
//
// Group-by writes go through writePartialKeyToColumn to dispatch on the
// destination column's batch.TypeID without an `any` round-trip. Agg
// writes still go through SetValue (the kernel finalizer returns `any`
// today; making it typed would be a parallel refactor).
func (h *HashAggregate) writeMergedRow(out *batch.RecordBatch, i int, g *partialGroup, nGroupCols int) {
	for k := 0; k < nGroupCols && k < len(g.KeyVals); k++ {
		writePartialKeyToColumn(out.Columns[k], i, &g.KeyVals[k])
	}
	for j, agg := range h.Aggs {
		colIdx := nGroupCols + j
		result := finalizeKernelAcc(&g.Accs[j], agg.Func)
		if result == nil {
			out.Columns[colIdx].Nulls.SetNull(i)
			continue
		}
		out.Columns[colIdx].SetValue(i, result)
	}
}

// writePartialKeyToColumn dispatches a typed partialKeyValue into the
// matching typed slot on dst. The destination column's batch.TypeID drives
// the dispatch; the kv tag is used only to detect the null sentinel (and
// for variable-width types where len(kv.Bytes) is the payload). No `any`
// round-trip and no type assertion.
func writePartialKeyToColumn(dst *batch.Vector, i int, kv *partialKeyValue) {
	if kv.Tag == partialTagNull {
		dst.Nulls.SetNull(i)
		switch dst.Type {
		case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
			dst.BytesData.Set(i, nil)
		}
		return
	}
	dst.Nulls.SetValid(i)
	switch dst.Type {
	case batch.TypeBool:
		dst.BoolData[i] = kv.Tag == partialTagBoolTrue || kv.I64 != 0
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		dst.Int32Data[i] = int32(kv.I64)
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		dst.Int64Data[i] = kv.I64
	case batch.TypeFloat32:
		dst.Float32Data[i] = float32(kv.F64)
	case batch.TypeFloat64:
		dst.Float64Data[i] = kv.F64
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
		dst.BytesData.Set(i, kv.Bytes)
	default:
		// Decimal and any other types not enumerated above: defer to the
		// generic SetValue path. Decimal's display value isn't trivial to
		// dispatch from kv.Dec without recreating Decimal-store semantics
		// here. SF100 group keys are not decimals; the slow path is fine.
		writePartialKeyFallback(dst, i, kv)
	}
}

// writePartialKeyFallback handles types not in the typed-dispatch fast
// path. Goes through Vector.SetValue, which boxes once per call. Reached
// only for decimal group-by keys today.
func writePartialKeyFallback(dst *batch.Vector, i int, kv *partialKeyValue) {
	switch kv.Tag {
	case partialTagInt128:
		dst.SetValue(i, kv.Dec)
	case partialTagInt64:
		dst.SetValue(i, kv.I64)
	case partialTagInt32:
		dst.SetValue(i, int32(kv.I64))
	case partialTagFloat64:
		dst.SetValue(i, kv.F64)
	case partialTagFloat32:
		dst.SetValue(i, float32(kv.F64))
	case partialTagString:
		dst.SetValue(i, string(kv.Bytes))
	case partialTagBytes:
		dst.SetValue(i, kv.Bytes)
	}
}
