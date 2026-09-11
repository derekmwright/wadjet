package exec

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// Container group-key VALUES need a lossless spill codec, separate from merge keys
// (#361, #566, #576). A merge key folds NaNs/-0 and re-keys CIDR; never decode a value from it.
// Round-trip Vector.GetValue/SetValue boxes: nil, bool, int32/int64, float32/float64,
// string, []byte, []any (ARRAY or MAP entries), map[string]any (ROW), []float32
// (VECTOR), plus batch.Int128 for compact DECIMAL keys; retain nested shapes.
// Every element is self-delimiting: a kind tag plus fixed-width or length-prefixed
// payload. Preserve float RAW bits, including NaN payloads and signed zero.
// Both encoder and decoder enforce ckMaxDepth; refused subtrees must decode as errors.
// See docs/internals/container-group-key-value-codec.md for the design.
const (
	ckNull byte = iota
	ckFalse
	ckTrue
	ckInt32
	ckInt64
	ckFloat32
	ckFloat64
	ckString
	ckBytes
	ckList
	ckFields
	ckFloats
	ckDecimal
	// ckTooDeep marks a subtree the encoder REFUSED, and every decode of it
	// is an error. It exists so the depth cap cannot be enforced on only one
	// side: the encoder used to write the subtree's `%v` rendering instead,
	// which is not injective — two different values render alike and would
	// become one group — and the reader errored on the depth before it ever
	// looked at the tag anyway, so those bytes encoded and then failed to
	// decode. Refusing loudly at both ends is the only version of this that
	// is neither a wrong answer nor a write nobody can read.
	ckTooDeep
)

// ckMaxDepth bounds the recursion on BOTH sides, at the same depth. Container
// nesting comes from a declared schema, which is a handful of levels at most;
// the cap is here so a corrupted run file cannot drive the decoder into a
// stack overflow, and the encoder honours the same number so it never writes
// bytes its own reader would refuse.
const ckMaxDepth = 64

// isBoxedContainer reports whether v is one of the three boxed shapes
// Vector.GetValue produces for a container column. Callers use it to decide
// whether a group-key box needs the container encoding — the flat shapes
// keep their existing typed slots, so nothing about a non-container key
// changes.
func isBoxedContainer(v any) bool {
	switch v.(type) {
	case []any, map[string]any, []float32:
		return true
	}
	return false
}

// appendContainerKeyValue appends v's tagged, lossless encoding to buf.
func appendContainerKeyValue(buf []byte, v any, depth int) []byte {
	if depth > ckMaxDepth {
		// Unreachable from a declared schema. Marked refused rather than
		// rendered: a rendering is not injective, and this is a group KEY.
		return append(buf, ckTooDeep)
	}
	switch tv := v.(type) {
	case nil:
		return append(buf, ckNull)
	case bool:
		if tv {
			return append(buf, ckTrue)
		}
		return append(buf, ckFalse)
	case int32:
		return binary.LittleEndian.AppendUint32(append(buf, ckInt32), uint32(tv))
	case int:
		return binary.LittleEndian.AppendUint64(append(buf, ckInt64), uint64(int64(tv)))
	case int64:
		return binary.LittleEndian.AppendUint64(append(buf, ckInt64), uint64(tv))
	case float32:
		return binary.LittleEndian.AppendUint32(append(buf, ckFloat32), math.Float32bits(tv))
	case float64:
		return binary.LittleEndian.AppendUint64(append(buf, ckFloat64), math.Float64bits(tv))
	case string:
		return appendCkRaw(append(buf, ckString), tv)
	case []byte:
		return appendCkRaw(append(buf, ckBytes), string(tv))
	case batch.Int128:
		buf = binary.LittleEndian.AppendUint64(append(buf, ckDecimal), uint64(tv.Hi))
		return binary.LittleEndian.AppendUint64(buf, tv.Lo)
	case []any:
		buf = binary.AppendUvarint(append(buf, ckList), uint64(len(tv)))
		for _, e := range tv {
			buf = appendContainerKeyValue(buf, e, depth+1)
		}
		return buf
	case map[string]any:
		// Field order is not part of the value — Go map iteration is
		// randomized and the encoding has to be stable across the two sides
		// of a merge, so names are sorted the way mapEntryRows and
		// appendKeyFields already sort them.
		buf = binary.AppendUvarint(append(buf, ckFields), uint64(len(tv)))
		names := make([]string, 0, len(tv))
		for name := range tv {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			buf = appendCkRaw(buf, name)
			buf = appendContainerKeyValue(buf, tv[name], depth+1)
		}
		return buf
	case []float32:
		buf = binary.AppendUvarint(append(buf, ckFloats), uint64(len(tv)))
		for _, f := range tv {
			buf = binary.LittleEndian.AppendUint32(buf, math.Float32bits(f))
		}
		return buf
	default:
		// A box GetValue does not produce. Rendered — the last resort
		// setPartialKeyFromAny's default already took — but TAGGED, so the
		// decoder hands back a string instead of misreading a payload whose
		// width it cannot know.
		return appendCkRaw(append(buf, ckString), fmt.Sprint(tv))
	}
}

func appendCkRaw(buf []byte, s string) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(s)))
	return append(buf, s...)
}

// decodeContainerKeyValue rebuilds the boxed value appendContainerKeyValue
// wrote. The payload must be consumed exactly: trailing bytes are a
// corruption, not slack.
func decodeContainerKeyValue(b []byte) (any, error) {
	r := ckReader{b: b}
	v, err := r.value(0)
	if err != nil {
		return nil, err
	}
	if r.pos != len(b) {
		return nil, fmt.Errorf("container key: %d bytes but the walk consumed %d", len(b), r.pos)
	}
	return v, nil
}

type ckReader struct {
	b   []byte
	pos int
}

func (r *ckReader) take(n int) ([]byte, error) {
	if n < 0 || r.pos+n > len(r.b) {
		return nil, fmt.Errorf("container key: want %d bytes at %d, have %d", n, r.pos, len(r.b)-r.pos)
	}
	out := r.b[r.pos : r.pos+n]
	r.pos += n
	return out, nil
}

func (r *ckReader) uvarint() (int, error) {
	v, n := binary.Uvarint(r.b[r.pos:])
	if n <= 0 {
		return 0, fmt.Errorf("container key: malformed length at %d", r.pos)
	}
	r.pos += n
	// A count is bounded by the bytes left: every element costs at least its
	// tag byte, so a header claiming more than that is corruption, and
	// refusing it here is what keeps the make() below from a huge allocation.
	if v > uint64(len(r.b)-r.pos) {
		return 0, fmt.Errorf("container key: length %d exceeds the %d bytes left", v, len(r.b)-r.pos)
	}
	return int(v), nil
}

func (r *ckReader) raw() (string, error) {
	n, err := r.uvarint()
	if err != nil {
		return "", err
	}
	p, err := r.take(n)
	if err != nil {
		return "", err
	}
	return string(p), nil
}

func (r *ckReader) value(depth int) (any, error) {
	if depth > ckMaxDepth {
		return nil, fmt.Errorf("container key: nesting deeper than %d", ckMaxDepth)
	}
	tag, err := r.take(1)
	if err != nil {
		return nil, err
	}
	switch tag[0] {
	case ckNull:
		return nil, nil
	case ckFalse:
		return false, nil
	case ckTrue:
		return true, nil
	case ckInt32:
		p, err := r.take(4)
		if err != nil {
			return nil, err
		}
		return int32(binary.LittleEndian.Uint32(p)), nil
	case ckInt64:
		p, err := r.take(8)
		if err != nil {
			return nil, err
		}
		return int64(binary.LittleEndian.Uint64(p)), nil
	case ckFloat32:
		p, err := r.take(4)
		if err != nil {
			return nil, err
		}
		return math.Float32frombits(binary.LittleEndian.Uint32(p)), nil
	case ckFloat64:
		p, err := r.take(8)
		if err != nil {
			return nil, err
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(p)), nil
	case ckString:
		return r.raw()
	case ckBytes:
		s, err := r.raw()
		if err != nil {
			return nil, err
		}
		return []byte(s), nil
	case ckDecimal:
		p, err := r.take(16)
		if err != nil {
			return nil, err
		}
		return batch.Int128{
			Hi: int64(binary.LittleEndian.Uint64(p[:8])),
			Lo: binary.LittleEndian.Uint64(p[8:]),
		}, nil
	case ckList:
		n, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		out := make([]any, n)
		for i := 0; i < n; i++ {
			if out[i], err = r.value(depth + 1); err != nil {
				return nil, err
			}
		}
		return out, nil
	case ckFields:
		n, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		out := make(map[string]any, n)
		for i := 0; i < n; i++ {
			name, err := r.raw()
			if err != nil {
				return nil, err
			}
			v, err := r.value(depth + 1)
			if err != nil {
				return nil, err
			}
			out[name] = v
		}
		return out, nil
	case ckTooDeep:
		return nil, fmt.Errorf("container key: a subtree nested deeper than %d was refused at encode", ckMaxDepth)
	case ckFloats:
		n, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		p, err := r.take(n * 4)
		if err != nil {
			return nil, err
		}
		out := make([]float32, n)
		for i := 0; i < n; i++ {
			out[i] = math.Float32frombits(binary.LittleEndian.Uint32(p[i*4:]))
		}
		return out, nil
	default:
		return nil, fmt.Errorf("container key: unknown kind tag %d at %d", tag[0], r.pos-1)
	}
}
