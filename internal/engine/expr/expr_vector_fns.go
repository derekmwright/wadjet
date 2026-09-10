// This file holds expr vector fns; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// --- Vectorized scalar function implementations ---
//
// These operate on entire columns (batch.Vector) instead of per-row values,
// eliminating interface dispatch, boxing/unboxing, and string↔[]byte conversion.

// vecReadFloat64 reads a float64 from a vector at the given index,
// handling both Float64 and Int64 source types.
func vecReadFloat64(v *batch.Vector, i int) float64 {
	switch v.Type {
	case batch.TypeFloat64:
		return v.Float64Data[i]
	case batch.TypeInt64, batch.TypeTimestamp:
		return float64(v.Int64Data[i])
	case batch.TypeInt32:
		return float64(v.Int32Data[i])
	case batch.TypeFloat32:
		return float64(v.Float32Data[i])
	default:
		return 0
	}
}

func vecUpper(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	totalBytes := int(src.BytesData.Offsets[n] - src.BytesData.Offsets[0])
	out.BytesData.PreAllocBytes(totalBytes)

	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			out.BytesData.Set(i, nil)
			continue
		}
		b := src.BytesData.Value(i)
		start := len(out.BytesData.Data)
		out.BytesData.Data = append(out.BytesData.Data, b...)
		for j := start; j < len(out.BytesData.Data); j++ {
			c := out.BytesData.Data[j]
			if c >= 'a' && c <= 'z' {
				out.BytesData.Data[j] = c - 32
			}
		}
		out.BytesData.Offsets[i+1] = uint32(len(out.BytesData.Data))
	}
}

func vecLower(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	totalBytes := int(src.BytesData.Offsets[n] - src.BytesData.Offsets[0])
	out.BytesData.PreAllocBytes(totalBytes)

	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			out.BytesData.Set(i, nil)
			continue
		}
		b := src.BytesData.Value(i)
		start := len(out.BytesData.Data)
		out.BytesData.Data = append(out.BytesData.Data, b...)
		for j := start; j < len(out.BytesData.Data); j++ {
			c := out.BytesData.Data[j]
			if c >= 'A' && c <= 'Z' {
				out.BytesData.Data[j] = c + 32
			}
		}
		out.BytesData.Offsets[i+1] = uint32(len(out.BytesData.Data))
	}
}

func vecTrim(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			out.BytesData.Set(i, nil)
			continue
		}
		b := src.BytesData.Value(i)
		// Trim ASCII whitespace from both ends
		lo, hi := 0, len(b)
		for lo < hi && (b[lo] == ' ' || b[lo] == '\t' || b[lo] == '\n' || b[lo] == '\r') {
			lo++
		}
		for hi > lo && (b[hi-1] == ' ' || b[hi-1] == '\t' || b[hi-1] == '\n' || b[hi-1] == '\r') {
			hi--
		}
		out.BytesData.Set(i, b[lo:hi])
	}
}

func vecLTrim(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			out.BytesData.Set(i, nil)
			continue
		}
		b := src.BytesData.Value(i)
		lo := 0
		for lo < len(b) && (b[lo] == ' ' || b[lo] == '\t' || b[lo] == '\n' || b[lo] == '\r') {
			lo++
		}
		out.BytesData.Set(i, b[lo:])
	}
}

func vecRTrim(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			out.BytesData.Set(i, nil)
			continue
		}
		b := src.BytesData.Value(i)
		hi := len(b)
		for hi > 0 && (b[hi-1] == ' ' || b[hi-1] == '\t' || b[hi-1] == '\n' || b[hi-1] == '\r') {
			hi--
		}
		out.BytesData.Set(i, b[:hi])
	}
}

func vecSubstr(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	hasLen := len(args) >= 3
	// bytea has no characters: substring over it indexes BYTES on the server,
	// and reading its bytes as UTF-8 replaced every invalid one with U+FFFD —
	// a value the column does not hold (#583). Same rule as fnSubstr's bytea
	// arm, because the two evaluators must not answer differently.
	if src.Type == batch.TypeBytes {
		for i := 0; i < n; i++ {
			if hasNulls && src.Nulls.IsNullFast(i) {
				out.Nulls.SetNull(i)
				out.BytesData.Set(i, nil)
				continue
			}
			raw := src.BytesData.Value(i)
			start := int(vecReadFloat64(args[1], i)) - 1
			if hasLen {
				length := int(vecReadFloat64(args[2], i))
				if length < 0 {
					raiseNegativeSubstringLength()
				}
				lo, hi := substrWindow(start, length, len(raw))
				out.BytesData.Set(i, raw[lo:hi])
				continue
			}
			if start < 0 {
				start = 0
			}
			if start >= len(raw) {
				out.BytesData.Set(i, nil)
				continue
			}
			out.BytesData.Set(i, raw[start:])
		}
		return
	}

	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			out.BytesData.Set(i, nil)
			continue
		}
		// CHARACTERS, matching fnSubstr — this kernel indexed BYTES and cut
		// multi-byte characters in half (#856), and the two implementations
		// have to agree or the answer depends on which evaluator ran.
		r := []rune(string(src.BytesData.Value(i)))
		start := int(vecReadFloat64(args[1], i)) - 1 // SQL is 1-indexed
		if hasLen {
			length := int(vecReadFloat64(args[2], i))
			if length < 0 {
				raiseNegativeSubstringLength()
			}
			// PostgreSQL's window rule; must match fnSubstr (#373).
			lo, hi := substrWindow(start, length, len(r))
			out.BytesData.Set(i, []byte(string(r[lo:hi])))
			continue
		}
		if start < 0 {
			start = 0
		}
		if start >= len(r) {
			out.BytesData.Set(i, nil)
			continue
		}
		out.BytesData.Set(i, []byte(string(r[start:])))
	}
}

func vecReplace(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls() || args[1].Nulls.HasNulls() || args[2].Nulls.HasNulls()

	for i := 0; i < n; i++ {
		if hasNulls && (src.Nulls.IsNullFast(i) || args[1].Nulls.IsNullFast(i) || args[2].Nulls.IsNullFast(i)) {
			out.Nulls.SetNull(i)
			out.BytesData.Set(i, nil)
			continue
		}
		s := src.BytesData.StringValue(i)
		old := args[1].BytesData.StringValue(i)
		new := args[2].BytesData.StringValue(i)
		result := strings.ReplaceAll(s, old, new)
		out.BytesData.Set(i, []byte(result))
	}
}

func vecReverse(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			out.BytesData.Set(i, nil)
			continue
		}
		// RUNES, matching fnReverse, which has always reversed runes: this
		// kernel reversed BYTES, so REVERSE('éàü') answered mojibake through
		// the vectorized path and `üàé` through the boxed one — one function,
		// two answers, decided by which evaluator the plan reached (#856).
		rev := []rune(string(src.BytesData.Value(i)))
		for j, k := 0, len(rev)-1; j < k; j, k = j+1, k-1 {
			rev[j], rev[k] = rev[k], rev[j]
		}
		out.BytesData.Set(i, []byte(string(rev)))
	}
}

func vecLeft(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			out.BytesData.Set(i, nil)
			continue
		}
		b := src.BytesData.Value(i)
		count := int(vecReadFloat64(args[1], i))
		if count < 0 {
			out.BytesData.Set(i, nil)
		} else if count >= len(b) {
			out.BytesData.Set(i, b)
		} else {
			out.BytesData.Set(i, b[:count])
		}
	}
}

func vecRight(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	hasNulls := src.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && src.Nulls.IsNullFast(i) {
			out.Nulls.SetNull(i)
			out.BytesData.Set(i, nil)
			continue
		}
		b := src.BytesData.Value(i)
		count := int(vecReadFloat64(args[1], i))
		if count < 0 {
			out.BytesData.Set(i, nil)
		} else if count >= len(b) {
			out.BytesData.Set(i, b)
		} else {
			out.BytesData.Set(i, b[len(b)-count:])
		}
	}
}

// vecConcat is fnConcat's kernel: a NULL argument contributes nothing and
// the row is never NULL. The row and vector paths are separately reachable —
// which one runs depends on whether every argument is a byte-array-shaped
// vector (see FuncCall.EvalVec) — so both carry the rule (#609).
func vecConcat(args []*batch.Vector, out *batch.Vector, n int) {
	for i := 0; i < n; i++ {
		total := 0
		for _, arg := range args {
			if arg.Nulls.HasNulls() && arg.Nulls.IsNullFast(i) {
				continue
			}
			total += int(arg.BytesData.Offsets[i+1] - arg.BytesData.Offsets[i])
		}
		buf := make([]byte, 0, total)
		for _, arg := range args {
			if arg.Nulls.HasNulls() && arg.Nulls.IsNullFast(i) {
				continue
			}
			buf = append(buf, arg.BytesData.Value(i)...)
		}
		out.BytesData.Set(i, buf)
	}
}

// vecConcatOp is fnConcatOp's kernel: the `||` operator, NULL-propagating.
func vecConcatOp(args []*batch.Vector, out *batch.Vector, n int) {
	for i := 0; i < n; i++ {
		isNull := false
		for _, arg := range args {
			if arg.Nulls.HasNulls() && arg.Nulls.IsNullFast(i) {
				isNull = true
				break
			}
		}
		if isNull {
			out.Nulls.SetNull(i)
			out.BytesData.Set(i, nil)
			continue
		}
		// Calculate total length then build in one shot
		total := 0
		for _, arg := range args {
			total += int(arg.BytesData.Offsets[i+1] - arg.BytesData.Offsets[i])
		}
		buf := make([]byte, 0, total)
		for _, arg := range args {
			buf = append(buf, arg.BytesData.Value(i)...)
		}
		out.BytesData.Set(i, buf)
	}
}

func vecStartsWith(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	prefix := args[1]
	hasNulls := src.Nulls.HasNulls() || prefix.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && (src.Nulls.IsNullFast(i) || prefix.Nulls.IsNullFast(i)) {
			out.Nulls.SetNull(i)
			continue
		}
		s := src.BytesData.Value(i)
		p := prefix.BytesData.Value(i)
		result := len(s) >= len(p)
		if result {
			for j := 0; j < len(p); j++ {
				if s[j] != p[j] {
					result = false
					break
				}
			}
		}
		out.BoolData[i] = result
	}
}

func vecEndsWith(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	suffix := args[1]
	hasNulls := src.Nulls.HasNulls() || suffix.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && (src.Nulls.IsNullFast(i) || suffix.Nulls.IsNullFast(i)) {
			out.Nulls.SetNull(i)
			continue
		}
		s := src.BytesData.Value(i)
		p := suffix.BytesData.Value(i)
		result := len(s) >= len(p)
		if result {
			off := len(s) - len(p)
			for j := 0; j < len(p); j++ {
				if s[off+j] != p[j] {
					result = false
					break
				}
			}
		}
		out.BoolData[i] = result
	}
}

func vecContains(args []*batch.Vector, out *batch.Vector, n int) {
	src := args[0]
	sub := args[1]
	hasNulls := src.Nulls.HasNulls() || sub.Nulls.HasNulls()
	for i := 0; i < n; i++ {
		if hasNulls && (src.Nulls.IsNullFast(i) || sub.Nulls.IsNullFast(i)) {
			out.Nulls.SetNull(i)
			continue
		}
		s := src.BytesData.StringValue(i)
		p := sub.BytesData.StringValue(i)
		out.BoolData[i] = strings.Contains(s, p)
	}
}
