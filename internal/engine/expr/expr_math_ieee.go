// This file holds expr math ieee; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"math"
	"math/bits"
	"strconv"
)

// --- Math: IEEE 754 and utility ---

func fnE(args []any) any {
	return math.E
}

func fnLog10(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	if v <= 0 {
		return nil
	}
	return math.Log10(v)
}

func fnInfinity(args []any) any {
	return math.Inf(1)
}

func fnNaN(args []any) any {
	return math.NaN()
}

func fnIsNaN(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return math.IsNaN(ToFloat64(args[0]))
}

func fnIsFinite(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	return !math.IsInf(v, 0) && !math.IsNaN(v)
}

func fnIsInfinite(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return math.IsInf(ToFloat64(args[0]), 0)
}

func fnWidthBucket(args []any) any {
	if len(args) < 4 || args[0] == nil || args[1] == nil || args[2] == nil || args[3] == nil {
		return nil
	}
	value := ToFloat64(args[0])
	bound1 := ToFloat64(args[1])
	bound2 := ToFloat64(args[2])
	n := int(ToFloat64(args[3]))
	// PostgreSQL refuses both of these with 2201G rather than answering NULL:
	// a non-positive count names no buckets, and equal bounds leave the width
	// zero (#855). The two messages are distinct on the server and are kept
	// distinct here.
	if n <= 0 {
		raiseWidthBucketCount()
	}
	if bound1 == bound2 {
		raiseWidthBucketBounds()
	}
	if value < bound1 {
		return int32(0)
	}
	if value >= bound2 {
		return int32(n + 1)
	}
	width := (bound2 - bound1) / float64(n)
	bucket := int((value-bound1)/width) + 1
	if bucket > n {
		bucket = n + 1
	}
	return int32(bucket)
}

func fnFromBase(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	s := toString(args[0])
	base := int(ToFloat64(args[1]))
	if base < 2 || base > 36 {
		return nil
	}
	n, err := strconv.ParseInt(s, base, 64)
	if err != nil {
		return nil
	}
	return float64(n)
}

func fnToBase(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	n := int64(ToFloat64(args[0]))
	base := int(ToFloat64(args[1]))
	if base < 2 || base > 36 {
		return nil
	}
	return strconv.FormatInt(n, base)
}

func fnBitCount(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	n := int64(ToFloat64(args[0]))
	return float64(bits.OnesCount64(uint64(n)))
}
