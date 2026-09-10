// This file holds expr trig fns; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"math"
	"math/rand"
)

// --- Math: trigonometry ---

func fnPi(args []any) any {
	return math.Pi
}

func fnDegrees(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return ToFloat64(args[0]) * 180.0 / math.Pi
}

func fnRadians(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return ToFloat64(args[0]) * math.Pi / 180.0
}

func fnSin(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return math.Sin(ToFloat64(args[0]))
}

func fnCos(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return math.Cos(ToFloat64(args[0]))
}

func fnTan(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return math.Tan(ToFloat64(args[0]))
}

func fnAsin(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	raiseTrigDomain(v)
	return math.Asin(v)
}

func fnAcos(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	raiseTrigDomain(v)
	return math.Acos(v)
}

func fnAtan(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return math.Atan(ToFloat64(args[0]))
}

func fnAtan2(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	return math.Atan2(ToFloat64(args[0]), ToFloat64(args[1]))
}

func fnCbrt(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return math.Cbrt(ToFloat64(args[0]))
}

func fnLog2(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	// PostgreSQL has no LOG2; this is a wadjet extension, and it takes LOG's
	// refusal because one engine cannot have two answers to "what is the
	// logarithm of zero" depending on the base a caller spells.
	raiseLogarithmDomain(v)
	return math.Log2(v)
}

func fnTruncate(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	decimals := 0
	if len(args) >= 2 && args[1] != nil {
		decimals = int(ToFloat64(args[1]))
	}
	pow := math.Pow(10, float64(decimals))
	return math.Trunc(v*pow) / pow
}

func fnRandom(args []any) any {
	return rand.Float64()
}
