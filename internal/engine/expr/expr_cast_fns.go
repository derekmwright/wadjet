// This file holds expr cast fns; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"fmt"
	"math"
)

// --- Type casting ---

func fnCastInt(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	// Same rule as CAST(x AS integer): round half away from zero (#373).
	if i, ok := toInt64Safe(args[0]); ok {
		return i
	}
	return int64(math.Round(ToFloat64(args[0])))
}

func fnCastFloat(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return ToFloat64(args[0])
}

func fnCastString(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return fmt.Sprint(args[0])
}
