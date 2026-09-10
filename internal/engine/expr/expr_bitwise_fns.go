// This file holds expr bitwise fns; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

// --- Bitwise functions ---

func fnBitwiseAnd(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	return float64(int64(ToFloat64(args[0])) & int64(ToFloat64(args[1])))
}

func fnBitwiseOr(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	return float64(int64(ToFloat64(args[0])) | int64(ToFloat64(args[1])))
}

func fnBitwiseXor(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	return float64(int64(ToFloat64(args[0])) ^ int64(ToFloat64(args[1])))
}

func fnBitwiseNot(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return float64(^int64(ToFloat64(args[0])))
}
