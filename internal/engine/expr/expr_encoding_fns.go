// This file holds expr encoding fns; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"encoding/base64"
	"fmt"
)

// --- Encoding functions ---

func fnToHex(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return fmt.Sprintf("%x", int64(ToFloat64(args[0])))
}

func fnFromHex(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	var n int64
	_, err := fmt.Sscanf(toString(args[0]), "%x", &n)
	if err != nil {
		return nil
	}
	return float64(n)
}

func fnToBase64(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return base64.StdEncoding.EncodeToString([]byte(toString(args[0])))
}

func fnFromBase64(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	decoded, err := base64.StdEncoding.DecodeString(toString(args[0]))
	if err != nil {
		return nil
	}
	return string(decoded)
}
