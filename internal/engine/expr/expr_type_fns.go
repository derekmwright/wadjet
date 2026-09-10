// This file holds expr type fns; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"fmt"
)

// --- Type introspection ---

func fnTypeof(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return "null"
	}
	switch args[0].(type) {
	case int64:
		return "bigint"
	case int32:
		return "integer"
	case int:
		return "integer"
	case float64:
		return "double"
	case float32:
		return "real"
	case string:
		return "varchar"
	case bool:
		return "boolean"
	case []byte:
		return "varbinary"
	default:
		return fmt.Sprintf("%T", args[0])
	}
}
