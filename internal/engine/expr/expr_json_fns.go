// This file holds expr json fns; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"encoding/json"
	"fmt"
	"strings"
)

// --- JSON functions ---

func fnJSONExtract(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	jsonStr := toString(args[0])
	path := toString(args[1])
	result := jsonPathExtract(jsonStr, path)
	if result == nil {
		return nil
	}
	// Return as JSON string for non-scalar values
	switch v := result.(type) {
	case string:
		return v
	case float64:
		return v
	case bool:
		return v
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return nil
		}
		return string(b)
	}
}

func fnJSONExtractScalar(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	result := jsonPathExtract(toString(args[0]), toString(args[1]))
	if result == nil {
		return nil
	}
	switch v := result.(type) {
	case string:
		return v
	case float64:
		return v
	case bool:
		if v {
			return "true"
		}
		return "false"
	default:
		return nil // non-scalar
	}
}

func fnJSONArrayLength(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := toString(args[0])
	var arr []any
	if err := json.Unmarshal([]byte(s), &arr); err != nil {
		return nil
	}
	return float64(len(arr))
}

func fnJSONValid(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return json.Valid([]byte(toString(args[0])))
}

// jsonPathExtract extracts a value from a JSON string using a simple dot-path.
// Supports: $.key, $.key.nested, $.key[0], $.key[0].nested
func jsonPathExtract(jsonStr, path string) any {
	var data any
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return nil
	}
	// Strip leading "$." or "$"
	if strings.HasPrefix(path, "$.") {
		path = path[2:]
	} else if strings.HasPrefix(path, "$") {
		path = path[1:]
	}
	if path == "" {
		return data
	}
	return navigateJSON(data, path)
}

func navigateJSON(data any, path string) any {
	current := data
	for path != "" {
		// Parse next segment
		var segment string
		dotIdx := strings.IndexAny(path, ".[")
		if dotIdx < 0 {
			segment = path
			path = ""
		} else if path[dotIdx] == '.' {
			segment = path[:dotIdx]
			path = path[dotIdx+1:]
		} else {
			// '[' found
			segment = path[:dotIdx]
			path = path[dotIdx:]
		}

		if segment != "" {
			obj, ok := current.(map[string]any)
			if !ok {
				return nil
			}
			current = obj[segment]
			if current == nil {
				return nil
			}
		}

		// Handle array index [N]
		if strings.HasPrefix(path, "[") {
			end := strings.Index(path, "]")
			if end < 0 {
				return nil
			}
			idxStr := path[1:end]
			path = path[end+1:]
			if strings.HasPrefix(path, ".") {
				path = path[1:]
			}
			var idx int
			if _, err := fmt.Sscanf(idxStr, "%d", &idx); err != nil {
				return nil
			}
			arr, ok := current.([]any)
			if !ok || idx < 0 || idx >= len(arr) {
				return nil
			}
			current = arr[idx]
		}
	}
	return current
}
