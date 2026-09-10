// This file holds expr url encoding; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"net/url"
)

// --- URL encoding/decoding ---

func fnURLEncode(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return url.QueryEscape(toString(args[0]))
}

func fnURLDecode(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	decoded, err := url.QueryUnescape(toString(args[0]))
	if err != nil {
		return nil
	}
	return decoded
}
