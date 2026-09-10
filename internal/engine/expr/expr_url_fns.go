// This file holds expr url fns; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"fmt"
	"net/url"
)

// --- URL functions ---

func fnURLExtractHost(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	u, err := url.Parse(toString(args[0]))
	if err != nil {
		return nil
	}
	return u.Hostname()
}

func fnURLExtractPort(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	u, err := url.Parse(toString(args[0]))
	if err != nil || u.Port() == "" {
		return nil
	}
	var port int
	if _, err := fmt.Sscanf(u.Port(), "%d", &port); err != nil {
		return nil
	}
	return float64(port)
}

func fnURLExtractPath(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	u, err := url.Parse(toString(args[0]))
	if err != nil {
		return nil
	}
	return u.Path
}

func fnURLExtractProtocol(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	u, err := url.Parse(toString(args[0]))
	if err != nil {
		return nil
	}
	return u.Scheme
}

func fnURLExtractQuery(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	u, err := url.Parse(toString(args[0]))
	if err != nil {
		return nil
	}
	return u.RawQuery
}

func fnURLExtractParameter(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	u, err := url.Parse(toString(args[0]))
	if err != nil {
		return nil
	}
	return u.Query().Get(toString(args[1]))
}
