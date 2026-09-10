// This file holds expr icmp fns; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"fmt"
)

func fnICMPTypeName(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	t := int(ToInt64(args[0]))
	name, ok := icmpTypeNames[t]
	if !ok {
		return fmt.Sprintf("Type %d", t)
	}
	return name
}

func fnICMPCodeName(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	t := int(ToInt64(args[0]))
	c := int(ToInt64(args[1]))
	switch t {
	case 3:
		if name, ok := icmpUnreachableCodes[c]; ok {
			return name
		}
	case 5:
		if name, ok := icmpRedirectCodes[c]; ok {
			return name
		}
	case 11:
		if name, ok := icmpTimeExceededCodes[c]; ok {
			return name
		}
	}
	if c == 0 {
		if name, ok := icmpTypeNames[t]; ok {
			return name
		}
	}
	return fmt.Sprintf("Type %d Code %d", t, c)
}

func fnIsICMPEcho(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	t := int(ToInt64(args[0]))
	return t == 0 || t == 8
}

func fnICMPParse(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 2 {
		return nil
	}
	return fmt.Sprintf("%d:%d", data[0], data[1])
}

func fnICMPType(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 1 {
		return nil
	}
	return int64(data[0])
}

func fnICMPCode(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 2 {
		return nil
	}
	return int64(data[1])
}
