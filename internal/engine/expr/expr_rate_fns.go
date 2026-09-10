// This file holds expr rate fns; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"fmt"
	"strings"
)

// fnFormatBytes formats a byte count into human-readable form.
// format_bytes(bytes)           → '1.50 GiB'  (IEC binary, default)
// format_bytes(bytes, 'si')     → '1.61 GB'   (SI decimal)
// format_bytes(bytes, 'iec')    → '1.50 GiB'  (IEC binary)
func fnFormatBytes(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])
	if v < 0 {
		return "-" + fnFormatBytes([]any{-v, safeArg(args, 1)}).(string)
	}

	units := byteUnits
	if len(args) >= 2 && args[1] != nil && strings.ToLower(toString(args[1])) == "si" {
		units = byteUnitsSI
	}

	for _, u := range units {
		if v >= u.threshold {
			val := v / u.threshold
			if val >= 100 {
				return fmt.Sprintf("%.0f %s", val, u.suffix)
			} else if val >= 10 {
				return fmt.Sprintf("%.1f %s", val, u.suffix)
			}
			return fmt.Sprintf("%.2f %s", val, u.suffix)
		}
	}
	return fmt.Sprintf("%.0f B", v)
}

// fnParseBytes parses a human-readable byte string back to numeric bytes.
// parse_bytes('1.5 GiB') → 1610612736
// parse_bytes('500 MB')   → 500000000
func fnParseBytes(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := strings.TrimSpace(toString(args[0]))
	s = strings.ToUpper(s)

	multipliers := map[string]float64{
		"B":  1,
		"KB": 1e3, "MB": 1e6, "GB": 1e9, "TB": 1e12, "PB": 1e15, "EB": 1e18,
		"KIB": 1024, "MIB": 1048576, "GIB": 1073741824,
		"TIB": 1099511627776, "PIB": 1125899906842624, "EIB": 1152921504606846976,
		// Also handle Kbps-style (bits)
		"BPS": 0.125, "KBPS": 125, "MBPS": 125000, "GBPS": 125000000,
		"KIBPS": 128, "MIBPS": 131072, "GIBPS": 134217728,
	}

	// Try each suffix from longest to shortest
	for _, suffix := range []string{
		"GIBPS", "MIBPS", "KIBPS", "GBPS", "MBPS", "KBPS", "BPS",
		"EIB", "PIB", "TIB", "GIB", "MIB", "KIB",
		"EB", "PB", "TB", "GB", "MB", "KB", "B",
	} {
		if strings.HasSuffix(s, suffix) {
			numStr := strings.TrimSpace(s[:len(s)-len(suffix)])
			if numStr == "" {
				return nil
			}
			num := ToFloat64(numStr)
			return int64(num * multipliers[suffix])
		}
	}
	// No unit — assume raw bytes
	return int64(ToFloat64(s))
}

// fnFormatRate formats a byte-per-second rate into human-readable form.
// format_rate(bytes_per_sec)            → '1.50 Gbps'  (bits/sec, default)
// format_rate(bytes_per_sec, 'bytes')   → '192.00 MiB/s'  (bytes/sec)
// format_rate(bytes_per_sec, 'si')      → '1.50 Gbps'  (SI bits/sec)
func fnFormatRate(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	bytesPerSec := ToFloat64(args[0])

	mode := "bits"
	if len(args) >= 2 && args[1] != nil {
		mode = strings.ToLower(toString(args[1]))
	}

	if mode == "bytes" || mode == "byte" {
		// Format as bytes/sec using IEC units
		formatted := fnFormatBytes([]any{bytesPerSec})
		if formatted == nil {
			return nil
		}
		return formatted.(string) + "/s"
	}

	// Format as bits/sec (SI)
	bitsPerSec := bytesPerSec * 8

	type rateUnit struct {
		threshold float64
		suffix    string
	}
	units := []rateUnit{
		{1e12, "Tbps"},
		{1e9, "Gbps"},
		{1e6, "Mbps"},
		{1e3, "Kbps"},
	}

	for _, u := range units {
		if bitsPerSec >= u.threshold {
			val := bitsPerSec / u.threshold
			if val >= 100 {
				return fmt.Sprintf("%.0f %s", val, u.suffix)
			} else if val >= 10 {
				return fmt.Sprintf("%.1f %s", val, u.suffix)
			}
			return fmt.Sprintf("%.2f %s", val, u.suffix)
		}
	}
	return fmt.Sprintf("%.0f bps", bitsPerSec)
}

// fnParseRate parses a human-readable rate string back to bytes per second.
// parse_rate('1.5 Gbps')   → 187500000  (bytes/sec)
// parse_rate('100 Mbps')   → 12500000   (bytes/sec)
// parse_rate('10 MiB/s')   → 10485760   (bytes/sec)
func fnParseRate(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := strings.TrimSpace(toString(args[0]))
	upper := strings.ToUpper(s)

	// Bits-per-second units → convert to bytes/sec
	bitRates := []struct {
		suffix     string
		bitsPerSec float64
	}{
		{"TBPS", 1e12},
		{"GBPS", 1e9},
		{"MBPS", 1e6},
		{"KBPS", 1e3},
		{"BPS", 1},
	}

	for _, r := range bitRates {
		if strings.HasSuffix(upper, r.suffix) {
			numStr := strings.TrimSpace(upper[:len(upper)-len(r.suffix)])
			if numStr == "" {
				return nil
			}
			bps := ToFloat64(numStr) * r.bitsPerSec
			return int64(bps / 8) // bits to bytes
		}
	}

	// Bytes-per-second units (MiB/s, GB/s, etc.)
	byteRates := []struct {
		suffix      string
		bytesPerSec float64
	}{
		{"TIB/S", 1099511627776},
		{"GIB/S", 1073741824},
		{"MIB/S", 1048576},
		{"KIB/S", 1024},
		{"TB/S", 1e12},
		{"GB/S", 1e9},
		{"MB/S", 1e6},
		{"KB/S", 1e3},
		{"B/S", 1},
	}

	for _, r := range byteRates {
		if strings.HasSuffix(upper, r.suffix) {
			numStr := strings.TrimSpace(upper[:len(upper)-len(r.suffix)])
			if numStr == "" {
				return nil
			}
			return int64(ToFloat64(numStr) * r.bytesPerSec)
		}
	}

	// No unit — assume bytes/sec
	return int64(ToFloat64(s))
}

func safeArg(args []any, i int) any {
	if i < len(args) {
		return args[i]
	}
	return nil
}
