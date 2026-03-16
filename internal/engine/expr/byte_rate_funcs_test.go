package expr

import (
	"math"
	"testing"
)

func TestFormatBytes(t *testing.T) {
	fn := DefaultRegistry.Lookup("format_bytes")
	if fn == nil {
		t.Fatal("format_bytes function not registered")
	}

	tests := []struct {
		name string
		args []any
		want any
	}{
		// IEC (binary) defaults
		{"zero bytes", []any{float64(0)}, "0 B"},
		{"small bytes", []any{float64(512)}, "512 B"},
		{"1 KiB", []any{float64(1024)}, "1.00 KiB"},
		{"1.5 KiB", []any{float64(1536)}, "1.50 KiB"},
		{"1 MiB", []any{float64(1048576)}, "1.00 MiB"},
		{"1 GiB", []any{float64(1073741824)}, "1.00 GiB"},
		{"1.5 GiB", []any{float64(1610612736)}, "1.50 GiB"},
		{"1 TiB", []any{float64(1099511627776)}, "1.00 TiB"},
		{"large value", []any{float64(10737418240)}, "10.0 GiB"},
		{"very large value", []any{float64(107374182400)}, "100 GiB"},

		// SI mode
		{"1 GB SI", []any{float64(1e9), "si"}, "1.00 GB"},
		{"500 MB SI", []any{float64(500e6), "si"}, "500 MB"},
		{"1.5 GB SI", []any{float64(1.5e9), "si"}, "1.50 GB"},
		{"1 TB SI", []any{float64(1e12), "si"}, "1.00 TB"},

		// IEC explicit
		{"1 GiB explicit", []any{float64(1073741824), "iec"}, "1.00 GiB"},

		// Negative values
		{"negative bytes", []any{float64(-1073741824)}, "-1.00 GiB"},

		// Nil handling
		{"nil input", []any{nil}, nil},
		{"no args", []any{}, nil},

		// Integer input
		{"int64 input", []any{int64(1048576)}, "1.00 MiB"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := fn(tt.args)
			if got != tt.want {
				t.Errorf("format_bytes(%v) = %v, want %v", tt.args, got, tt.want)
			}
		})
	}
}

func TestParseBytes(t *testing.T) {
	fn := DefaultRegistry.Lookup("parse_bytes")
	if fn == nil {
		t.Fatal("parse_bytes function not registered")
	}

	tests := []struct {
		name string
		args []any
		want any
	}{
		{"1.5 GiB", []any{"1.5 GiB"}, int64(1610612736)},
		{"500 MB", []any{"500 MB"}, int64(500000000)},
		{"1 KiB", []any{"1 KiB"}, int64(1024)},
		{"1 MiB", []any{"1 MiB"}, int64(1048576)},
		{"1 TiB", []any{"1 TiB"}, int64(1099511627776)},
		{"100 KB", []any{"100 KB"}, int64(100000)},
		{"1 GB", []any{"1 GB"}, int64(1000000000)},
		{"raw number", []any{"1024"}, int64(1024)},
		{"100 B", []any{"100 B"}, int64(100)},

		// Nil handling
		{"nil input", []any{nil}, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := fn(tt.args)
			if got != tt.want {
				t.Errorf("parse_bytes(%v) = %v (%T), want %v (%T)", tt.args, got, got, tt.want, tt.want)
			}
		})
	}
}

func TestFormatBytesParseRoundTrip(t *testing.T) {
	formatFn := DefaultRegistry.Lookup("format_bytes")
	parseFn := DefaultRegistry.Lookup("parse_bytes")

	// format_bytes then parse_bytes should round-trip approximately
	values := []float64{0, 1024, 1048576, 1073741824, 1099511627776}
	for _, v := range values {
		formatted := formatFn([]any{v})
		if formatted == nil {
			continue
		}
		parsed := parseFn([]any{formatted})
		if parsed == nil {
			t.Errorf("parse_bytes(format_bytes(%v)) = nil", v)
			continue
		}
		parsedFloat := float64(parsed.(int64))
		// Allow small rounding differences due to formatting precision
		if v > 0 && math.Abs(parsedFloat-v)/v > 0.01 {
			t.Errorf("round-trip: format_bytes(%v) = %v, parse_bytes = %v (diff > 1%%)", v, formatted, parsed)
		}
	}
}

func TestFormatRate(t *testing.T) {
	fn := DefaultRegistry.Lookup("format_rate")
	if fn == nil {
		t.Fatal("format_rate function not registered")
	}

	tests := []struct {
		name string
		args []any
		want any
	}{
		// Default: bits per second
		{"zero", []any{float64(0)}, "0 bps"},
		{"1 Gbps", []any{float64(125000000)}, "1.00 Gbps"},       // 125M bytes/s = 1 Gbps
		{"100 Mbps", []any{float64(12500000)}, "100 Mbps"},        // 12.5M bytes/s = 100 Mbps
		{"10 Mbps", []any{float64(1250000)}, "10.0 Mbps"},         // 1.25M bytes/s = 10 Mbps
		{"1 Kbps", []any{float64(125)}, "1.00 Kbps"},              // 125 bytes/s = 1 Kbps
		{"1.5 Gbps", []any{float64(187500000)}, "1.50 Gbps"},      // 187.5M bytes/s = 1.5 Gbps

		// Bytes mode
		{"bytes mode", []any{float64(1048576), "bytes"}, "1.00 MiB/s"},
		{"bytes mode large", []any{float64(1073741824), "bytes"}, "1.00 GiB/s"},

		// SI bits mode explicit
		{"si mode", []any{float64(125000000), "si"}, "1.00 Gbps"},

		// Nil handling
		{"nil input", []any{nil}, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := fn(tt.args)
			if got != tt.want {
				t.Errorf("format_rate(%v) = %v, want %v", tt.args, got, tt.want)
			}
		})
	}
}

func TestParseRate(t *testing.T) {
	fn := DefaultRegistry.Lookup("parse_rate")
	if fn == nil {
		t.Fatal("parse_rate function not registered")
	}

	tests := []struct {
		name string
		args []any
		want any
	}{
		// Bits per second → bytes per second
		{"1.5 Gbps", []any{"1.5 Gbps"}, int64(187500000)},
		{"100 Mbps", []any{"100 Mbps"}, int64(12500000)},
		{"1 Kbps", []any{"1 Kbps"}, int64(125)},
		{"1 Tbps", []any{"1 Tbps"}, int64(125000000000)},

		// Bytes per second
		{"10 MiB/s", []any{"10 MiB/s"}, int64(10485760)},
		{"1 GiB/s", []any{"1 GiB/s"}, int64(1073741824)},
		{"1 GB/s", []any{"1 GB/s"}, int64(1000000000)},
		{"1 MB/s", []any{"1 MB/s"}, int64(1000000)},
		{"1 KB/s", []any{"1 KB/s"}, int64(1000)},
		{"1 B/s", []any{"1 B/s"}, int64(1)},

		// Raw number (assume bytes/sec)
		{"raw number", []any{"1000"}, int64(1000)},

		// Nil handling
		{"nil input", []any{nil}, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := fn(tt.args)
			if got != tt.want {
				t.Errorf("parse_rate(%v) = %v (%T), want %v (%T)", tt.args, got, got, tt.want, tt.want)
			}
		})
	}
}

func TestFormatRateParseRoundTrip(t *testing.T) {
	formatFn := DefaultRegistry.Lookup("format_rate")
	parseFn := DefaultRegistry.Lookup("parse_rate")

	// Bits mode: format then parse should round-trip approximately
	bytesPerSec := []float64{125000000, 12500000, 1250000, 125000}
	for _, v := range bytesPerSec {
		formatted := formatFn([]any{v})
		if formatted == nil {
			continue
		}
		parsed := parseFn([]any{formatted})
		if parsed == nil {
			t.Errorf("parse_rate(format_rate(%v)) = nil", v)
			continue
		}
		parsedFloat := float64(parsed.(int64))
		if v > 0 && math.Abs(parsedFloat-v)/v > 0.01 {
			t.Errorf("round-trip: format_rate(%v) = %v, parse_rate = %v (diff > 1%%)", v, formatted, parsed)
		}
	}
}
