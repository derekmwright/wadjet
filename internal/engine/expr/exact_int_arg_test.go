// SPDX-License-Identifier: MIT

package expr

import "testing"

// #1031: five registry sites read an integer argument through a float64 and
// lost every digit past 2^53. This replaces the pin
// `TestTheSitesThatStillReadAnArgumentThroughADouble` (#966 round 2, P2),
// which recorded the residual and whose deletion is this fix's proof.
//
// PostgreSQL has none of these functions, so the oracle is the specification
// ADR-0024 item 2 states: an integer means the same thing in every spelling.
// 9007199254740993 is 2^53+1, the first integer a double cannot hold.
func TestEveryIntegerArgumentIsReadExactly(t *testing.T) {
	for _, tc := range []struct {
		fn   string
		args []any
		want any
	}{
		{"parse_bytes", []any{"9007199254740993"}, int64(9007199254740993)},
		{"parse_bytes", []any{"9007199254740993 B"}, int64(9007199254740993)},
		{"parse_rate", []any{"9007199254740993"}, int64(9007199254740993)},
		{"parse_rate", []any{"9007199254740993 B/S"}, int64(9007199254740993)},
		{"human_readable_seconds", []any{int64(9007199254740993)},
			"104249991374 days, 7 hours, 36 minutes, 33 seconds"},
		{"from_unixtime", []any{int64(9007199254740993)}, "285428751-11-12 07:36:33"},
		// A STRING spelling of the same integer reaches the same value: the
		// box a scalar subquery or a CAST hands over is not always an int64.
		{"human_readable_seconds", []any{"9007199254740993"},
			"104249991374 days, 7 hours, 36 minutes, 33 seconds"},
		{"from_unixtime", []any{"9007199254740993"}, "285428751-11-12 07:36:33"},

		// The controls the pin carried, unchanged: these were already exact,
		// so the fix is about the carrier and not about the functions.
		{"parse_bytes", []any{"9007199254740992"}, int64(9007199254740992)},
		{"parse_bytes", []any{"4503599627370497"}, int64(4503599627370497)},
		{"human_readable_seconds", []any{int64(4503599627370497)},
			"52124995687 days, 3 hours, 48 minutes, 17 seconds"},

		// The UNIT paths still answer what they answered. Every multiplier but
		// the bit-rate ones is a whole number, so these take the exact route;
		// a fractional spelling keeps the float product it always had.
		{"parse_bytes", []any{"1.5 GiB"}, int64(1610612736)},
		{"parse_bytes", []any{"500 MB"}, int64(500000000)},
		{"parse_bytes", []any{"2 TiB"}, int64(2199023255552)},
		{"parse_rate", []any{"1.5 Gbps"}, int64(187500000)},
		{"parse_rate", []any{"100 Mbps"}, int64(12500000)},
		{"parse_rate", []any{"10 MiB/s"}, int64(10485760)},
		// A bit rate whose exact product is not a multiple of 8 keeps the
		// float division rather than answering a truncated integer twice.
		{"parse_rate", []any{"1 bps"}, int64(0)},
	} {
		got := DefaultRegistry.Lookup(tc.fn)(tc.args)
		if got != tc.want {
			t.Errorf("%s(%v) = %#v, want %#v", tc.fn, tc.args, got, tc.want)
		}
	}
}

// A scaled product with no int64 is 22003, not the undefined result
// `int64(float64)` produces for an out-of-range double (which on amd64 is the
// most negative integer there is).
func TestAScaledByteCountWithNoIntegerIsRefused(t *testing.T) {
	for _, in := range []string{"9 EiB", "99999999999999 EB"} {
		state, msg := recoverFatalEvalForTest(t, func() {
			DefaultRegistry.Lookup("parse_bytes")([]any{in})
		})
		if state != "22003" || msg != "bigint out of range" {
			t.Errorf("PARSE_BYTES(%q) raised [%s] %s, want [22003] bigint out of range",
				in, state, msg)
		}
	}
}
