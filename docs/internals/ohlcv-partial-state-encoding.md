# Ohlcv partial state encoding

Source: internal/engine/exec/agg_ohlcv.go — ohlcvStateWidth, moved 2026-09-11 (#1026)

ohlcvStateWidth is the encoded width of a bar's partial state: 136 raw bytes
hex-encoded to 272 ASCII characters.

	[0]       format version (1)
	[1]       flags: bit0 exact, bit1 overflow
	[2]       carrier price scale  [3] volume scale  [4] product scale
	[5:8]     the PRICE field's declared (type, precision, scale)
	[8:11]    the VOLUME field's declared (type, precision, scale)
	[11:14]   the VWAP field's declared (type, precision, scale)
	[14:16]   reserved, zero
	[16:24]   n            int64 BE
	[24:32]   firstTS      int64 BE
	[32:40]   lastTS       int64 BE
	[40:56]   open   [56:72] high   [72:88] low   [88:104] close
	[104:120] sumVolume    [120:136] sumPriceTimesVolume

A value slot is an Int128 (Hi then Lo, big-endian) when exact, and a float64
in its low 8 bytes when not. The state travels as a STRING column — through
parquet, the .wshf shuffle format and the NATS gather — for the reason
varianceState.encode records: every one of those is happier with text than
with arbitrary bytes, and float64 bits round-trip exactly through hex.

Both the carrier AND the declared ROW are in the header, so the
coordinator's fold finishes a bar with nothing but the string. Everything
there fits a byte: 22 type ids, and a DECIMAL's precision and scale top out
at 38.
