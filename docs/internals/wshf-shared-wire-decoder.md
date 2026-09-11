# Wshf shared wire decoder

Source: internal/wshf/wshf.go — package wshf, moved 2026-09-11 (#1026)

Package wshf owns the WSHF columnar shuffle wire format: the magics, the
envelope codecs, and the one bounds-checked decoder every consumer uses.

The format replaces Parquet for inter-stage shuffle data. It avoids
per-row goparquet.Value allocation (nRows × nCols objects), alphabetical
column reordering, and Parquet page/RLE encoding overhead.

	Magic "WSHF" (4 bytes)
	NumChunks uint32 (4 bytes)
	NumCols   uint16 (2 bytes)
	Schema: for each column:
	  NameLen uint16
	  Name    []byte
	  TypeID  uint8
	  Scale, Precision uint8 ×2   — DECIMAL only
	Chunks: for each chunk:
	  NumRows uint32 (4 bytes)
	  For each column:
	    NullBitmapWords uint32 (number of uint64 words)
	    NullBitmap      []uint64
	    DataLen         uint32 (byte length of column data)
	    Data            []byte (type-dependent raw data)

The WRITER lives in internal/worker (it needs the engine's batch gather,
view resolution and the WIDX extent-index footer). This package is the
read side, and it is the only read side: the coordinator's inline-result
path and the worker's file/stream/pread paths all decode through it, so
a payload cannot be interpreted two ways (#422).

Every read goes through Cursor, which returns an error rather than
panicking on short input. That matters because the bytes are untrusted:
the coordinator decodes a NATS payload from a worker in the decode
goroutine of readInlineResults, where a panic is not a failed query but
a dead coordinator.
