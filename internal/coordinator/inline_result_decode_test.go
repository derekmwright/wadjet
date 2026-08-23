package coordinator

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// decodeInlineResult used to answer (nil, nil, 0) and log at Debug for every
// failure — a corrupt payload, a truncated shuffle blob, an unreadable
// parquet result. The caller cannot tell that apart from a worker that
// legitimately produced no rows, so one bad partial silently removed that
// worker's whole share of the answer and the query came back short with
// nothing said. readOneResultFile, reading the same bytes off S3 instead of
// inline, has always returned the error.
func TestDecodeInlineResultReportsFailures(t *testing.T) {
	var c Coordinator
	cases := []struct {
		name string
		data []byte
		want string
	}{
		{"s2 frame that is not one", append([]byte("WSHC"), 0xFF, 0xFF, 0xFF, 0xFF), "decompressing"},
		{"WSHF header with no body", []byte("WSHF"), "shuffle"},
		// The truncated BODY cases (#422). Each of these used to index
		// straight off the end of the slice inside the decode goroutine of
		// readInlineResults, which nothing above recovers: a short payload
		// from one worker took the whole coordinator down.
		{"header promises a chunk that is not there",
			append([]byte("WSHF"), 1, 0, 0, 0, 1, 0), "truncated"},
		{"schema name runs past the end",
			append([]byte("WSHF"), 1, 0, 0, 0, 1, 0, 0xFF, 0x00), "schema"},
		{"chunk row count with no column data",
			truncatedInlineChunk(), "chunk"},
		{"not parquet at all", []byte("this is not a parquet file, nor a shuffle blob"), "parquet"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			batches, cols, rows, err := c.decodeInlineResult(tc.data)
			if err == nil {
				t.Fatalf("accepted: %d batches, %v, %d rows", len(batches), cols, rows)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
			if batches != nil || rows != 0 {
				t.Errorf("results came back alongside the error: %d batches, %d rows", len(batches), rows)
			}
		})
	}
}

// truncatedInlineChunk is a syntactically valid WSHF header for one INT64
// column promising one chunk of eight rows, then nothing. The bytes the
// chunk claims never arrive.
func truncatedInlineChunk() []byte {
	b := []byte("WSHF")
	b = append(b, 1, 0, 0, 0) // numChunks = 1
	b = append(b, 1, 0)       // numCols = 1
	b = append(b, 1, 0)       // name length = 1
	b = append(b, 'v')
	b = append(b, byte(parquet.TypeInt64))
	b = append(b, 8, 0, 0, 0) // chunk row count = 8, and the payload ends
	return b
}
