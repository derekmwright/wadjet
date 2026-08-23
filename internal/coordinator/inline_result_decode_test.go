package coordinator

import (
	"strings"
	"testing"
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
		// A truncated WSHF BODY is a separate, unfixed defect: readShuffleBatches
		// walks the payload with no bounds at all and panics rather than
		// erroring (issue filed). Only the header cases belong here.
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
