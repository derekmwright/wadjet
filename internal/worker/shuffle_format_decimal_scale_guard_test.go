package worker

import (
	"bytes"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestShuffleWriteRefusesTwoScalesInOneFile.
//
// A WSHF file declares its schema ONCE, in the header, and every chunk after
// the first is read under it. Half a DECIMAL value lives in that header: the
// chunk carries the unscaled integer and the header carries the scale. So a
// second chunk whose vector is at a different scale is not a formatting
// mismatch — it is the same integer read as a different number, and nothing
// downstream can tell. 127501 written at scale 4 and read at scale 2 is
// 1275.01 where the value is 12.7501 (#533).
//
// The planner makes a set operation's arms agree before they meet, which is
// where #533 came from and where it is fixed. This is the backstop for every
// producer the planner did NOT reconcile, and it is what turns the whole
// class from a wrong answer into a failed task.
func TestShuffleWriteRefusesTwoScalesInOneFile(t *testing.T) {
	schema := []parquet.Column{
		{Name: "v", Type: parquet.TypeDecimal, Nullable: true, Precision: 9, Scale: 2},
	}
	chunk := func(scale int, unscaled int64) []*batch.Vector {
		v := batch.NewVectorWithScale(parquet.TypeDecimal, 1, scale)
		v.DecimalData.Data[0] = batch.Int128From(unscaled)
		return []*batch.Vector{v}
	}

	var buf bytes.Buffer
	sw := newShuffleWriter(&buf, schema)
	if err := sw.writeHeader(); err != nil {
		t.Fatal(err)
	}
	// The header's own scale writes fine.
	if err := sw.writeChunk(chunk(2, 1275), nil, 1); err != nil {
		t.Fatalf("a chunk at the header's scale must write: %v", err)
	}
	// A second chunk at a different scale must not.
	err := sw.writeChunk(chunk(4, 127501), nil, 1)
	if err == nil {
		t.Fatal("a chunk whose DECIMAL scale differs from the file header's was accepted; " +
			"its unscaled values would be read as different numbers")
	}
	for _, want := range []string{`"v"`, "scale 4", "scale 2"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should name %s; got %q", want, err)
		}
	}
}

// TestShuffleWriteAcceptsAViewAtTheHeadersScale: a view owns no storage, so
// the scale it carries is its base's — reading the view's own zero-valued
// DecimalData would reject every late-materialized DECIMAL column.
func TestShuffleWriteAcceptsAViewAtTheHeadersScale(t *testing.T) {
	schema := []parquet.Column{
		{Name: "v", Type: parquet.TypeDecimal, Nullable: true, Precision: 18, Scale: 4},
	}
	base := batch.NewVectorWithScale(parquet.TypeDecimal, 2, 4)
	base.DecimalData.Data[0] = batch.Int128From(127501)
	base.DecimalData.Data[1] = batch.Int128From(-100)
	view := batch.NewViewVector(base, []uint32{1, 0})

	var buf bytes.Buffer
	sw := newShuffleWriter(&buf, schema)
	if err := sw.writeHeader(); err != nil {
		t.Fatal(err)
	}
	if err := sw.writeChunk([]*batch.Vector{view}, nil, 2); err != nil {
		t.Fatalf("a view over a base at the header's scale must write: %v", err)
	}
}
