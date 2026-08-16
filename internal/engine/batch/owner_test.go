package batch

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func ownerTestSchema() []parquet.Column {
	return []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}
}

func TestBatchPool_StampsReservoirOwner(t *testing.T) {
	p := NewBatchPool(ownerTestSchema(), DefaultBatchSize)

	// Fresh mint via Get.
	b := p.Get()
	if b.ownerID != ReservoirOwner {
		t.Fatalf("Get fresh mint ownerID = %d, want %d", b.ownerID, ReservoirOwner)
	}

	// Reuse path: return and re-Get; stamp must survive Reset.
	b.Release()
	b2 := p.Get()
	if b2.ownerID != ReservoirOwner {
		t.Fatalf("reused batch lost ownerID = %d", b2.ownerID)
	}
}

func TestBatchPool_PreWarmAndGetForSizeStamp(t *testing.T) {
	p := NewBatchPool(ownerTestSchema(), DefaultBatchSize)

	p.PreWarm(2)
	b := p.Get() // drawn from prewarmed
	if b.ownerID != ReservoirOwner {
		t.Fatalf("PreWarm'd batch ownerID = %d", b.ownerID)
	}

	in := p.GetForSize(10)
	if in.ownerID != ReservoirOwner {
		t.Fatalf("GetForSize in-size ownerID = %d", in.ownerID)
	}

	// Over-size escape: unpooled, must stay unstamped (0).
	over := p.GetForSize(DefaultBatchSize + 1)
	if over.ownerID != 0 {
		t.Fatalf("over-size batch should be unstamped, got %d", over.ownerID)
	}
}

func TestBatchPool_GetForSizeFreshAllocStamp(t *testing.T) {
	// Empty pool, in-size request → fresh-alloc branch must stamp.
	p := NewBatchPool(ownerTestSchema(), DefaultBatchSize)
	b := p.GetForSize(10)
	if b.ownerID != ReservoirOwner {
		t.Fatalf("GetForSize fresh-alloc ownerID = %d, want %d", b.ownerID, ReservoirOwner)
	}
}
