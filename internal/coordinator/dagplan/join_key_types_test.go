// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestKeyTypeUnresolvedSentinelsAgree holds the two spellings of "no
// widening" together. distribution.go keeps its own copy so the distribution
// algebra does not import the execution package for one constant, and the two
// disagreeing would make an exchange look interchangeable with one that
// hashes differently.
func TestKeyTypeUnresolvedSentinelsAgree(t *testing.T) {
	if keyTypeUnresolved != exec.KeyTypeUnresolved {
		t.Fatalf("keyTypeUnresolved = %v, exec.KeyTypeUnresolved = %v",
			keyTypeUnresolved, exec.KeyTypeUnresolved)
	}
	// And neither may collide with a real type.
	for i := 0; i <= int(parquet.TypeVector); i++ {
		if parquet.TypeID(i) == keyTypeUnresolved {
			t.Fatalf("the unresolved sentinel collides with %v", parquet.TypeID(i))
		}
	}
}
