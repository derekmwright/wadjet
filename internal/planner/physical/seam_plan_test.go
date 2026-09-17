// SPDX-License-Identifier: MIT

package physical

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func TestDeferredJoinBridge(t *testing.T) {
	// Verify that physical.DeferredJoinBridge collects child pipeline batches
	// and waits for the build barrier before returning.
	ctx := context.Background()

	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "val", Type: parquet.TypeString},
	}
	rows := []map[string]any{
		{"id": int64(1), "val": "a"},
		{"id": int64(2), "val": "b"},
		{"id": int64(3), "val": "c"},
	}
	source := exec.NewSliceSource(schema, rows)
	barrier := make(chan struct{})
	var buildErr error

	bridge := &DeferredJoinBridge{
		ChildSource: source,
		ChildOps:    nil,
		Barrier:     barrier,
		BuildErr:    &buildErr,
		Workers:     1,
	}

	// Simulate build completing
	close(barrier)

	if err := bridge.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}

	// Should be able to read all batches
	count := 0
	for {
		b, err := bridge.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if b == nil {
			break
		}
		count += b.Len
	}
	if count != 3 {
		t.Errorf("expected 3 rows, got %d", count)
	}
}

func TestDeferredJoinBridgeBuildError(t *testing.T) {
	ctx := context.Background()
	source := exec.NewSliceSource(nil, nil)
	barrier := make(chan struct{})
	buildErr := fmt.Errorf("build failed: out of memory")

	bridge := &DeferredJoinBridge{
		ChildSource: source,
		ChildOps:    nil,
		Barrier:     barrier,
		BuildErr:    &buildErr,
		Workers:     1,
	}

	close(barrier)

	err := bridge.Init(ctx)
	if err == nil {
		t.Fatal("expected error from build failure")
	}
	if err.Error() != "build failed: out of memory" {
		t.Errorf("unexpected error: %v", err)
	}
}
