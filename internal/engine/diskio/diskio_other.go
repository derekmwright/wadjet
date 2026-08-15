//go:build !linux

package diskio

import (
	"io"
	"os"
)

// sync_file_range is Linux-only; elsewhere the mechanism is a no-op and
// writers are returned unwrapped.

// Flusher is inert on non-Linux platforms.
type Flusher struct{}

// NewWriter returns f unchanged and a nil Flusher on non-Linux platforms.
func NewWriter(f *os.File, _ Class) (io.Writer, *Flusher) { return f, nil }

// Finish is a no-op on non-Linux platforms. nil-safe.
func (fl *Flusher) Finish() {}

// NewDropBehindReader returns f unchanged on non-Linux platforms.
func NewDropBehindReader(f *os.File) io.Reader { return f }

// DropBehindCursor is inert on non-Linux platforms.
type DropBehindCursor struct{}

// NewDropBehindCursor returns nil on non-Linux platforms; a nil cursor's
// Advance is a no-op.
func NewDropBehindCursor(_ *os.File) *DropBehindCursor { return nil }

// Advance is a no-op on non-Linux platforms. nil-safe.
func (c *DropBehindCursor) Advance(_ int64) {}
