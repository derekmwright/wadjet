//go:build !linux

package memory

import "unsafe"

// Non-linux fallback: off-heap group-state arrays are a linux-only
// optimization; every constructor returns a plain heap slice.

// OffheapRegistry is inert off linux.
type OffheapRegistry struct{}

func NewOffheapRegistry() *OffheapRegistry            { return &OffheapRegistry{} }
func (r *OffheapRegistry) AdoptFrom(*OffheapRegistry) {}
func (r *OffheapRegistry) Close()                     {}
func (r *OffheapRegistry) Mappings() int              { return 0 }

// OffheapAvailable reports false off linux.
func OffheapAvailable() bool { return false }

// Offheap returns a heap slice off linux.
func Offheap[T any](_ *OffheapRegistry, heapCap int) []T {
	return make([]T, 0, heapCap)
}

// OffheapSized reports unavailable off linux (callers heap-allocate).
func OffheapSized[T any](_ *OffheapRegistry, _ int) ([]T, bool) { return nil, false }

// Release is inert off linux.
func (r *OffheapRegistry) Release(_ unsafe.Pointer) bool { return false }
