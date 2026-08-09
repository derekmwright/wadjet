package worker

import "golang.org/x/sys/unix"

// madvisePopulateRead batch-faults a page-aligned mmap range via
// MADV_POPULATE_READ (kernel >= 5.14). Errors (older kernel, non-mmap
// backing memory) make the caller fall back to its byte-per-page walk.
func madvisePopulateRead(b []byte) error {
	return unix.Madvise(b, unix.MADV_POPULATE_READ)
}
