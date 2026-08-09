//go:build !linux

package worker

import "errors"

// madvisePopulateRead is Linux-only (MADV_POPULATE_READ); elsewhere the
// toucher always uses its byte-per-page walk.
func madvisePopulateRead([]byte) error {
	return errors.ErrUnsupported
}
