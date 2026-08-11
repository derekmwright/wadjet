//go:build !linux

package worker

import "os"

// posix_fadvise is Linux-only here; elsewhere the pread path runs
// without readahead hints (the staging read itself still works).
func fadviseSequential(*os.File, int64) {}

func fdWillNeedAdviser(*os.File) func(off, n int64) { return nil }
