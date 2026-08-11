package worker

import (
	"os"

	"golang.org/x/sys/unix"
)

// fadviseSequential is adviseSequential for the pread scan path: doubled
// kernel readahead on the fd, pages behind the read preferred for
// reclaim.
func fadviseSequential(f *os.File, size int64) {
	if f == nil || size <= 0 {
		return
	}
	_ = unix.Fadvise(int(f.Fd()), 0, size, unix.FADV_SEQUENTIAL)
}

// fdWillNeedAdviser is willNeedAdviser for the pread scan path: the
// decode-ahead iterator announces upcoming projected column-chunk
// ranges, and POSIX_FADV_WILLNEED starts asynchronous kernel readahead
// so the later staging pread copies from page cache instead of blocking
// on NVMe under a held CPU token. Advisory — errors are ignored.
func fdWillNeedAdviser(f *os.File) func(off, n int64) {
	fd := int(f.Fd())
	return func(off, n int64) {
		if off < 0 || n <= 0 {
			return
		}
		if unix.Fadvise(fd, off, n, unix.FADV_WILLNEED) == nil {
			readaheadAdviseBytes.Add(n)
		}
	}
}
