//go:build linux

package diskio

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

// syscallRecorder replaces the syscall seams and records every call.
// restore() must be deferred. Tests in this package cannot run in
// parallel: they toggle the package-level enabled flag and seams.
type syscallRecorder struct {
	calls   []string
	syncErr error
}

func record(t *testing.T) *syscallRecorder {
	t.Helper()
	rec := &syscallRecorder{}
	origSync, origFadvise := syncFileRange, fadvise
	origEnabled := Enabled()
	syncFileRange = func(fd int, off, n int64, flags int) error {
		if rec.syncErr != nil {
			return rec.syncErr
		}
		var kind string
		switch flags {
		case unix.SYNC_FILE_RANGE_WRITE:
			kind = "write"
		case unix.SYNC_FILE_RANGE_WAIT_BEFORE | unix.SYNC_FILE_RANGE_WRITE | unix.SYNC_FILE_RANGE_WAIT_AFTER:
			kind = "wait"
		default:
			kind = fmt.Sprintf("flags=%d", flags)
		}
		rec.calls = append(rec.calls, fmt.Sprintf("%s[%d,%d)", kind, off, off+n))
		return nil
	}
	fadvise = func(fd int, off, n int64, advice int) error {
		if advice != unix.FADV_DONTNEED {
			t.Fatalf("unexpected fadvise advice %d", advice)
		}
		rec.calls = append(rec.calls, fmt.Sprintf("dontneed[%d,%d)", off, off+n))
		return nil
	}
	t.Cleanup(func() {
		syncFileRange, fadvise = origSync, origFadvise
		SetEnabled(origEnabled)
	})
	return rec
}

func tempFile(t *testing.T) *os.File {
	t.Helper()
	f, err := os.Create(filepath.Join(t.TempDir(), "f.bin"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return f
}

func TestDisabledPassthrough(t *testing.T) {
	rec := record(t)
	SetEnabled(false)
	f := tempFile(t)
	w, fl := NewWriter(f, Spill)
	if w != io.Writer(f) {
		t.Fatalf("disabled NewWriter should return the file itself, got %T", w)
	}
	if fl != nil {
		t.Fatalf("disabled NewWriter should return nil Flusher")
	}
	fl.Finish() // nil-safe
	if len(rec.calls) != 0 {
		t.Fatalf("disabled mode made syscalls: %v", rec.calls)
	}
}

func TestSpillWindowedWriteback(t *testing.T) {
	rec := record(t)
	SetEnabled(true)
	f := tempFile(t)
	w, fl := NewWriter(f, Spill)
	if fl == nil {
		t.Fatal("expected a live Flusher")
	}

	const win = windowBytes
	// Write 2.5 windows in odd-sized chunks to exercise the loop.
	total := int64(2.5 * float64(win))
	chunk := make([]byte, 1<<20+12345)
	var written int64
	for written < total {
		n := int64(len(chunk))
		if written+n > total {
			n = total - written
		}
		if _, err := w.Write(chunk[:n]); err != nil {
			t.Fatal(err)
		}
		written += n
	}
	fl.Finish()

	want := []string{
		// window 0 completes: async write only (no predecessor)
		fmt.Sprintf("write[0,%d)", win),
		// window 1 completes: async write, then wait+drop window 0
		fmt.Sprintf("write[%d,%d)", win, 2*win),
		fmt.Sprintf("wait[0,%d)", win),
		fmt.Sprintf("dontneed[0,%d)", win),
		// Finish: whole-file wait + drop (offset 0, nbytes 0 = to EOF)
		"wait[0,0)",
		"dontneed[0,0)",
	}
	if len(rec.calls) != len(want) {
		t.Fatalf("calls = %v\nwant   %v", rec.calls, want)
	}
	for i := range want {
		if rec.calls[i] != want[i] {
			t.Fatalf("call %d = %q, want %q (all: %v)", i, rec.calls[i], want[i], rec.calls)
		}
	}
}

func TestKeepResidentNeverDrops(t *testing.T) {
	rec := record(t)
	SetEnabled(true)
	f := tempFile(t)
	w, fl := NewWriter(f, KeepResident)

	buf := make([]byte, windowBytes)
	for i := 0; i < 3; i++ {
		if _, err := w.Write(buf); err != nil {
			t.Fatal(err)
		}
	}
	fl.Finish()

	for _, c := range rec.calls {
		if len(c) >= 8 && c[:8] == "dontneed" {
			t.Fatalf("KeepResident issued fadvise: %v", rec.calls)
		}
	}
	// Dirty bounding must still happen: 3 async writes + 2 waits.
	var writes, waits int
	for _, c := range rec.calls {
		switch c[:4] {
		case "writ":
			writes++
		case "wait":
			waits++
		}
	}
	if writes != 3 || waits != 2 {
		t.Fatalf("got %d writes / %d waits, want 3/2: %v", writes, waits, rec.calls)
	}
}

func TestSyscallErrorDisables(t *testing.T) {
	rec := record(t)
	SetEnabled(true)
	f := tempFile(t)
	w, fl := NewWriter(f, Spill)

	rec.syncErr = errors.New("EOPNOTSUPP")
	buf := make([]byte, windowBytes)
	if _, err := w.Write(buf); err != nil {
		t.Fatal(err)
	}
	rec.syncErr = nil
	// Further writes and Finish must stay silent.
	if _, err := w.Write(buf); err != nil {
		t.Fatal(err)
	}
	fl.Finish()
	if len(rec.calls) != 0 {
		t.Fatalf("flusher kept issuing syscalls after error: %v", rec.calls)
	}
	// Content is unaffected by the advisory failure.
	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != 2*windowBytes {
		t.Fatalf("file size %d, want %d", fi.Size(), 2*windowBytes)
	}
}

// TestRealSyscallsRoundTrip runs the real sync_file_range/fadvise calls
// against a temp file and verifies the written content is byte-identical
// — the machinery must never affect file contents.
func TestRealSyscallsRoundTrip(t *testing.T) {
	origEnabled := Enabled()
	SetEnabled(true)
	t.Cleanup(func() { SetEnabled(origEnabled) })

	for _, class := range []Class{Spill, KeepResident} {
		f := tempFile(t)
		w, fl := NewWriter(f, class)

		data := make([]byte, 2*windowBytes+123457)
		if _, err := rand.Read(data); err != nil {
			t.Fatal(err)
		}
		// Odd-sized writes so window boundaries land mid-write.
		for off := 0; off < len(data); {
			n := 1<<20 + 7777
			if off+n > len(data) {
				n = len(data) - off
			}
			if _, err := w.Write(data[off : off+n]); err != nil {
				t.Fatal(err)
			}
			off += n
		}
		fl.Finish()

		got, err := os.ReadFile(f.Name())
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("class %d: file content differs after windowed writeback", class)
		}
	}
}
