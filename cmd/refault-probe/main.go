// Command refault-probe is a field diagnostic for the §9 page-cache
// pressure sensor (internal/engine/memory/pressure_os.go). It prints,
// once per second, the raw workingset_refault counter this process can
// see next to the sensor's sampled rate and active state — same
// process, same instant, so a moving counter with a flat sensor rate
// indicts the sensor, not the environment.
//
// With --thrash <file> it also generates workingset refaults itself:
// the file is mmap'd and pages are touched in a random order, twice per
// sweep, so pages join the kernel's workingset before eviction — run it
// inside a memory-capped cgroup smaller than the file and the
// eviction/re-touch cycle registers as refaults (a sequential cat loop
// does not: streamed pages never activate).
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"syscall"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/memory"
)

func main() {
	thrash := flag.String("thrash", "", "file to mmap and randomly re-touch to generate workingset refaults")
	secs := flag.Int("secs", 30, "seconds to run")
	flag.Parse()

	if *thrash != "" {
		go thrashLoop(*thrash)
	}

	for i := 0; i < *secs; i++ {
		active := memory.PageCachePressureActive()
		rate, act := memory.PageCachePressureStats()
		raw, ok := memory.ReadRefaultCounterForDiagnostics()
		fmt.Printf("t=%02d raw=%d raw_ok=%v active=%v rate=%.0f activations=%d\n",
			i, raw, ok, active, rate, act)
		time.Sleep(1 * time.Second)
	}
}

func thrashLoop(path string) {
	f, err := os.Open(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "thrash open:", err)
		return
	}
	st, err := f.Stat()
	if err != nil || st.Size() == 0 {
		fmt.Fprintln(os.Stderr, "thrash stat:", err)
		return
	}
	data, err := syscall.Mmap(int(f.Fd()), 0, int(st.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		fmt.Fprintln(os.Stderr, "thrash mmap:", err)
		return
	}
	const page = 4096
	pages := len(data) / page
	order := rand.Perm(pages)
	var sink byte
	for {
		// Two touches per sweep in random order: the first faults the
		// page in, the second (same sweep, different position) marks it
		// referenced so eviction comes off the active list — the
		// workingset shape the refault counter detects.
		for _, p := range order {
			sink += data[p*page]
		}
		for _, p := range order {
			sink += data[(pages-1-p)*page]
		}
	}
}
