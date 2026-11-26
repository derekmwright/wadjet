// SPDX-License-Identifier: MIT

package exec

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/memory"
)

// #1706, line 1706o: a spilled partition's build writer (its file, its
// descriptor and its buffer) is finished when the build ends, not at the first
// NextFlush. Before, every build writer stayed open through the whole probe
// beside every probe writer: at a 2 MiB budget the census measured 63 build
// and 63 probe writers of 64 KiB open at once (8,257,536 bytes held, none of
// it charged).
func TestHashJoinBuildWritersCloseAtBuildEnd(t *testing.T) {
	lcFixtures()
	for _, jt := range []JoinType{InnerJoin, LeftJoin, FullOuterJoin, RightSemiJoin} {
		t.Run(fmt.Sprint(jt), func(t *testing.T) {
			tracker := memory.NewTracker("lc", 2<<20)
			sm, err := memory.NewSpillManager(t.TempDir(), tracker)
			if err != nil {
				t.Fatal(err)
			}
			hj := NewHashJoin(jt, []string{"pk"}, []string{"bk"})
			hj.MemTracker, hj.Spill = tracker, sm
			defer hj.Close()
			ctx := context.Background()
			if err := hj.Build(ctx, &lcSource{batches: lcBig.build}); err != nil {
				t.Fatal(err)
			}
			ss := hj.spillState
			if ss == nil || len(ss.spilledParts) == 0 {
				t.Fatal("the build spilled nothing: this test never reaches a build writer")
			}
			if n := len(ss.partBuildWriters); n != 0 || ss.partBuildWriters != nil {
				t.Errorf("after Build, %d build writers are still open", n)
			}
			if fds := buildSpillFDs(sm.SpillDir()); len(fds) != 0 {
				t.Errorf("after Build, %d build-run descriptors are still open: %v", len(fds), fds)
			}
			runs := 0
			for _, files := range ss.partBuildFiles {
				runs += len(files)
			}
			if runs != len(ss.spilledParts) {
				t.Errorf("after Build, %d build runs are complete for %d spilled partitions", runs, len(ss.spilledParts))
			}

			// The probe opens its own writers; none of the build's may be
			// open beside them.
			p := hj.Probe()
			defer p.Close()
			p.EnableBoundedOutput()
			if err := p.Init(ctx); err != nil {
				t.Fatal(err)
			}
			if err := lcProbe(ctx, p, lcBig.probe, nil); err != nil {
				t.Fatal(err)
			}
			if len(ss.partProbeWriters) == 0 {
				t.Fatal("the probe opened no probe writer: this test never sees both kinds")
			}
			if fds := buildSpillFDs(sm.SpillDir()); len(fds) != 0 {
				t.Errorf("during the probe, %d build-run descriptors are open beside %d probe writers: %v",
					len(fds), len(ss.partProbeWriters), fds)
			}
			if _, err := lcFlush(ctx, p, nil); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// buildSpillFDs lists this process's open descriptors on build runs in dir.
func buildSpillFDs(dir string) []string {
	var out []string
	for _, fd := range lcOpenFDs(dir) {
		if strings.HasPrefix(filepath.Base(fd), "build-spill-") {
			out = append(out, fd)
		}
	}
	return out
}
