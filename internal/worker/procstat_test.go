package worker

import "testing"

// The procfs readers feed advisory marker lines; the contract under
// test is "sane values on linux, never panic" — exact numbers belong
// to the kernel.
func TestProcSelfFaults(t *testing.T) {
	minflt, majflt := procSelfFaults()
	if minflt <= 0 {
		t.Errorf("minflt = %d, want > 0 (every process minor-faults at startup)", minflt)
	}
	if majflt < 0 {
		t.Errorf("majflt = %d, want >= 0", majflt)
	}
}

func TestProcSelfIO(t *testing.T) {
	readBytes, writeBytes := procSelfIO()
	if readBytes < 0 || writeBytes < 0 {
		t.Errorf("proc io = (%d, %d), want both >= 0", readBytes, writeBytes)
	}
}

func TestNvmeDiskstats(t *testing.T) {
	readBytes, writeBytes := nvmeDiskstats()
	if readBytes < 0 || writeBytes < 0 {
		t.Errorf("nvme diskstats = (%d, %d), want both >= 0", readBytes, writeBytes)
	}
}

func TestProcSelfCPU(t *testing.T) {
	utime, stime := procSelfCPU()
	if utime <= 0 && stime <= 0 {
		t.Errorf("proc cpu = (%d, %d), want at least one > 0 (the test binary has burned CPU)", utime, stime)
	}
}

func TestHostCPUTimes(t *testing.T) {
	busy, idle, iowait, steal := hostCPUTimes()
	if busy <= 0 {
		t.Errorf("host busy = %d, want > 0", busy)
	}
	if idle < 0 || iowait < 0 || steal < 0 {
		t.Errorf("host cpu = idle %d iowait %d steal %d, want all >= 0", idle, iowait, steal)
	}
}

func TestParsePSITotals(t *testing.T) {
	tests := []struct {
		name     string
		data     string
		wantSome int64
		wantFull int64
	}{
		{
			name: "memory style with some and full",
			data: "some avg10=1.23 avg60=0.50 avg300=0.10 total=4567890\n" +
				"full avg10=0.00 avg60=0.00 avg300=0.00 total=123456\n",
			wantSome: 4567890,
			wantFull: 123456,
		},
		{
			name:     "cpu style some only",
			data:     "some avg10=0.00 avg60=0.00 avg300=0.00 total=98765\n",
			wantSome: 98765,
			wantFull: 0,
		},
		{name: "empty", data: "", wantSome: 0, wantFull: 0},
		{name: "garbage", data: "not a psi file\n", wantSome: 0, wantFull: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			some, full := parsePSITotals(tt.data)
			if some != tt.wantSome || full != tt.wantFull {
				t.Errorf("parsePSITotals = (%d, %d), want (%d, %d)", some, full, tt.wantSome, tt.wantFull)
			}
		})
	}
}

func TestPSITotals(t *testing.T) {
	cpuSome, memSome, memFull, ioSome, ioFull := psiTotals()
	for _, v := range []int64{cpuSome, memSome, memFull, ioSome, ioFull} {
		if v < 0 {
			t.Errorf("psi totals = (%d, %d, %d, %d, %d), want all >= 0", cpuSome, memSome, memFull, ioSome, ioFull)
		}
	}
}

func TestSchedWaitTotalMs(t *testing.T) {
	if v := schedWaitTotalMs(); v < 0 {
		t.Errorf("sched wait = %d, want >= 0", v)
	}
}
