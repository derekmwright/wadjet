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
