package harness

import "testing"

func TestCheckFreeRAMDetectsExcessiveRequest(t *testing.T) {
	err := checkFreeRAM(1 << 50) // 1 PB
	if err == nil {
		t.Error("expected failure for 1PB request")
	}
}

func TestCheckFreeRAMTinyRequestSucceeds(t *testing.T) {
	err := checkFreeRAM(1 * int64(MB))
	if err != nil {
		t.Errorf("1 MB request failed: %v", err)
	}
}

func TestCheckFreeDiskTinyRequestSucceeds(t *testing.T) {
	dir := t.TempDir() + "/foo/bar"
	err := checkFreeDisk(dir, 1*int64(MB))
	if err != nil {
		t.Errorf("1 MB disk request failed: %v", err)
	}
}
