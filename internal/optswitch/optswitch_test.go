package optswitch

import "testing"

func TestRegisterDefaultsAndSet(t *testing.T) {
	t.Setenv("WADJET_TEST_TOGGLE_OFF", "0")
	t.Setenv("WADJET_TEST_TOGGLE_ON", "")

	off := Register("test-off", "WADJET_TEST_TOGGLE_OFF", "env-disabled toggle")
	on := Register("test-on", "WADJET_TEST_TOGGLE_ON", "default-on toggle")

	if off.On() {
		t.Error("env=0 toggle should start disabled")
	}
	if !on.On() {
		t.Error("unset-env toggle should start enabled")
	}

	if prev := on.Set(false); !prev {
		t.Error("Set should return previous value true")
	}
	if on.On() {
		t.Error("Set(false) did not stick")
	}
	on.Set(true)

	found := 0
	for _, tg := range All() {
		if tg.Name == "test-off" || tg.Name == "test-on" {
			found++
		}
	}
	if found != 2 {
		t.Errorf("All() missing registered toggles, found %d/2", found)
	}
}

func TestDuplicateRegisterPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("duplicate Register did not panic")
		}
	}()
	Register("test-dup", "WADJET_TEST_DUP", "first")
	Register("test-dup", "WADJET_TEST_DUP", "second")
}
