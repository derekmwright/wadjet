package sqlerr

import (
	"errors"
	"fmt"
	"testing"
)

func TestStateOfFindsCodeThroughWrapping(t *testing.T) {
	base := New("42P01", "relation %q does not exist", "nope")
	if base.Error() != `relation "nope" does not exist` {
		t.Fatalf("message = %q", base.Error())
	}
	wrapped := fmt.Errorf("planning: %w", fmt.Errorf("validating: %w", base))
	if got := StateOf(wrapped); got != "42P01" {
		t.Fatalf("StateOf(wrapped) = %q, want 42P01", got)
	}
	if got := StateOf(errors.New("plain")); got != "" {
		t.Fatalf("StateOf(plain) = %q, want empty", got)
	}
	if got := StateOf(nil); got != "" {
		t.Fatalf("StateOf(nil) = %q, want empty", got)
	}
}

type coderErr struct{}

func (coderErr) Error() string    { return "boom" }
func (coderErr) SQLState() string { return "42883" }

func TestStateOfHonorsForeignCoders(t *testing.T) {
	err := fmt.Errorf("compile: %w", coderErr{})
	if got := StateOf(err); got != "42883" {
		t.Fatalf("StateOf(coder) = %q, want 42883", got)
	}
}

func TestWrapKeepsChainAndAttachesCode(t *testing.T) {
	sentinel := errors.New("sentinel")
	err := Wrap("42601", fmt.Errorf("parsing: %w", sentinel))
	if !errors.Is(err, sentinel) {
		t.Fatal("Wrap broke the error chain")
	}
	if got := StateOf(err); got != "42601" {
		t.Fatalf("StateOf = %q, want 42601", got)
	}
	if err.Error() != "parsing: sentinel" {
		t.Fatalf("message = %q, want the wrapped error's own text", err.Error())
	}
	if Wrap("42601", nil) != nil {
		t.Fatal("Wrap(nil) must be nil")
	}
	// The OUTERMOST code wins when wraps stack.
	stacked := Wrap("22012", New("42P01", "inner"))
	if got := StateOf(stacked); got != "22012" {
		t.Fatalf("StateOf(stacked) = %q, want the outermost 22012", got)
	}
}
