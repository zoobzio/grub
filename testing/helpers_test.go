package testing

import (
	"errors"
	"testing"
	"time"
)

func TestWithTimeout(t *testing.T) {
	ctx := WithTimeout(t, 5*time.Second)
	if ctx == nil {
		t.Fatal("expected non-nil context")
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("expected context to have deadline")
	}
	if deadline.Before(time.Now()) {
		t.Error("deadline should be in the future")
	}
}

func TestAssertNoError(t *testing.T) {
	// Should not panic with nil error
	AssertNoError(t, nil)
}

func TestAssertError(t *testing.T) {
	// Should not panic with non-nil error
	AssertError(t, errors.New("test error"))
}

func TestAssertEqual(t *testing.T) {
	AssertEqual(t, 42, 42)
	AssertEqual(t, "hello", "hello")
	AssertEqual(t, true, true)
}

func TestAssertTrue(t *testing.T) {
	AssertTrue(t, true, "should be true")
}

func TestAssertFalse(t *testing.T) {
	AssertFalse(t, false, "should be false")
}

func TestAssertNil(t *testing.T) {
	AssertNil(t, nil)
}

func TestAssertNotNil(t *testing.T) {
	AssertNotNil(t, "not nil")
	AssertNotNil(t, 42)
	AssertNotNil(t, []int{1, 2, 3})
}

func TestAssertLen(t *testing.T) {
	AssertLen(t, []int{1, 2, 3}, 3)
	AssertLen(t, []string{}, 0)
	AssertLen(t, []string{"a", "b"}, 2)
}

func TestAssertContains(t *testing.T) {
	AssertContains(t, []int{1, 2, 3}, 2)
	AssertContains(t, []string{"a", "b", "c"}, "b")
}

func TestAssertMapHasKey(t *testing.T) {
	m := map[string]int{"a": 1, "b": 2}
	AssertMapHasKey(t, m, "a")
	AssertMapHasKey(t, m, "b")
}
