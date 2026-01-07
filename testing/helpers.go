// Package testing provides shared test utilities for grub.
package testing

import (
	"context"
	"testing"
	"time"
)

// WithTimeout returns a context with the given timeout, cancelling on test cleanup.
func WithTimeout(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// AssertNoError fails the test if err is not nil.
func AssertNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// AssertError fails the test if err is nil.
func AssertError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// AssertEqual fails the test if got != want.
func AssertEqual[T comparable](t *testing.T, got, want T) {
	t.Helper()
	if got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

// AssertTrue fails the test if condition is false.
func AssertTrue(t *testing.T, condition bool, msg string) {
	t.Helper()
	if !condition {
		t.Errorf("expected true: %s", msg)
	}
}

// AssertFalse fails the test if condition is true.
func AssertFalse(t *testing.T, condition bool, msg string) {
	t.Helper()
	if condition {
		t.Errorf("expected false: %s", msg)
	}
}

// AssertNil fails the test if v is not nil.
func AssertNil(t *testing.T, v any) {
	t.Helper()
	if v != nil {
		t.Errorf("expected nil, got %v", v)
	}
}

// AssertNotNil fails the test if v is nil.
func AssertNotNil(t *testing.T, v any) {
	t.Helper()
	if v == nil {
		t.Error("expected non-nil value")
	}
}

// AssertLen fails the test if len(slice) != want.
func AssertLen[T any](t *testing.T, slice []T, want int) {
	t.Helper()
	if len(slice) != want {
		t.Errorf("expected length %d, got %d", want, len(slice))
	}
}

// AssertContains fails the test if slice does not contain item.
func AssertContains[T comparable](t *testing.T, slice []T, item T) {
	t.Helper()
	for _, v := range slice {
		if v == item {
			return
		}
	}
	t.Errorf("slice does not contain %v", item)
}

// AssertMapHasKey fails the test if m does not have key k.
func AssertMapHasKey[K comparable, V any](t *testing.T, m map[K]V, k K) {
	t.Helper()
	if _, ok := m[k]; !ok {
		t.Errorf("map does not contain key %v", k)
	}
}
