package azure

import (
	"testing"

	"github.com/zoobzio/grub"
)

func TestProvider_New(t *testing.T) {
	// Azure client requires connection string, so we test with nil for unit tests
	provider := New(nil, "test-container", "prefix/")

	if provider == nil {
		t.Fatal("expected non-nil provider")
	}

	if provider.container != "test-container" {
		t.Errorf("container: got %s, want test-container", provider.container)
	}

	if provider.prefix != "prefix/" {
		t.Errorf("prefix: got %s, want prefix/", provider.prefix)
	}
}

func TestProvider_prefixKey(t *testing.T) {
	provider := New(nil, "container", "prefix/")

	result := provider.prefixKey("key1")
	if result != "prefix/key1" {
		t.Errorf("got %s, want prefix/key1", result)
	}
}

func TestProvider_stripPrefix(t *testing.T) {
	provider := New(nil, "container", "prefix/")

	result := provider.stripPrefix("prefix/key1")
	if result != "key1" {
		t.Errorf("got %s, want key1", result)
	}

	// Short key
	result = provider.stripPrefix("x")
	if result != "x" {
		t.Errorf("got %s, want x", result)
	}
}

func TestInt32Ptr(t *testing.T) {
	val := int32(42)
	ptr := int32Ptr(val)

	if ptr == nil {
		t.Fatal("expected non-nil pointer")
	}
	if *ptr != 42 {
		t.Errorf("got %d, want 42", *ptr)
	}
}

func TestProvider_ImplementsProvider(t *testing.T) {
	var _ grub.Provider = (*Provider)(nil)
}
