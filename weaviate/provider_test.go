package weaviate

import (
	"testing"
)

func TestNew(t *testing.T) {
	p := New(nil, Config{Class: "TestClass"})
	if p == nil {
		t.Fatal("New returned nil")
	}
	if p.config.Class != "TestClass" {
		t.Errorf("expected class 'TestClass', got %q", p.config.Class)
	}
}
