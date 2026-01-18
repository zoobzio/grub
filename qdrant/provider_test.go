package qdrant

import (
	"testing"
)

func TestNew(t *testing.T) {
	p := New(nil, Config{Collection: "test"})
	if p == nil {
		t.Fatal("New returned nil")
	}
	if p.config.Collection != "test" {
		t.Errorf("expected collection 'test', got %q", p.config.Collection)
	}
}
