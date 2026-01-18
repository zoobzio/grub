package milvus

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

func TestConfig_Defaults(t *testing.T) {
	p := New(nil, Config{Collection: "test"})

	if p.config.IDField != "id" {
		t.Errorf("expected IDField default 'id', got %q", p.config.IDField)
	}
	if p.config.VectorField != "embedding" {
		t.Errorf("expected VectorField default 'embedding', got %q", p.config.VectorField)
	}
	if p.config.MetadataField != "metadata" {
		t.Errorf("expected MetadataField default 'metadata', got %q", p.config.MetadataField)
	}
}
