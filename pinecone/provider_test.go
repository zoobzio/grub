package pinecone

import (
	"testing"
)

func TestNew(t *testing.T) {
	p := New(nil, Config{Namespace: "test"})
	if p == nil {
		t.Fatal("New returned nil")
	}
	if p.config.Namespace != "test" {
		t.Errorf("expected namespace 'test', got %q", p.config.Namespace)
	}
}
