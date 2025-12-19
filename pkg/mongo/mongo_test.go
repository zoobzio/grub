package mongo

import (
	"testing"

	"github.com/zoobzio/grub"
)

func TestProvider_New(t *testing.T) {
	// MongoDB collection requires a client, so we test with nil for unit tests
	provider := New(nil)

	if provider == nil {
		t.Fatal("expected non-nil provider")
	}
}

func TestProvider_ImplementsProvider(t *testing.T) {
	var _ grub.Provider = (*Provider)(nil)
}

func TestDocument_Structure(t *testing.T) {
	doc := document{
		Key:  "test-key",
		Data: []byte("test-data"),
	}

	if doc.Key != "test-key" {
		t.Errorf("Key: got %s, want test-key", doc.Key)
	}
	if string(doc.Data) != "test-data" {
		t.Errorf("Data: got %s, want test-data", doc.Data)
	}
}
