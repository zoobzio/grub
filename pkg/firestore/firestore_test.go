package firestore

import (
	"testing"

	"github.com/zoobzio/grub"
)

func TestProvider_New(t *testing.T) {
	// Firestore client requires project/credentials, so we test with nil for unit tests
	provider := New(nil, "test-collection")

	if provider == nil {
		t.Fatal("expected non-nil provider")
	}

	if provider.collection != "test-collection" {
		t.Errorf("collection: got %s, want test-collection", provider.collection)
	}
}

func TestProvider_ImplementsProvider(t *testing.T) {
	var _ grub.Provider = (*Provider)(nil)
}

func TestDocument_Structure(t *testing.T) {
	doc := document{
		Data: []byte("test-data"),
	}

	if string(doc.Data) != "test-data" {
		t.Errorf("Data: got %s, want test-data", doc.Data)
	}
}
