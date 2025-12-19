package grub

import (
	"context"
	"testing"
)

// customCodec is a test codec implementation.
type customCodec struct {
	contentType string
}

func (c customCodec) Marshal(v any) ([]byte, error) {
	return JSONCodec{}.Marshal(v)
}

func (c customCodec) Unmarshal(data []byte, v any) error {
	return JSONCodec{}.Unmarshal(data, v)
}

func (c customCodec) ContentType() string {
	return c.contentType
}

func TestWithCodec(t *testing.T) {
	provider := newMockProvider()
	custom := customCodec{contentType: "application/custom"}

	svc := New[testUser](provider, WithCodec[testUser](custom))

	// Verify custom codec is used
	if c, ok := svc.codec.(customCodec); !ok {
		t.Error("expected customCodec")
	} else if c.ContentType() != "application/custom" {
		t.Errorf("content type: got %s, want application/custom", c.ContentType())
	}
}

func TestWithCodec_NilCodec(t *testing.T) {
	provider := newMockProvider()

	// Pass nil codec - should fall back to JSONCodec
	svc := New[testUser](provider, WithCodec[testUser](nil))

	if _, ok := svc.codec.(JSONCodec); !ok {
		t.Error("expected JSONCodec fallback for nil codec")
	}
}

func TestWithCodec_Functional(t *testing.T) {
	provider := newMockProvider()
	custom := customCodec{contentType: "application/xml"}

	svc := New[testUser](provider, WithCodec[testUser](custom))

	ctx := context.Background()
	user := testUser{ID: "opt", Email: "options@test.com"}

	// Set and Get should work with custom codec
	err := svc.Set(ctx, "optkey", user)
	if err != nil {
		t.Fatalf("Set error: %v", err)
	}

	result, err := svc.Get(ctx, "optkey")
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}

	if *result != user {
		t.Errorf("got %+v, want %+v", result, user)
	}
}

func TestMultipleOptions(t *testing.T) {
	provider := newMockProvider()
	codec1 := customCodec{contentType: "first"}
	codec2 := customCodec{contentType: "second"}

	// Last option wins
	svc := New[testUser](provider,
		WithCodec[testUser](codec1),
		WithCodec[testUser](codec2),
	)

	if c, ok := svc.codec.(customCodec); !ok {
		t.Error("expected customCodec")
	} else if c.ContentType() != "second" {
		t.Errorf("expected second codec to win, got %s", c.ContentType())
	}
}
