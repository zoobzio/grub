package grub

import (
	"testing"
)

type testRecord struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func TestJSONCodec_Marshal(t *testing.T) {
	codec := JSONCodec{}

	record := testRecord{ID: "123", Name: "Test", Age: 30}
	data, err := codec.Marshal(record)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := `{"id":"123","name":"Test","age":30}`
	if string(data) != expected {
		t.Errorf("got %s, want %s", data, expected)
	}
}

func TestJSONCodec_Marshal_Nil(t *testing.T) {
	codec := JSONCodec{}

	data, err := codec.Marshal(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if string(data) != "null" {
		t.Errorf("got %s, want null", data)
	}
}

func TestJSONCodec_Marshal_InvalidValue(t *testing.T) {
	codec := JSONCodec{}

	// Channels cannot be marshaled to JSON
	ch := make(chan int)
	_, err := codec.Marshal(ch)
	if err == nil {
		t.Error("expected error for invalid value")
	}
}

func TestJSONCodec_Unmarshal(t *testing.T) {
	codec := JSONCodec{}

	data := []byte(`{"id":"456","name":"User","age":25}`)
	var result testRecord
	err := codec.Unmarshal(data, &result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.ID != "456" {
		t.Errorf("ID: got %s, want 456", result.ID)
	}
	if result.Name != "User" {
		t.Errorf("Name: got %s, want User", result.Name)
	}
	if result.Age != 25 {
		t.Errorf("Age: got %d, want 25", result.Age)
	}
}

func TestJSONCodec_Unmarshal_InvalidJSON(t *testing.T) {
	codec := JSONCodec{}

	data := []byte(`{invalid json}`)
	var result testRecord
	err := codec.Unmarshal(data, &result)
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestJSONCodec_Unmarshal_TypeMismatch(t *testing.T) {
	codec := JSONCodec{}

	// Age is string instead of int
	data := []byte(`{"id":"123","name":"Test","age":"not a number"}`)
	var result testRecord
	err := codec.Unmarshal(data, &result)
	if err == nil {
		t.Error("expected error for type mismatch")
	}
}

func TestJSONCodec_ContentType(t *testing.T) {
	codec := JSONCodec{}

	ct := codec.ContentType()
	if ct != "application/json" {
		t.Errorf("got %s, want application/json", ct)
	}
}

func TestJSONCodec_RoundTrip(t *testing.T) {
	codec := JSONCodec{}

	original := testRecord{ID: "round", Name: "Trip", Age: 42}

	data, err := codec.Marshal(original)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	var decoded testRecord
	err = codec.Unmarshal(data, &decoded)
	if err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	if decoded != original {
		t.Errorf("round trip failed: got %+v, want %+v", decoded, original)
	}
}

func TestJSONCodec_ImplementsCodec(_ *testing.T) {
	var _ Codec = JSONCodec{}
}
