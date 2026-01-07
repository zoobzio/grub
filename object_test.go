package grub

import (
	"encoding/json"
	"testing"
)

func TestObject_ZeroValue(t *testing.T) {
	var obj Object[string]

	if obj.Key != "" {
		t.Errorf("expected empty Key, got %q", obj.Key)
	}
	if obj.ContentType != "" {
		t.Errorf("expected empty ContentType, got %q", obj.ContentType)
	}
	if obj.Size != 0 {
		t.Errorf("expected Size 0, got %d", obj.Size)
	}
	if obj.ETag != "" {
		t.Errorf("expected empty ETag, got %q", obj.ETag)
	}
	if obj.Metadata != nil {
		t.Errorf("expected nil Metadata, got %v", obj.Metadata)
	}
	if obj.Data != "" {
		t.Errorf("expected empty Data, got %q", obj.Data)
	}
}

func TestObject_WithStringPayload(t *testing.T) {
	obj := Object[string]{
		Key:         "test/file.txt",
		ContentType: "text/plain",
		Size:        100,
		ETag:        "etag123",
		Metadata:    map[string]string{"key": "value"},
		Data:        "hello world",
	}

	if obj.Key != "test/file.txt" {
		t.Errorf("unexpected Key: %q", obj.Key)
	}
	if obj.Data != "hello world" {
		t.Errorf("unexpected Data: %q", obj.Data)
	}
}

func TestObject_WithStructPayload(t *testing.T) {
	type Payload struct {
		Name  string `json:"name"`
		Value int    `json:"value"`
	}

	obj := Object[Payload]{
		Key:         "test/data.json",
		ContentType: "application/json",
		Data: Payload{
			Name:  testName,
			Value: 42,
		},
	}

	if obj.Data.Name != testName {
		t.Errorf("unexpected Data.Name: %q", obj.Data.Name)
	}
	if obj.Data.Value != 42 {
		t.Errorf("unexpected Data.Value: %d", obj.Data.Value)
	}
}

func TestObject_JSONSerialization(t *testing.T) {
	obj := Object[string]{
		Key:         "key",
		ContentType: "text/plain",
		Size:        5,
		ETag:        "etag",
		Metadata:    map[string]string{"m": "v"},
		Data:        "hello",
	}

	data, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var decoded Object[string]
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if decoded.Key != obj.Key {
		t.Errorf("Key mismatch: got %q, want %q", decoded.Key, obj.Key)
	}
	if decoded.ContentType != obj.ContentType {
		t.Errorf("ContentType mismatch: got %q, want %q", decoded.ContentType, obj.ContentType)
	}
	if decoded.Size != obj.Size {
		t.Errorf("Size mismatch: got %d, want %d", decoded.Size, obj.Size)
	}
	if decoded.ETag != obj.ETag {
		t.Errorf("ETag mismatch: got %q, want %q", decoded.ETag, obj.ETag)
	}
	if decoded.Data != obj.Data {
		t.Errorf("Data mismatch: got %q, want %q", decoded.Data, obj.Data)
	}
}

func TestObject_WithPointerPayload(t *testing.T) {
	type Inner struct {
		Value int
	}

	obj := Object[*Inner]{
		Key:  "ptr",
		Data: &Inner{Value: 123},
	}

	if obj.Data == nil {
		t.Fatal("Data should not be nil")
	}
	if obj.Data.Value != 123 {
		t.Errorf("unexpected Data.Value: %d", obj.Data.Value)
	}
}

func TestObject_WithSlicePayload(t *testing.T) {
	obj := Object[[]int]{
		Key:  "slice",
		Data: []int{1, 2, 3, 4, 5},
	}

	if len(obj.Data) != 5 {
		t.Errorf("expected 5 elements, got %d", len(obj.Data))
	}
	if obj.Data[0] != 1 || obj.Data[4] != 5 {
		t.Errorf("unexpected slice contents: %v", obj.Data)
	}
}

func TestObject_WithMapPayload(t *testing.T) {
	obj := Object[map[string]int]{
		Key:  "map",
		Data: map[string]int{"a": 1, "b": 2},
	}

	if len(obj.Data) != 2 {
		t.Errorf("expected 2 entries, got %d", len(obj.Data))
	}
	if obj.Data["a"] != 1 {
		t.Errorf("unexpected value for 'a': %d", obj.Data["a"])
	}
}

func TestObjectInfo_Alias(t *testing.T) {
	// Verify ObjectInfo is usable as expected
	info := ObjectInfo{
		Key:         "test",
		ContentType: "application/octet-stream",
		Size:        256,
		ETag:        "xyz",
		Metadata:    map[string]string{"foo": "bar"},
	}

	if info.Key != "test" {
		t.Errorf("unexpected Key: %q", info.Key)
	}
	if info.Size != 256 {
		t.Errorf("unexpected Size: %d", info.Size)
	}
}
