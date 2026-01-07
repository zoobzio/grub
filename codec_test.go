package grub

import (
	"testing"
)

type testStruct struct {
	Name  string `json:"name"`
	Value int    `json:"value"`
}

func TestJSONCodec_Encode(t *testing.T) {
	codec := JSONCodec{}

	t.Run("struct", func(t *testing.T) {
		v := testStruct{Name: "test", Value: 42}
		data, err := codec.Encode(v)
		if err != nil {
			t.Fatalf("Encode failed: %v", err)
		}
		if len(data) == 0 {
			t.Error("expected non-empty data")
		}
	})

	t.Run("pointer", func(t *testing.T) {
		v := &testStruct{Name: "test", Value: 42}
		data, err := codec.Encode(v)
		if err != nil {
			t.Fatalf("Encode failed: %v", err)
		}
		if len(data) == 0 {
			t.Error("expected non-empty data")
		}
	})

	t.Run("nil", func(t *testing.T) {
		data, err := codec.Encode(nil)
		if err != nil {
			t.Fatalf("Encode nil failed: %v", err)
		}
		if string(data) != "null" {
			t.Errorf("expected 'null', got %q", string(data))
		}
	})

	t.Run("map", func(t *testing.T) {
		v := map[string]int{"a": 1, "b": 2}
		data, err := codec.Encode(v)
		if err != nil {
			t.Fatalf("Encode map failed: %v", err)
		}
		if len(data) == 0 {
			t.Error("expected non-empty data")
		}
	})
}

func TestJSONCodec_Decode(t *testing.T) {
	codec := JSONCodec{}

	t.Run("struct", func(t *testing.T) {
		data := []byte(`{"name":"test","value":42}`)
		var v testStruct
		if err := codec.Decode(data, &v); err != nil {
			t.Fatalf("Decode failed: %v", err)
		}
		if v.Name != "test" {
			t.Errorf("expected name 'test', got %q", v.Name)
		}
		if v.Value != 42 {
			t.Errorf("expected value 42, got %d", v.Value)
		}
	})

	t.Run("invalid json", func(t *testing.T) {
		data := []byte(`{invalid}`)
		var v testStruct
		if err := codec.Decode(data, &v); err == nil {
			t.Error("expected error for invalid JSON")
		}
	})

	t.Run("empty", func(t *testing.T) {
		data := []byte(`{}`)
		var v testStruct
		if err := codec.Decode(data, &v); err != nil {
			t.Fatalf("Decode empty failed: %v", err)
		}
		if v.Name != "" || v.Value != 0 {
			t.Errorf("expected zero values, got %+v", v)
		}
	})
}

func TestJSONCodec_RoundTrip(t *testing.T) {
	codec := JSONCodec{}

	original := testStruct{Name: "roundtrip", Value: 123}
	data, err := codec.Encode(original)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	var decoded testStruct
	if err := codec.Decode(data, &decoded); err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Name != original.Name {
		t.Errorf("name mismatch: got %q, want %q", decoded.Name, original.Name)
	}
	if decoded.Value != original.Value {
		t.Errorf("value mismatch: got %d, want %d", decoded.Value, original.Value)
	}
}

func TestGobCodec_Encode(t *testing.T) {
	codec := GobCodec{}

	t.Run("struct", func(t *testing.T) {
		v := testStruct{Name: "test", Value: 42}
		data, err := codec.Encode(v)
		if err != nil {
			t.Fatalf("Encode failed: %v", err)
		}
		if len(data) == 0 {
			t.Error("expected non-empty data")
		}
	})

	t.Run("pointer", func(t *testing.T) {
		v := &testStruct{Name: "test", Value: 42}
		data, err := codec.Encode(v)
		if err != nil {
			t.Fatalf("Encode failed: %v", err)
		}
		if len(data) == 0 {
			t.Error("expected non-empty data")
		}
	})
}

func TestGobCodec_Decode(t *testing.T) {
	codec := GobCodec{}

	t.Run("invalid data", func(t *testing.T) {
		data := []byte(`not gob data`)
		var v testStruct
		if err := codec.Decode(data, &v); err == nil {
			t.Error("expected error for invalid Gob data")
		}
	})
}

func TestGobCodec_RoundTrip(t *testing.T) {
	codec := GobCodec{}

	original := testStruct{Name: "roundtrip", Value: 456}
	data, err := codec.Encode(original)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	var decoded testStruct
	if err := codec.Decode(data, &decoded); err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Name != original.Name {
		t.Errorf("name mismatch: got %q, want %q", decoded.Name, original.Name)
	}
	if decoded.Value != original.Value {
		t.Errorf("value mismatch: got %d, want %d", decoded.Value, original.Value)
	}
}

func TestGobCodec_ComplexTypes(t *testing.T) {
	codec := GobCodec{}

	type complexStruct struct {
		Names  []string
		Values map[string]int
		Nested *testStruct
	}

	original := complexStruct{
		Names:  []string{"a", "b", "c"},
		Values: map[string]int{"x": 1, "y": 2},
		Nested: &testStruct{Name: "nested", Value: 99},
	}

	data, err := codec.Encode(original)
	if err != nil {
		t.Fatalf("Encode complex failed: %v", err)
	}

	var decoded complexStruct
	if err := codec.Decode(data, &decoded); err != nil {
		t.Fatalf("Decode complex failed: %v", err)
	}

	if len(decoded.Names) != 3 {
		t.Errorf("expected 3 names, got %d", len(decoded.Names))
	}
	if decoded.Nested == nil || decoded.Nested.Name != "nested" {
		t.Errorf("nested struct mismatch: %+v", decoded.Nested)
	}
}
