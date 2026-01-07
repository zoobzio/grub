package grub

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
)

// Codec defines encoding/decoding operations for Store values.
type Codec interface {
	Encode(v any) ([]byte, error)
	Decode(data []byte, v any) error
}

// JSONCodec implements Codec using JSON encoding.
type JSONCodec struct{}

// Encode marshals v to JSON bytes.
func (JSONCodec) Encode(v any) ([]byte, error) {
	return json.Marshal(v)
}

// Decode unmarshals JSON data into v.
func (JSONCodec) Decode(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

// GobCodec implements Codec using Gob encoding.
type GobCodec struct{}

// Encode marshals v to Gob bytes.
func (GobCodec) Encode(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode unmarshals Gob data into v.
func (GobCodec) Decode(data []byte, v any) error {
	return gob.NewDecoder(bytes.NewReader(data)).Decode(v)
}
