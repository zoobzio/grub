package grub

import "encoding/json"

// JSONCodec implements Codec using JSON encoding.
type JSONCodec struct{}

// Marshal serializes a value to JSON bytes.
func (JSONCodec) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

// Unmarshal deserializes JSON bytes into a value.
func (JSONCodec) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

// ContentType returns the JSON MIME type.
func (JSONCodec) ContentType() string {
	return "application/json"
}

// Ensure JSONCodec implements Codec.
var _ Codec = JSONCodec{}
