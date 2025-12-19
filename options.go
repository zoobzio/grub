package grub

// Option configures a Service.
type Option[T any] func(*Service[T])

// WithCodec sets a custom codec for the service.
// If not specified, JSONCodec is used.
func WithCodec[T any](c Codec) Option[T] {
	return func(s *Service[T]) {
		s.codec = c
	}
}
