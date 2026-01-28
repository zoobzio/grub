package grub

import "context"

// BeforeSave is called before persisting T. Return an error to abort the operation.
type BeforeSave interface {
	BeforeSave(ctx context.Context) error
}

// AfterSave is called after T has been successfully persisted.
// Return an error to signal a post-save invariant failure.
type AfterSave interface {
	AfterSave(ctx context.Context) error
}

// AfterLoad is called after T has been loaded and decoded.
// Return an error to signal a post-load invariant failure.
type AfterLoad interface {
	AfterLoad(ctx context.Context) error
}

// BeforeDelete is called before deleting a record.
// Invoked on a zero-value T (no loaded state). Return an error to abort the operation.
type BeforeDelete interface {
	BeforeDelete(ctx context.Context) error
}

// AfterDelete is called after a record has been successfully deleted.
// Invoked on a zero-value T (no loaded state). Return an error to signal a post-delete failure.
type AfterDelete interface {
	AfterDelete(ctx context.Context) error
}

// callBeforeSave calls BeforeSave on value if T implements the interface.
func callBeforeSave[T any](ctx context.Context, value *T) error {
	if h, ok := any(value).(BeforeSave); ok {
		return h.BeforeSave(ctx)
	}
	return nil
}

// callAfterSave calls AfterSave on value if T implements the interface.
func callAfterSave[T any](ctx context.Context, value *T) error {
	if h, ok := any(value).(AfterSave); ok {
		return h.AfterSave(ctx)
	}
	return nil
}

// callAfterLoad calls AfterLoad on value if T implements the interface.
func callAfterLoad[T any](ctx context.Context, value *T) error {
	if h, ok := any(value).(AfterLoad); ok {
		return h.AfterLoad(ctx)
	}
	return nil
}

// callAfterLoadSlice calls AfterLoad on each element if T implements the interface.
func callAfterLoadSlice[T any](ctx context.Context, values []*T) error {
	for _, v := range values {
		if err := callAfterLoad(ctx, v); err != nil {
			return err
		}
	}
	return nil
}

// callBeforeDelete calls BeforeDelete on a zero-value T if T implements the interface.
func callBeforeDelete[T any](ctx context.Context) error {
	var zero T
	if h, ok := any(&zero).(BeforeDelete); ok {
		return h.BeforeDelete(ctx)
	}
	return nil
}

// callAfterDelete calls AfterDelete on a zero-value T if T implements the interface.
func callAfterDelete[T any](ctx context.Context) error {
	var zero T
	if h, ok := any(&zero).(AfterDelete); ok {
		return h.AfterDelete(ctx)
	}
	return nil
}
