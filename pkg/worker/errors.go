package worker

// WError is the Error type returned by the Func
type WError interface {
	error
	ShouldRetry() bool
}

// ErrBadRequest is error for Bad request
type ErrBadRequest struct {
	error
}

// ShouldRetry is implementation of WError interface
func (e ErrBadRequest) ShouldRetry() bool {
	return false
}

// ErrExhaustedRetries is error sent when maximum number of retries has been reached
type ErrExhaustedRetries struct {
	error
}

// ShouldRetry is implementation of WError interface
func (e ErrExhaustedRetries) ShouldRetry() bool {
	return false
}
