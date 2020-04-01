package store

import "fmt"

// NotFoundError returns a new ErrNotFound
func NotFoundError(what string) error {
	return ErrNotFound{what}
}

// ErrNotFound is the error returned when something requested could not be found.
// This error should not be retried.
type ErrNotFound struct {
	what string
}

func (err ErrNotFound) Error() string {
	return fmt.Sprintf("%s not found", err.what)
}
