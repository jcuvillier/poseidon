package client

import "fmt"

// HTTPError is an HTTP Error
type HTTPError struct {
	Message interface{} `json:"message"`
}

func (err HTTPError) Error() string {
	return fmt.Sprintf("%v", err.Message)
}

// ErrNotFound is the error returned when something requested could not be found.
type ErrNotFound struct {
	what string
}

func (err ErrNotFound) Error() string {
	return fmt.Sprintf("%s not found", err.what)
}

// ErrBadRequest is the error returned when there is something wrong with the request.
type ErrBadRequest struct {
	error
}

func (err ErrBadRequest) Error() string {
	return err.error.Error()
}
