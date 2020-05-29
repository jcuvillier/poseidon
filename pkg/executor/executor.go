package executor

import (
	"poseidon/pkg/api"
	"poseidon/pkg/util/context"
)

// CallbackFunc is the function called when a job is finished
type CallbackFunc func(ctx context.Context, payload interface{}, s api.Status) error

// Executor is the mechanism used to run jobs
type Executor interface {
	// Start informs the executor a task is started with n jobs.
	Start(ctx context.Context, spec interface{}, n int) error

	// Stop stops the execution of a task
	Stop(ctx context.Context, graceful bool) error

	// SubmitJob submits a job
	SubmitJob(ctx context.Context, jobID string, param interface{}) error

	// SetCallbackFunc sets the function to be called when a job is done
	SetCallbackFunc(f CallbackFunc)
}

// JobError is the payload returned to TaskScheduler when a job failed
type JobError struct {
	Message string `json:"message"`
}
