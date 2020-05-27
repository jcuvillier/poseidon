package store

import (
	"context"
	"poseidon/pkg/api"
	"time"
)

// TimeOption is used when setting time is necessary
type TimeOption struct {
	CreateTime time.Time
	StartTime  time.Time
	EndTime    time.Time
}

// Store interface defines access to the store backend
type Store interface {
	PipelineSchedulerStore
	TaskSchedulerStore
	ReadOnlyStore
}

// PipelineSchedulerStore defines access to the store backend for TaskScheduler
type PipelineSchedulerStore interface {
	// CreatePipeline creates a new pipline with its spec and arguments
	CreatePipeline(ctx context.Context, processID string, spec api.PipelineSpec, args interface{}) error
	CreateTasks(ctx context.Context, processID string, names []string) error
	GetPipelineSpec(ctx context.Context, processID string) (api.PipelineSpec, error)
	GetPipelineArgs(ctx context.Context, processID string) (interface{}, error)
	GetTaskStatuses(ctx context.Context, processID string) (map[string]api.Status, error)
	GetTasksWithStatus(ctx context.Context, processID string, statuses []api.Status) ([]string, error)
	SetPipelineStatus(ctx context.Context, processID string, status api.Status, opt TimeOption) error
	IsPipelineFinished(ctx context.Context, processID string) (bool, error)
	GetTaskResult(ctx context.Context, processID, taskID string) (interface{}, error)
}

// TaskSchedulerStore defines access to the store backend for TaskScheduler
type TaskSchedulerStore interface {
	// SetTaskStatus set the given status to the task with time option
	SetTaskStatus(ctx context.Context, processID, taskID string, status api.Status, opt TimeOption) error

	// GetTaskStatus returns task's status
	GetTaskStatus(ctx context.Context, processID, taskID string) (api.Status, error)

	//GetTaskSpec returns the spec of the given task
	GetTaskSpec(ctx context.Context, processID, taskID string) (api.TaskSpec, error)

	//CreateJobs creates the jobs for the given task
	CreateJobs(ctx context.Context, processID, taskID string, params map[string]interface{}) error
	SetJobStatus(ctx context.Context, processID, taskID, jobID string, status api.Status, opt TimeOption, payload interface{}) error

	// CountJobWithStatus counts the task's jobs with status in the given status set
	CountJobWithStatus(ctx context.Context, processID, taskID string, statuses []api.Status) (int, error)

	// GetOneJob returns a jobID corresponding to the given status with its parameters.
	GetOneJob(ctx context.Context, processID, taskID string, status api.Status) (string, interface{}, error)

	StopJobs(ctx context.Context, processID, taskID string, status api.Status) error
}

// ReadOnlyStore are functions used by controller to access data in RO
type ReadOnlyStore interface {
	// List the pipelines as a map with processID as key and name as value
	ListPipelines(ctx context.Context) (map[string]string, error)
	GetPipelineState(ctx context.Context, processID string) (api.PipelineState, error)
	GetTaskState(ctx context.Context, pid, taskID string) (api.TaskState, error)
	GetJobState(ctx context.Context, pid, taskID, jobID string) (api.JobState, error)
	GetJobResult(ctx context.Context, pid, taskID, jobID string) (interface{}, error)
}
