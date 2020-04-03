package store

import (
	"context"
	"poseidon/pkg/api"
	"time"
)

// Store interface defines access to the store backend
type Store interface {
	PipelineStore
	ExecutorStore
}

// PipelineStore interface defines access to the pipeline store backend.
type PipelineStore interface {
	// CreatePipeline creates a new pipline with its spec and arguments
	CreatePipeline(ctx context.Context, pid string, spec api.PipelineSpec, args interface{}) error
	CreateNodes(ctx context.Context, pid string, names []string) error
	GetPipelineSpec(ctx context.Context, pid string) (api.PipelineSpec, error)
	GetPipelineArgs(ctx context.Context, pid string) (interface{}, error)
	GetNodeStatuses(ctx context.Context, pid string) (map[string]api.Status, error)
	SetPipelineStatus(ctx context.Context, pid string, status api.Status) error
	IsPipelineFinished(ctx context.Context, pid string) (bool, error)
	// List the pipelines as a map with processID as key and name as value
	ListPipelines(ctx context.Context) (map[string]string, error)
	PipelineState(ctx context.Context, pid string) (api.PipelineState, error)
}

// ExecutorStore interface defines access to the pipeline store backend.
type ExecutorStore interface {
	// CreateJobs creates
	GetNodeSpec(ctx context.Context, pid, nodename string) (api.NodeSpec, error)
	CreateJobs(ctx context.Context, pid, nodename string, params []interface{}) error
	SetNodeStatus(ctx context.Context, pid, nodename string, status api.Status) error
	GetJobsStatuses(ctx context.Context, pid, nodename string) (map[string]api.Status, error)
	// SetNodeRunning sets the node status to running  if not already in running or a finished status.
	SetNodeRunning(ctx context.Context, pid, nodename string) error
	SetJobStatus(ctx context.Context, pid, nodename, jobID string, status api.Status, t time.Time, payload interface{}) error
	GetJobStatus(ctx context.Context, pid, nodename, jobID string) (api.Status, error)
	StopJobs(ctx context.Context, pid, nodename string, status api.Status) error

	// States
	NodeState(ctx context.Context, pid, nodename string) (api.NodeState, error)
	JobState(ctx context.Context, pid, nodename, jobID string) (api.JobState, error)

	//Results
	NodeResult(ctx context.Context, pid, nodename string) (interface{}, error)
	JobResult(ctx context.Context, pid, nodenam, jobid string) (interface{}, error)
}
