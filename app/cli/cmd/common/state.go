package common

import (
	"context"
	"poseidon/pkg/api"
	"poseidon/pkg/client"

	"github.com/pkg/errors"
)

// Fullstate returns the pipeline state with job state filled for running and finished tasks.
func Fullstate(ctx context.Context, cli client.Client, pid string) (api.PipelineState, error) {
	pstate, err := cli.PipelineState(ctx, pid)
	if err != nil {
		return api.PipelineState{}, errors.Wrapf(err, "cannot get state for process %s", pid)
	}
	for i, t := range pstate.Tasks {
		if t.Status.Finished() || t.Status == api.StatusRunning {
			tstate, err := cli.TaskState(ctx, pid, t.Name)
			if err != nil {
				return api.PipelineState{}, errors.Wrapf(err, "cannot get state for task %s of process %s", t.Name, pid)
			}
			pstate.Tasks[i] = api.TaskState(tstate)
		}
	}
	return api.PipelineState(pstate), nil
}
