package common

import (
	"context"
	"poseidon/pkg/api"
	"poseidon/pkg/client"

	"github.com/pkg/errors"
)

// Fullstate returns the pipeline state with job state filled for running and finished nodes.
func Fullstate(ctx context.Context, cli client.Client, pid string) (api.PipelineState, error) {
	pstate, err := cli.PipelineState(ctx, pid)
	if err != nil {
		return api.PipelineState{}, errors.Wrapf(err, "cannot get state for process %s", pid)
	}
	for i, n := range pstate.Nodes {
		if n.Status.Finished() || n.Status == api.StatusRunning {
			nstate, err := cli.NodeState(ctx, pid, n.Name)
			if err != nil {
				return api.PipelineState{}, errors.Wrapf(err, "cannot get state for node %s of process %s", n.Name, pid)
			}
			pstate.Nodes[i] = api.NodeState(nstate)
		}
	}
	return api.PipelineState(pstate), nil
}
