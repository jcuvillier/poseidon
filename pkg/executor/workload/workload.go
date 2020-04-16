package workload

import (
	"math"
	"poseidon/pkg/api"
	"poseidon/pkg/context"
)

const (
	maxParallelism = 5
)

// Workload defines how the workload is scheduled and deleted.
type Workload interface {

	// Schedule schedules the workload to handle the given node.
	// n is the expected parallelism. May be replaced with the max value defined for the scheduler
	Schedule(ctx context.Context, spec api.NodeSpec, env map[string]string, n int) error

	// Delete deletes the workload for the given node.
	Delete(ctx context.Context, nodename string) error
}

// New returns a new Workload
func New() (Workload, error) {
	return NewK8SJobWorkload(K8SJobWorkloadConfig{
		OutOfCluster: true,
		BrokerSecret: "broker-conf-db9b792g55",
	})
}

type dummy struct{}

func (d *dummy) Schedule(ctx context.Context, spec api.NodeSpec, n int) error {
	ctx.Logger().Tracef("scheduling workload for node %s", spec.Name)
	return nil
}

func (d *dummy) Delete(ctx context.Context, nodename string) error {
	ctx.Logger().Tracef("deleting workload for node %s", nodename)
	return nil
}

func parallelism(n, specParallelism int) int {
	if specParallelism == 0 {
		specParallelism = math.MaxInt16
	}
	min := n
	if min > specParallelism {
		min = specParallelism
	}
	if min > maxParallelism {
		min = maxParallelism
	}
	return min
}
