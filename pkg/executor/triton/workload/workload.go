package workload

import (
	"math"
	"poseidon/pkg/util/context"
)

const (
	maxParallelism = 5
)

type WorkloadSpec struct {
	Image     string            `json:"image"`
	Resources ResourcesSpec     `json:"resources"`
	MaxWorker int               `json:"maxworkers"`
	Env       map[string]string `json:"env"`
}

type ResourcesSpec struct {
	CPU    string `json:"cpu"`
	Memory string `json:"memory"`
	GPU    string `json:"gpu"`
}

// Workload defines how the workload is scheduled and deleted.
type Workload interface {

	// Schedule schedules the workload to handle the given node.
	// n is the expected parallelism. May be replaced with the max value defined for the scheduler
	Schedule(ctx context.Context, spec WorkloadSpec, n int) error

	// Delete deletes the workload for the given node.
	Delete(ctx context.Context, nodename string) error
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
