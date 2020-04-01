package workload

import (
	"fmt"
	"poseidon/pkg/api"
	"poseidon/pkg/context"
)

const namespace = "default"

type k8sDeployment struct {
	// clientset *kubernetes.Clientset
}

func (k k8sDeployment) Schedule(ctx context.Context, spec api.NodeSpec, n int) error {
	p := parallelism(n, spec.Parallelism)
	ctx.Logger().Tracef("scheduling workload for node %s with parallelism at %d", spec.Name, p)
	// deploymentsClient := clientset.AppsV1().Deployments(namespace)
	// dep := &appsv1.Deployment{}
	return nil
}

func (k k8sDeployment) Delete(ctx context.Context, spec api.NodeSpec) error {
	ctx.Logger().Tracef("deleting workload for node %s", spec.Name)
	return nil
}

func deploymentName(ctx context.Context, spec api.NodeSpec) string {
	return fmt.Sprintf("%s-%s", spec.Name, ctx.ProcessID())
}
