package workload

import (
	"fmt"
	"poseidon/pkg/api"
	"poseidon/pkg/util/context"
	"poseidon/pkg/worker"

	"github.com/docker/docker/api/types/filters"

	"github.com/pkg/errors"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

// DockerWorkloadConfig is config struct for docker workload
type DockerWorkloadConfig struct {
	Env map[string]string
}

type docker struct {
	cli    *client.Client
	config DockerWorkloadConfig
}

// NewDockerWorkload returns a new instance of Workload using docker
func NewDockerWorkload(config DockerWorkloadConfig) (Workload, error) {
	cli, err := client.NewEnvClient()
	if err != nil {
		return nil, errors.Wrap(err, "cannot create docker client")
	}
	return docker{
		cli:    cli,
		config: config,
	}, nil
}

func (d docker) Schedule(ctx context.Context, spec WorkloadSpec, n int) error {
	p := parallelism(n, spec.MaxWorker)
	ctx.Logger().Tracef("scheduling workload for node %s with parallelism %d", ctx.NodeName(), p)

	containerEnv := []string{
		fmt.Sprintf("%s=%s", worker.EnvProcessID, ctx.ProcessID()),
		fmt.Sprintf("%s=%s", worker.EnvTaskID, ctx.TaskID()),
		fmt.Sprintf("%s=%s", worker.EnvPublishQName, "poseidon.ex.events"),
	}
	for k, v := range d.config.Env {
		containerEnv = append(containerEnv, fmt.Sprintf("%s=%s", k, v))
	}

	for i := 0; i < p; i++ {
		resp, err := d.cli.ContainerCreate(ctx, &container.Config{
			Image: spec.Image,
			Cmd:   []string{"/app/worker"},
			Tty:   true,
			Labels: map[string]string{
				api.HeaderProcessID: ctx.ProcessID(),
				api.HeaderTaskID:    ctx.TaskID(),
			},
			Env: containerEnv,
		}, &container.HostConfig{
			NetworkMode: "host",
		}, nil, "")
		if err != nil {
			return errors.Wrapf(err, "cannot create container for image %s", spec.Image)
		}

		if err := d.cli.ContainerStart(ctx, resp.ID, types.ContainerStartOptions{}); err != nil {
			return errors.Wrapf(err, "cannot start container %s", resp.ID)
		}
	}
	return nil
}

func (d docker) Delete(ctx context.Context, taskID string) error {
	filterArgs := filters.NewArgs()
	filterArgs.Add("label", fmt.Sprintf("%s=%s", api.HeaderProcessID, ctx.ProcessID()))
	filterArgs.Add("label", fmt.Sprintf("%s=%s", api.HeaderTaskID, taskID))
	containers, err := d.cli.ContainerList(ctx, types.ContainerListOptions{
		All:     true,
		Filters: filterArgs,
	})
	if err != nil {
		return errors.Wrapf(err, "cannot list containers for task %s", taskID)
	}
	for _, c := range containers {
		ctx.Logger().Tracef("removing container %s (%s)", c.ID, c.Names[0])
		if err := d.cli.ContainerRemove(ctx, c.ID, types.ContainerRemoveOptions{
			Force: true,
		}); err != nil {
			return errors.Wrapf(err, "cannot remove container %s", c.ID)
		}
	}

	return nil
}

func dockernamePrefix(processID, taskID string) string {
	return fmt.Sprintf("%s-%s", processID[:7], taskID)
}
