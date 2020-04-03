package workload

import (
	"fmt"
	"poseidon/pkg/api"
	"poseidon/pkg/context"
	"poseidon/pkg/worker"

	"github.com/docker/docker/api/types/filters"

	"github.com/pkg/errors"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

type docker struct {
	cli *client.Client
}

func newDockerWorkload() (Workload, error) {
	cli, err := client.NewEnvClient()
	if err != nil {
		return nil, errors.Wrap(err, "cannot create docker client")
	}
	return docker{cli}, nil
}

func (d docker) Schedule(ctx context.Context, spec api.NodeSpec, env map[string]string, n int) error {
	p := parallelism(n, spec.Parallelism)
	ctx.Logger().Tracef("scheduling workload for node %s with parallelism %d", spec.Name, p)

	containerEnv := []string{
		fmt.Sprintf("%s=%s", worker.EnvProcessID, ctx.ProcessID()),
		fmt.Sprintf("%s=%s", worker.EnvNodeName, ctx.NodeName()),
		fmt.Sprintf("%s=%s", worker.EnvPublishQName, "poseidon.ex.events"),
	}
	for k, v := range env {
		containerEnv = append(containerEnv, fmt.Sprintf("%s=%s", k, v))
	}

	for i := 0; i < p; i++ {
		resp, err := d.cli.ContainerCreate(ctx, &container.Config{
			Image: spec.Image,
			Cmd:   []string{"/app/worker"},
			Tty:   true,
			Labels: map[string]string{
				api.HeaderProcessID: ctx.ProcessID(),
				api.HeaderNodename:  ctx.NodeName(),
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

func (d docker) Delete(ctx context.Context, nodename string) error {
	filterArgs := filters.NewArgs()
	filterArgs.Add("label", fmt.Sprintf("%s=%s", api.HeaderProcessID, ctx.ProcessID()))
	filterArgs.Add("label", fmt.Sprintf("%s=%s", api.HeaderNodename, nodename))
	containers, err := d.cli.ContainerList(ctx, types.ContainerListOptions{
		All:     true,
		Filters: filterArgs,
	})
	if err != nil {
		return errors.Wrapf(err, "cannot list containers for node %s", nodename)
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

func dockernamePrefix(pid, nodename string) string {
	return fmt.Sprintf("%s-%s", pid[:7], nodename)
}
