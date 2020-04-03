package cmd

import (
	"context"
	"log"
	"poseidon/app/cli/cmd/client"
	"poseidon/app/cli/cmd/common"
	"poseidon/pkg/api"
	"time"

	tm "github.com/buger/goterm"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

// NewWatchCommand returns a new instance of a poseidon command
func NewWatchCommand() *cobra.Command {
	command := &cobra.Command{
		Use:  "watch",
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if err := watch(context.Background(), args[0]); err != nil {
				log.Fatal(err)
			}
		},
	}
	return command
}

func watch(ctx context.Context, pid string) error {
	cli, err := client.New()
	if err != nil {
		return errors.Wrap(err, "cannot create poseidon client")
	}
	tm.Clear()
	for {
		state, err := cli.PipelineState(ctx, pid)
		if err != nil {
			return errors.Wrapf(err, "cannot get state of pipeline with processID %s", pid)
		}
		tm.MoveCursor(1, 1)
		common.PrintPipeline(tm.Screen, api.PipelineState(state), pid, common.PrintOptions{})
		tm.Flush()
		if state.Status.Finished() {
			break
		}
		time.Sleep(1 * time.Second)
	}
	return nil
}
