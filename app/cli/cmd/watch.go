package cmd

import (
	"context"
	"log"
	"poseidon/app/cli/cmd/client"
	"poseidon/app/cli/cmd/common"
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
	for {
		state, err := common.Fullstate(ctx, cli, pid)
		if err != nil {
			return err
		}
		tm.Clear()
		tm.MoveCursor(1, 1)
		common.PrintPipeline(tm.Screen, state, pid, common.PrintOptions{})
		tm.Flush()
		if state.Status.Finished() {
			break
		}
		time.Sleep(1 * time.Second)
	}
	return nil
}
