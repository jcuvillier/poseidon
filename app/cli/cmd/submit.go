package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"poseidon/app/cli/cmd/client"
	pclient "poseidon/pkg/client"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

type submitOpts struct {
	watch bool // --watch
}

// NewSubmitCommand returns a new instance of a poseidon command
func NewSubmitCommand() *cobra.Command {
	var submitOpts submitOpts
	command := &cobra.Command{
		Use:   "submit",
		Short: "submit a workflow",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cli, err := client.New()
			if err != nil {
				log.Fatal(err)
			}

			pipelineFile, err := os.Open(args[0])
			if err != nil {
				log.Fatal(errors.Errorf("cannot open file %s", args[0]))
			}
			var pipelineSpec pclient.SubmitRequest
			if err := json.NewDecoder(pipelineFile).Decode(&pipelineSpec); err != nil {
				log.Fatal(errors.Wrapf(err, "cannot decode file %s as Pipeline Specification", args[0]))
			}

			ctx := context.Background()
			pid, err := cli.Submit(ctx, pipelineSpec.PipelineSpec, pipelineSpec.Args)
			if err != nil {
				log.Fatal(err)
			}

			if submitOpts.watch {
				if err := watch(ctx, pid); err != nil {
					log.Fatal(err)
				}
			} else {
				fmt.Printf("Pipeline submitted with process ID %s\n", pid)
			}
		},
	}
	command.Flags().BoolVarP(&submitOpts.watch, "watch", "w", false, "watch the workflow until it completes")

	return command
}
