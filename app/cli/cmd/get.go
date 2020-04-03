package cmd

import (
	"context"
	"log"
	"os"
	"poseidon/app/cli/cmd/client"
	"poseidon/app/cli/cmd/common"
	"poseidon/pkg/api"

	"github.com/spf13/cobra"
)

// NewGetCommand returns a new instance of a poseidon command
func NewGetCommand() *cobra.Command {
	command := &cobra.Command{
		Use:  "get",
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cli, err := client.New()
			if err != nil {
				log.Fatal(err)
			}

			state, err := cli.PipelineState(context.Background(), args[0])
			if err != nil {
				log.Fatal(err)
			}
			common.PrintPipeline(os.Stdout, api.PipelineState(state), args[0], common.PrintOptions{})
		},
	}
	return command
}
