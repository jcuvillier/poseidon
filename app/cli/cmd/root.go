package cmd

import (
	"github.com/spf13/cobra"
)

// NewRootCommand returns a new instance of a poseidon command
func NewRootCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "poseidon",
		Short: "poseidon is the command line interface to Poseidon",
		Run: func(cmd *cobra.Command, args []string) {

		},
	}

	rootCmd.AddCommand(NewSubmitCommand())
	rootCmd.AddCommand(NewWatchCommand())
	rootCmd.AddCommand(NewGetCommand())
	return rootCmd
}
