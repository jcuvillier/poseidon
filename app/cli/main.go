package main

import (
	"poseidon/app/cli/cmd"
)

func main() {
	cmd.NewRootCommand().Execute()
}
