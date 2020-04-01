package common

import (
	"fmt"
	"io"
	"poseidon/pkg/api"
	"text/tabwriter"
)

var (
	jobStatusIconMap map[api.Status]string
)

func init() {
	jobStatusIconMap = map[api.Status]string{
		api.StatusCreated:    "◷",
		api.StatusSubmitted:  "◷",
		api.StatusRunning:    "●",
		api.StatusCancelled:  "ǁ",
		api.StatusTerminated: "ǁ",
		api.StatusCompleted:  "✔",
		api.StatusFailed:     "✖",
		api.StatusSkipped:    "○",
	}
}

// PrintOptions defines print options
type PrintOptions struct{}

// PrintPipeline prints the pipeline state in the given writer
func PrintPipeline(w io.Writer, pipeline api.PipelineState, opts PrintOptions) {
	fmt.Fprintln(w)

	// Header
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "Name:\t%s\n", pipeline.Name)
	fmt.Fprintf(tw, "Status:\t%s\n", pipeline.Status)
	tw.Flush()
	fmt.Fprintln(w)

	fmt.Fprintln(tw, "STEP\t")
	fmt.Fprintln(tw)
	fmt.Fprintln(tw, jobStatusIconMap[pipeline.Status], pipeline.Name)

	// Filter nodes with status CREATED
	var nodes []api.NodeState
	for _, node := range pipeline.Nodes {
		if node.Status != api.StatusCreated {
			nodes = append(nodes, node)
		}
	}
	// TODO order by start date

	for i := 0; i < len(nodes); i++ {
		node := nodes[i]
		prefix := "├"
		if i == len(nodes)-1 {
			prefix = "└"
		}
		printNode(w, node, prefix, opts)
	}
	tw.Flush()
}

func printNode(w io.Writer, node api.NodeState, prefix string, opts PrintOptions) {
	fmt.Fprintln(w, prefix, jobStatusIconMap[node.Status], node.Name)
}
