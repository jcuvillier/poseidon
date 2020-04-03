package common

import (
	"fmt"
	"io"
	"math"
	"poseidon/pkg/api"
	"sort"
	"text/tabwriter"
	"time"
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
func PrintPipeline(w io.Writer, pipeline api.PipelineState, pid string, opts PrintOptions) {
	fmt.Fprintln(w)

	// Header
	tw := tabwriter.NewWriter(w, 0, 0, 3, ' ', 0)
	fmt.Fprintf(tw, "Name:\t%s\n", pipeline.Name)
	fmt.Fprintf(tw, "ProcessID:\t%s\n", pid)
	fmt.Fprintf(tw, "Status:\t%s\n", pipeline.Status)
	fmt.Fprintf(tw, "Created:\t%s\n", date(pipeline.CreateTime))
	fmt.Fprintf(tw, "Started:\t%s\n", date(pipeline.StartTime))
	fmt.Fprintf(tw, "Finished:\t%s\n", date(pipeline.EndTime))
	fmt.Fprintf(tw, "Duration:\t%s\n", duration(pipeline.StartTime, pipeline.EndTime))
	tw.Flush()
	fmt.Fprintln(w)

	tw.Init(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "NODE\tDURATION")
	fmt.Fprintf(tw, "%s %s\t\n", jobStatusIconMap[pipeline.Status], pipeline.Name)

	// Filter nodes with status CREATED
	var nodes []api.NodeState
	for _, node := range pipeline.Nodes {
		if node.Status != api.StatusCreated {
			nodes = append(nodes, node)
		}
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].StartTime.Before(*nodes[j].StartTime)
	})

	for i := 0; i < len(nodes); i++ {
		node := nodes[i]
		prefix := "├"
		if i == len(nodes)-1 {
			prefix = "└"
		}
		printNode(tw, node, prefix, opts)
	}
	tw.Flush()
}

func printNode(w io.Writer, node api.NodeState, prefix string, opts PrintOptions) {
	fmt.Fprintf(w, "%s %s %s\t%s\n", prefix, jobStatusIconMap[node.Status], node.Name, duration(node.StartTime, node.EndTime))
}

func date(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.Format("2 Jan 2006 15:04:05.000")
}

func duration(start, end *time.Time) string {
	var d time.Duration
	if start == nil {
		return ""
	}
	if end == nil {
		d = time.Now().Sub(*start)
	} else {
		d = end.Sub(*start)
	}

	// Print
	if d.Seconds() <= 60.0 {
		return fmt.Sprintf("%0.0fs", d.Seconds())
	} else if d.Minutes() <= 60.0 {
		m := int64(d.Minutes())
		s := math.Mod(d.Seconds(), 60)
		return fmt.Sprintf("%0.dm %0.0fs", m, s)
	} else {
		h := int64(d.Hours())
		m := int64(math.Mod(d.Minutes(), 60))
		s := math.Mod(d.Seconds(), 60)
		return fmt.Sprintf("%0.dh %0.dm %0.0fs", h, m, s)
	}
}
