package api

// PipelineInfo represents basic pipeline information
type PipelineInfo struct {
	Name      string
	ProcessID string
}

// PipelineState represents pipeline state.
type PipelineState struct {
	Name   string      `json:"name"`
	Status Status      `json:"status"`
	Nodes  []NodeState `json:"nodes,omitempty"`
}

// NodeState represents node state.
type NodeState struct {
	Name   string     `json:"name"`
	Status Status     `json:"status"`
	Jobs   []JobState `json:"jobs,omitempty"`
}

// JobState represents job state.
type JobState struct {
	ID     string `json:"id"`
	Status Status `json:"status"`
}
