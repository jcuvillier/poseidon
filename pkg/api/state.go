package api

import (
	"time"
)

// PipelineInfo represents basic pipeline information
type PipelineInfo struct {
	Name      string
	ProcessID string
}

// PipelineState represents pipeline state.
type PipelineState struct {
	Name       string      `json:"name"`
	Status     Status      `json:"status"`
	Nodes      []NodeState `json:"nodes,omitempty"`
	CreateTime *time.Time  `json:"createTime,omitempty"`
	StartTime  *time.Time  `json:"startTime,omitempty"`
	EndTime    *time.Time  `json:"endTime,omitempty"`
}

// NodeState represents node state.
type NodeState struct {
	Name      string     `json:"name"`
	Status    Status     `json:"status"`
	Jobs      []JobState `json:"jobs,omitempty"`
	StartTime *time.Time `json:"startTime,omitempty"`
	EndTime   *time.Time `json:"endTime,omitempty"`
}

// JobState represents job state.
type JobState struct {
	ID        string     `json:"id"`
	Status    Status     `json:"status"`
	StartTime *time.Time `json:"startTime,omitempty"`
	EndTime   *time.Time `json:"endTime,omitempty"`
}
