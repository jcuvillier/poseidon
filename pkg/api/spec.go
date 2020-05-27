package api

const (
	// InputPipelineArgs is keyword used for refering to input pipeline arguments
	InputPipelineArgs = "args"
)

// PipelineSpec is the specification of a Pipeline.
type PipelineSpec struct {
	Name  string     `json:"name"` // Pipeline name.
	Tasks []TaskSpec `json:"tasks"`
}

// TaskSpec is the specification of a Task.
type TaskSpec struct {
	// Kind is the executor to be used to execute the node
	Kind           string      `json:"kind"`
	Name           string      `json:"name"`
	Dependencies   []string    `json:"dependencies"`
	Input          interface{} `json:"input"`
	ContinueOnFail bool        `json:"cof"`
	Parallelism    int         `json:"parallelism"`
	ExecutorSpec   interface{} `json:"spec"`
}
