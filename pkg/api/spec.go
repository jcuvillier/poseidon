package api

import (
	"strings"
)

const (
	// InputPipelineArgs is keyword used for refering to input pipeline arguments
	InputPipelineArgs = "args"

	inputDepPrefix      = "@"
	inputBatchDepPrefix = "#"
	inputDepPathSep     = ":"
)

// PipelineSpec is the specification of a Pipeline.
type PipelineSpec struct {
	Name  string     `json:"name"` // Pipeline name.
	Nodes []NodeSpec `json:"nodes"`
}

// NodeSpec is the specification of a Node.
type NodeSpec struct {
	// Kind is the executor to be used to execute the node
	Kind           string      `json:"kind"`
	Name           string      `json:"name"`
	Dependencies   []string    `json:"dependencies"`
	Input          interface{} `json:"input"`
	ContinueOnFail bool        `json:"cof"`
	ExecutorSpec   interface{} `json:"spec"`
}

// InputDependency is a structure representing a input dependency
type InputDependency struct {
	Name    string
	Path    string
	IsBatch bool
}

// AsInputDependency returns the given string as an InputDependency instance
func AsInputDependency(in string) (InputDependency, bool) {
	var str string
	batch := false
	if strings.HasPrefix(in, inputDepPrefix) {
		str = in[len(inputDepPrefix):]
	} else if strings.HasPrefix(in, inputBatchDepPrefix) {
		str = in[len(inputBatchDepPrefix):]
		batch = true
	} else {
		return InputDependency{}, false
	}
	i := strings.Index(str, inputDepPathSep)
	if i == -1 {
		return InputDependency{
			Name:    str,
			IsBatch: batch,
		}, true
	}
	return InputDependency{
		Name:    str[:i],
		Path:    str[i+1:],
		IsBatch: batch,
	}, true
}

// InputDependencies returns the node dependencies.
func (n NodeSpec) InputDependencies() []InputDependency {
	var deps []InputDependency
	dep(&deps, n.Input)
	return deps
}

// dep is a recursive function to retrieve node dependency in node input
func dep(res *[]InputDependency, i interface{}) {
	if asMap, isMap := i.(map[string]interface{}); isMap {
		for _, v := range asMap {
			dep(res, v)
		}
	}
	if asString, isString := i.(string); isString {
		if d, isDep := AsInputDependency(asString); isDep {
			*res = append(*res, d)
		}
	}
	return
}
