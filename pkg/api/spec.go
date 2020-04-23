package api

import (
	"regexp"
	"strings"

	"github.com/pkg/errors"
)

const (
	// InputPipelineArgs is keyword used for refering to input pipeline arguments
	InputPipelineArgs = "args"

	inputDepPrefix      = "$"
	inputBatchDepPrefix = "#"
	inputDepPathSep     = ":"
)

var (
	// InputDepRegexp is compiled regexp for dependency in input structure
	InputDepRegexp *regexp.Regexp
)

func init() {
	r, err := regexp.Compile("([\\#|\\$]\\{[^}]+\\})")
	if err != nil {
		panic(errors.Wrap(err, "cannot compile node dependency regexp"))
	}
	InputDepRegexp = r
}

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
	OriginalString string
	Name           string
	Path           string
	IsBatch        bool
}

// InputDependencies extracts the dependencies from the string
func InputDependencies(in string) []InputDependency {
	var deps []InputDependency
	strDeps := InputDepRegexp.FindAllString(in, -1)
	for _, str := range strDeps {
		dep := AsInputDependency(str)
		if dep.Name != "" {
			deps = append(deps, dep)
		}
	}
	return deps
}

// AsInputDependency create a InputDependency struct from a string.
// This function should be called
func AsInputDependency(in string) InputDependency {
	var str string
	batch := false
	if strings.HasPrefix(in, inputDepPrefix) {
		str = in[len(inputDepPrefix)+1 : len(in)-1]
	} else if strings.HasPrefix(in, inputBatchDepPrefix) {
		str = in[len(inputBatchDepPrefix)+1 : len(in)-1]
		batch = true
	} else {
		return InputDependency{}
	}
	i := strings.Index(str, inputDepPathSep)
	if i == -1 {
		return InputDependency{
			OriginalString: in,
			Name:           str,
			IsBatch:        batch,
		}
	}
	return InputDependency{
		OriginalString: in,
		Name:           str[:i],
		Path:           str[i+1:],
		IsBatch:        batch,
	}
}

// GetInputDependencies returns the node dependencies extracted in the inputs
func (n NodeSpec) GetInputDependencies() []InputDependency {
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
		deps := InputDependencies(asString)
		*res = append(*res, deps...)
	}
	return
}
