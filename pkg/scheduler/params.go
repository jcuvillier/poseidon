package scheduler

import (
	"poseidon/pkg/util/maps"
	"reflect"

	"poseidon/pkg/api"
	"poseidon/pkg/util/context"

	"github.com/pkg/errors"
)

// nodeParameters computes node parameters before execution
func (sc *scheduler) nodeParameters(ctx context.Context, node api.NodeSpec, args interface{}) ([]interface{}, error) {
	// Map containing node results
	nodeResults := make(map[string]interface{})
	nodeResults[api.InputPipelineArgs] = args
	var batchDeps []api.InputDependency //contains nodes that are declared batch
	deps := node.GetInputDependencies()
	if len(deps) == 0 {
		// No input dependencies
		return []interface{}{node.Input}, nil
	}

	// Iterate over input dependencies in order to retrieve nodes' results
	// Also identify batch nodes
	for _, d := range deps {
		if d.Name != api.InputPipelineArgs {
			r, err := sc.s.NodeResult(ctx, ctx.ProcessID(), d.Name)
			if err != nil {
				return nil, errors.Wrapf(err, "cannot get result for node %s", d.Name)
			}
			nodeResults[d.Name] = r
		}
		if d.IsBatch {
			batchDeps = append(batchDeps, d)
		}
	}

	batchSize := 1
	if len(batchDeps) != 0 {
		if len(batchDeps) > 1 { // Remove this when multiple batch nodes is implemented
			return nil, errors.New("only one batch node is permitted, multiple batch nodes not yet implemented")
		}
		//TODO: implement multiple batch nodes, Check all batch nodes are arrays with the same result size
		asArray, isArray := maps.Get(nodeResults[batchDeps[0].Name], batchDeps[0].Path).([]interface{})
		if !isArray {
			return nil, errors.Errorf("node %s is flagged as batch but is not an array", batchDeps[0].Name)
		}
		batchSize = len(asArray)
	}

	parameters := make([]interface{}, batchSize)
	for i := 0; i < batchSize; i++ {
		parameters[i] = param(ctx, i, node.Input, nodeResults)
	}
	return parameters, nil
}

func param(ctx context.Context, index int, input interface{}, nodeResults map[string]interface{}) interface{} {
	switch reflect.TypeOf(input).Kind() {
	case reflect.String:
		return paramFromString(ctx, index, input.(string), nodeResults)
	case reflect.Map:
		// Map is map[string]interface{}
		m := input.(map[string]interface{})
		for k, v := range m {
			newVal := param(ctx, index, v, nodeResults)
			m[k] = newVal
		}
	case reflect.Array, reflect.Slice:
		var a []interface{}
		for _, i := range input.([]interface{}) {
			newVal := param(ctx, index, i, nodeResults)
			a = append(a, newVal)
		}
	}
	return input
}

func paramFromString(ctx context.Context, index int, input string, nodeResults map[string]interface{}) interface{} {
	deps := api.InputDependencies(input)

	if len(deps) == 1 && len(deps[0].OriginalString) == len(input) { // input string is the dependency, replace with resolved interface

		d := deps[0]
		value := resolveDependency(d, nodeResults)
		if d.IsBatch {
			return value.([]interface{})[index] //Checks done previously
		}
		return value
	}

	return api.InputDepRegexp.ReplaceAllStringFunc(input, func(s string) string {
		dep := api.AsInputDependency(s)
		value := resolveDependency(dep, nodeResults)
	})
	// deps := api.InputDependencies(input.(string))
	// if len(deps) == 1 { //whole string is a dependency
	// 	d := deps[0]
	// 	if d.IsBatch {
	// 		return nodeResults[d.Name].([]interface{})[index] //Checks done previously
	// 	}
	// 	return nodeResults[d.Name]
	// } else {

	// }
	// //TODO: consider dependency part of the string instead of the whole string
	// if asDep, isDep := api.AsInputDependency(input.(string)); isDep {
	// 	if asDep.IsBatch {
	// 		return nodeResults[asDep.Name].([]interface{})[index] //Checks done previously
	// 	}
	// 	return nodeResults[asDep.Name]
	// }
}

func resolveDependency(dep api.InputDependency, nodeResults map[string]interface{}) interface{} {
	if dep.Path != "" {
		return maps.Get(nodeResults[dep.Name], dep.Path)
	}
	return nodeResults[dep.Name]
}
