package pipeline

import (
	"poseidon/pkg/api"
	"poseidon/pkg/context"
	"poseidon/pkg/executor"
	"poseidon/pkg/store"
	"reflect"

	"github.com/pkg/errors"
)

// SetupFunc is the function called when a pipeline is submitted.
type SetupFunc func(ctx context.Context) error

// TearDownFunc is the function called when a pipeline is finished. (Either success or failure)
type TearDownFunc func(ctx context.Context) error

// Pipeline defines the entries of the pipeline engine.
type Pipeline interface {
	Executor() executor.Executor
	// Submit the pipeline defined by the given spec with the given arguments.
	Submit(ctx context.Context, spec api.PipelineSpec, args interface{}) error

	// Terminate terminates a running pipeline and set its status to TERMINATED.
	Terminate(ctx context.Context, reason string) error

	// Cancel cancels gracefully (or not) a running pipeline.
	Cancel(ctx context.Context, gracefully bool) error

	// NodeFinished is the function called when a node is finished. (Success or failure)
	NodeFinished(ctx context.Context, nodename string, status api.Status) error

	// Set function to be called when a pipeline is submitted.
	SetSetupFunc(SetupFunc)

	// Set function to be called when a pipeline is finished. (Either success or failure)
	SetTearDownFunc(TearDownFunc)

	// ListPipelines returns a list of the pipelines in the system. Information returned are processID and name
	ListPipelines(ctx context.Context) ([]api.PipelineInfo, error)

	PipelineState(ctx context.Context, pid string) (api.PipelineState, error)

	NodeState(ctx context.Context, pid, nodename string) (api.NodeState, error)
}

// New returns a new instance of Pipeline
func New(e executor.Executor, s store.PipelineStore) (Pipeline, error) {
	c := make(chan executor.NodeFinished)
	e.SetCallbackChan(c)
	p := pipelineEngine{
		s:    s,
		exec: e,
	}
	go func(pip Pipeline, ch chan executor.NodeFinished) {
		for {
			nf := <-ch
			ctx := context.Background()
			ctx = context.WithCorrelationID(ctx, nf.CorrelationID)
			ctx = context.WithProcessID(ctx, nf.ProcessID)
			ctx = context.WithNodeName(ctx, nf.Nodename)
			if err := pip.NodeFinished(ctx, nf.Nodename, nf.Status); err != nil {
				if err := pip.Terminate(ctx, err.Error()); err != nil {
					ctx.Logger().Error(errors.Wrapf(err, "cannot terminate pipeline %s"))
				}
			}
		}
	}(&p, c)

	return &p, nil
}

type pipelineEngine struct {
	s            store.PipelineStore
	exec         executor.Executor
	setupFunc    SetupFunc
	teardownFunc TearDownFunc
}

func (p *pipelineEngine) Executor() executor.Executor {
	return p.exec
}

func (p *pipelineEngine) Submit(ctx context.Context, spec api.PipelineSpec, args interface{}) error {
	ctx.Logger().Infof("starting pipeline %s", spec.Name)
	pid := ctx.ProcessID()
	// Call setup func
	if p.setupFunc != nil {
		if err := p.setupFunc(ctx); err != nil {
			return err
		}
	}

	// Create pipeline & nodes into store
	err := p.s.CreatePipeline(ctx, pid, spec, args)
	if err != nil {
		return errors.Wrapf(err, "cannot create pipeline %s", spec.Name)
	}
	nodes := make([]string, len(spec.Nodes))
	for i := range spec.Nodes {
		nodes[i] = spec.Nodes[i].Name
	}
	err = p.s.CreateNodes(ctx, pid, nodes)
	if err != nil {
		return errors.Wrapf(err, "cannot create nodes for pipeline %s", spec.Name)
	}

	if err := p.next(ctx); err != nil {
		return err
	}

	p.s.SetPipelineStatus(ctx, ctx.ProcessID(), api.StatusRunning)

	return nil
}

// next selects the next nodes to be submitted and submits them
func (p *pipelineEngine) next(ctx context.Context) error {
	pid := ctx.ProcessID()
	args, err := p.s.GetPipelineArgs(ctx, pid)
	if err != nil {
		return errors.Wrapf(err, "cannot get pipeline arguments")
	}
	spec, err := p.s.GetPipelineSpec(ctx, pid)
	if err != nil {
		return errors.Wrapf(err, "cannot get spec for pipeline %s", pid)
	}

	nodestatuses, err := p.s.GetNodeStatuses(ctx, pid)
	if err != nil {
		return errors.Wrapf(err, "cannot get nodes with status for pipeline %s", pid)
	}

	// Select the nodes to submit
	nodesToSubmit, err := selectNodesForSubmission(ctx, spec, nodestatuses)
	if err != nil {
		return errors.Wrapf(err, "cannot select nodes to submit")
	}
	if len(nodesToSubmit) == 0 { // No nodes to submit, check there is at least one running node, otherwise, the pipeline is stalled.
		hasRunningNode := false
		for _, s := range nodestatuses {
			if s == api.StatusRunning {
				hasRunningNode = true
				break
			}
		}
		if !hasRunningNode {
			return errors.Errorf("no node to submit")
		}
	}

	//Submit the nodes
	for _, n := range nodesToSubmit {
		params, err := p.nodeParameters(ctx, n, args)
		if err != nil {
			return errors.Wrapf(err, "cannot compute parameters for node %s", n.Name)
		}
		if err := p.exec.Start(context.WithNodeName(ctx, n.Name), n, params); err != nil {
			return errors.Wrapf(err, "cannot start node %s", n.Name)
		}
	}
	return nil
}

func (p *pipelineEngine) NodeFinished(ctx context.Context, nodename string, status api.Status) error {
	ctx.Logger().Infof("node %s finished with status %s", nodename, status)
	if status == api.StatusFailed {
		if err := p.stop(ctx, api.StatusFailed, false); err != nil {
			return errors.Wrap(err, "cannot stop process")
		}
	}
	finished, err := p.s.IsPipelineFinished(ctx, ctx.ProcessID())
	if err != nil {
		return errors.Wrap(err, "cannot determine if pipeline is finished")
	}
	if finished { // Pipeline finished
		if err := p.s.SetPipelineStatus(ctx, ctx.ProcessID(), status); err != nil {
			return errors.Wrapf(err, "cannot set status %s for pipeline", status)
		}
		ctx.Logger().Infof("pipeline finished with status %s", status)

		//Call teardown func if set
		if p.teardownFunc != nil {
			err := p.teardownFunc(ctx)
			if err != nil {
				return errors.Wrap(err, "error calling teardown function")
			}
		}
	} else {
		if err := p.next(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (p *pipelineEngine) Terminate(ctx context.Context, reason string) error {
	ctx.Logger().Infof("terminating process %s because %s", ctx.ProcessID(), reason)
	return p.stop(ctx, api.StatusTerminated, false)
}

func (p *pipelineEngine) Cancel(ctx context.Context, gracefully bool) error {
	ctx.Logger().Infof("cancelling process %s", ctx.ProcessID())
	return p.stop(ctx, api.StatusCancelled, gracefully)
}

func (p *pipelineEngine) SetSetupFunc(f SetupFunc) {
	p.setupFunc = f
}

func (p *pipelineEngine) SetTearDownFunc(f TearDownFunc) {
	p.teardownFunc = f
}

// stop stops the running pipeline and sets the given status (FAILED, CANCEL or TERMINATED)
// If status is FAILED, all running nodes are terminates.
// If gracefully is true, all running nodes finished normally
func (p *pipelineEngine) stop(ctx context.Context, status api.Status, gracefully bool) error {
	if gracefully {
		return errors.New("gracefull stop not yet implemented")
	}
	nodeStatus := api.StatusCancelled
	if status == api.StatusFailed {
		nodeStatus = api.StatusTerminated
	}
	statuses, err := p.s.GetNodeStatuses(ctx, ctx.ProcessID())
	if err != nil {
		return errors.Wrap(err, "cannot get nodes status")
	}
	for n, s := range statuses {
		if !s.Finished() {
			ctx = context.WithNodeName(ctx, n)
			p.exec.Stop(ctx, ctx.ProcessID(), n, nodeStatus, gracefully)
		}
	}
	return nil
}

func selectNodesForSubmission(ctx context.Context, spec api.PipelineSpec, nodestatuses map[string]api.Status) ([]api.NodeSpec, error) {
	var toSubmit []api.NodeSpec
	for _, s := range spec.Nodes {
		if !nodestatuses[s.Name].Finished() && nodestatuses[s.Name] != api.StatusRunning && nodeDepsCompleted(ctx, s, nodestatuses) {
			toSubmit = append(toSubmit, s)
		}
	}
	return toSubmit, nil
}

func nodeDepsCompleted(ctx context.Context, node api.NodeSpec, statuses map[string]api.Status) bool {
	for _, dep := range node.Dependencies {
		s, exist := statuses[dep]
		if !exist {
			ctx.Logger().Errorf("missing node %s for pipeline %s", dep, ctx.ProcessID())
			return false
		}
		if s != api.StatusCompleted {
			return false
		}
	}
	return true
}

func (p *pipelineEngine) nodeParameters(ctx context.Context, node api.NodeSpec, args interface{}) ([]interface{}, error) {
	// Map containing node results
	nodeResults := make(map[string]interface{})
	nodeResults[api.InputPipelineArgs] = args
	var batchNodes []string //contains nodes that are declared batch
	deps := node.InputDependencies()
	if len(deps) == 0 {
		// No input dependencies
		return []interface{}{node.Input}, nil
	}

	// Iterate over input dependencies in order to retrieve nodes' results
	// Also identify batch nodes
	for _, d := range deps {
		if d.Name != api.InputPipelineArgs {
			r, err := p.exec.NodeResult(ctx, ctx.ProcessID(), d.Name)
			if err != nil {
				return nil, errors.Wrapf(err, "cannot get result for node %s", d.Name)
			}
			nodeResults[d.Name] = r
		}
		if d.IsBatch {
			batchNodes = append(batchNodes, d.Name)
		}
	}

	batchSize := 1
	if len(batchNodes) != 0 {
		if len(batchNodes) > 1 { // Remove this when multiple batch nodes is implemented
			return nil, errors.New("only one batch node is permitted, multiple batch nodes not yet implemented")
		}
		//TODO: implement multiple batch nodes, Check all batch nodes are arrays with the same result size
		asArray, isArray := nodeResults[batchNodes[0]].([]interface{})
		if !isArray {
			return nil, errors.Errorf("node %s is flagged as batch but is not an array", batchNodes[0])
		}
		batchSize = len(asArray)
	}

	parameters := make([]interface{}, batchSize)
	for i := 0; i < batchSize; i++ {
		p := param(ctx, i, node.Input, nodeResults)
		parameters[i] = p
	}
	return parameters, nil
}

func param(ctx context.Context, index int, input interface{}, nodeResults map[string]interface{}) interface{} {
	switch reflect.TypeOf(input).Kind() {
	case reflect.String:
		//TODO: consider dependency part of the string instead of the whole string
		if asDep, isDep := api.AsInputDependency(input.(string)); isDep {
			if asDep.IsBatch {
				return nodeResults[asDep.Name].([]interface{})[index] //Checks done previously
			}
			return nodeResults[asDep.Name]
		}
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
	default:
		return input
	}
	return input
}

func (p *pipelineEngine) ListPipelines(ctx context.Context) ([]api.PipelineInfo, error) {
	pipelines, err := p.s.ListPipelines(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "cannot list pipelines")
	}
	var res []api.PipelineInfo
	for processID, name := range pipelines {
		res = append(res, api.PipelineInfo{
			ProcessID: processID,
			Name:      name,
		})
	}
	return res, nil
}

func (p *pipelineEngine) PipelineState(ctx context.Context, pid string) (api.PipelineState, error) {
	return p.s.PipelineState(ctx, pid)
}

func (p *pipelineEngine) NodeState(ctx context.Context, pid, nodename string) (api.NodeState, error) {
	return p.exec.NodeState(ctx, pid, nodename)
}
