package scheduler

import (
	"poseidon/pkg/api"
	"poseidon/pkg/executor"
	"poseidon/pkg/store"
	"poseidon/pkg/util/context"
	"poseidon/pkg/util/template"
	"strings"

	"github.com/pkg/errors"
)

// SetupFunc is the function called when a pipeline is submitted.
type SetupFunc func(ctx context.Context) error

// TearDownFunc is the function called when a pipeline is finished. (Either success or failure)
type TearDownFunc func(ctx context.Context) error

// Scheduler defines the entries of the pipeline engine.
type Scheduler interface {
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
}

// NewScheduler returns a new instance of Pipeline scheduler
func NewScheduler(exec map[string]executor.Executor, s store.Store) (Scheduler, error) {
	c := make(chan executor.NodeFinished)
	for _, e := range exec {
		e.SetCallbackChan(c)
	}
	p := scheduler{
		s:    s,
		exec: exec,
	}

	// The following go routine handles the NodeFinished events sent by the executors
	go func(sc Scheduler, ch chan executor.NodeFinished) {
		for {
			nf := <-ch
			ctx := context.Background()
			ctx = context.WithCorrelationID(ctx, nf.CorrelationID)
			ctx = context.WithProcessID(ctx, nf.ProcessID)
			ctx = context.WithNodeName(ctx, nf.Nodename)
			if err := sc.NodeFinished(ctx, nf.Nodename, nf.Status); err != nil {
				if err := sc.Terminate(ctx, err.Error()); err != nil {
					ctx.Logger().Error(errors.Wrapf(err, "cannot terminate pipeline %s", nf.ProcessID))
				}
			}
		}
	}(&p, c)

	return &p, nil
}

type scheduler struct {
	s            store.Store
	exec         map[string]executor.Executor
	setupFunc    SetupFunc
	teardownFunc TearDownFunc
}

func (sc *scheduler) Submit(ctx context.Context, spec api.PipelineSpec, args interface{}) error {
	ctx.Logger().Infof("starting pipeline %s", spec.Name)
	pid := ctx.ProcessID()
	// Call setup func
	if sc.setupFunc != nil {
		if err := sc.setupFunc(ctx); err != nil {
			return err
		}
	}

	// Create pipeline & nodes into store
	err := sc.s.CreatePipeline(ctx, pid, spec, args)
	if err != nil {
		return errors.Wrapf(err, "cannot create pipeline %s", spec.Name)
	}
	nodes := make([]string, len(spec.Nodes))
	for i := range spec.Nodes {
		nodes[i] = spec.Nodes[i].Name
	}
	err = sc.s.CreateNodes(ctx, pid, nodes)
	if err != nil {
		return errors.Wrapf(err, "cannot create nodes for pipeline %s", spec.Name)
	}

	if err := sc.next(ctx); err != nil {
		return err
	}

	sc.s.SetPipelineStatus(ctx, ctx.ProcessID(), api.StatusRunning)

	return nil
}

// next selects the next nodes to be submitted and submits them
func (sc *scheduler) next(ctx context.Context) error {
	pid := ctx.ProcessID()
	args, err := sc.s.GetPipelineArgs(ctx, pid)
	if err != nil {
		return errors.Wrapf(err, "cannot get pipeline arguments")
	}
	spec, err := sc.s.GetPipelineSpec(ctx, pid)
	if err != nil {
		return errors.Wrapf(err, "cannot get spec for pipeline %s", pid)
	}

	nodestatuses, err := sc.s.GetNodeStatuses(ctx, pid)
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
		params, err := sc.nodeParameters(ctx, n, args)
		if err != nil {
			return errors.Wrapf(err, "cannot compute parameters for node %s", n.Name)
		}

		exec, ok := sc.exec[n.Kind]
		if !ok {
			return errors.Errorf("unknown executor %s in node %s", n.Kind, n.Name)
		}
		if err := exec.Start(context.WithNodeName(ctx, n.Name), n.ExecutorSpec, params); err != nil {
			return errors.Wrapf(err, "cannot start node %s", n.Name)
		}
	}
	return nil
}

func (sc *scheduler) NodeFinished(ctx context.Context, nodename string, status api.Status) error {
	ctx.Logger().Infof("node %s finished with status %s", nodename, status)
	if status == api.StatusFailed {
		if err := sc.stop(ctx, api.StatusFailed, false); err != nil {
			return errors.Wrap(err, "cannot stop process")
		}
	}
	finished, err := sc.s.IsPipelineFinished(ctx, ctx.ProcessID())
	if err != nil {
		return errors.Wrap(err, "cannot determine if pipeline is finished")
	}
	if finished { // Pipeline finished
		if err := sc.s.SetPipelineStatus(ctx, ctx.ProcessID(), status); err != nil {
			return errors.Wrapf(err, "cannot set status %s for pipeline", status)
		}
		ctx.Logger().Infof("pipeline finished with status %s", status)

		//Call teardown func if set
		if sc.teardownFunc != nil {
			err := sc.teardownFunc(ctx)
			if err != nil {
				return errors.Wrap(err, "error calling teardown function")
			}
		}
	} else {
		if err := sc.next(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (sc *scheduler) Terminate(ctx context.Context, reason string) error {
	ctx.Logger().Infof("terminating process %s because %s", ctx.ProcessID(), reason)
	return sc.stop(ctx, api.StatusTerminated, false)
}

func (sc *scheduler) Cancel(ctx context.Context, gracefully bool) error {
	ctx.Logger().Infof("cancelling process %s", ctx.ProcessID())
	return sc.stop(ctx, api.StatusCancelled, gracefully)
}

func (sc *scheduler) SetSetupFunc(f SetupFunc) {
	sc.setupFunc = f
}

func (sc *scheduler) SetTearDownFunc(f TearDownFunc) {
	sc.teardownFunc = f
}

// stop stops the running pipeline and sets the given status (FAILED, CANCEL or TERMINATED)
// If status is FAILED, all running nodes are terminates.
// If gracefully is true, all running nodes finished normally
func (sc *scheduler) stop(ctx context.Context, status api.Status, gracefully bool) error {
	if gracefully {
		return errors.New("gracefull stop not yet implemented")
	}
	spec, err := sc.s.GetPipelineSpec(ctx, ctx.ProcessID())
	if err != nil {
		return errors.Wrap(err, "cannot get pipeline spec")
	}
	nodeStatus := api.StatusCancelled
	if status == api.StatusFailed {
		nodeStatus = api.StatusTerminated
	}
	statuses, err := sc.s.GetNodeStatuses(ctx, ctx.ProcessID())
	if err != nil {
		return errors.Wrap(err, "cannot get nodes status")
	}

	for _, s := range spec.Nodes {
		if !statuses[s.Name].Finished() {
			ctx = context.WithNodeName(ctx, s.Name)
			exec, ok := sc.exec[s.Kind]
			if !ok {
				return errors.Errorf("unknown executor %s in node %s", s.Kind, s.Name)
			}
			exec.Stop(ctx, ctx.ProcessID(), s.Name, nodeStatus, gracefully)
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

func (sc *scheduler) nodeParameters(ctx context.Context, node api.NodeSpec, args interface{}) ([]interface{}, error) {
	// Map containing node results
	nodeResults := make(map[string]interface{})
	nodeResults[api.InputPipelineArgs] = args
	tpl := template.New(node.Input)
	for _, expr := range tpl.FindAll() {
		nodename := strings.Split(expr.Text, ".")[0]
		if nodename != api.InputPipelineArgs {
			r, err := sc.s.NodeResult(ctx, ctx.ProcessID(), nodename)
			if err != nil {
				return nil, errors.Wrapf(err, "cannot get result for node %s", nodename)
			}
			nodeResults[nodename] = r
		}
	}
	resolved, err := tpl.Resolve(template.ResolveWithMap(nodeResults))
	if err != nil {
		return nil, errors.Wrapf(err, "cannot compute parameter for node %s", node.Name)
	}
	if asArray, isArray := resolved.([]interface{}); isArray {
		return asArray, nil
	}
	return []interface{}{resolved}, nil
}
