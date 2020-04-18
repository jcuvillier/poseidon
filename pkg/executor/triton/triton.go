package triton

import (
	"fmt"
	"poseidon/pkg/api"
	"poseidon/pkg/broker"
	"poseidon/pkg/context"
	"poseidon/pkg/events"
	"poseidon/pkg/executor"
	"poseidon/pkg/executor/triton/workload"
	"poseidon/pkg/store"
	"strconv"

	"github.com/pkg/errors"
)

type triton struct {
	workload     workload.Workload
	store        store.ExecutorStore
	callback     chan executor.NodeFinished
	broker       broker.Broker
	publishQName string
}

// New returns a new triton executor with the given components
func New(broker broker.Broker, publishQName string, store store.ExecutorStore, workload workload.Workload) (executor.Executor, error) {
	return &triton{
		broker:       broker,
		publishQName: publishQName,
		store:        store,
		workload:     workload,
	}, nil
}

func (t *triton) Start(ctx context.Context, spec api.NodeSpec, params []interface{}) error {
	ctx.Logger().Infof("starting node %s", spec.Name)
	// Create jobs
	if err := t.store.CreateJobs(ctx, ctx.ProcessID(), spec.Name, params); err != nil {
		return errors.Wrapf(err, "cannot create jobs for node %s", spec.Name)
	}

	// Create processing queue
	if err := t.broker.CreateQueue(ctx, qname(ctx), t.publishQName); err != nil {
		return errors.Wrapf(err, "cannot create processing queue %s for node %s", qname(ctx), spec.Name)
	}

	//Schedule workload
	if err := t.workload.Schedule(ctx, spec, len(params)); err != nil {
		return errors.Wrapf(err, "cannot schedule workload for node %s", spec.Name)
	}

	// Send work order as events
	for i, p := range params {
		jobID := strconv.Itoa(i)
		evt := events.Event{
			Type:          events.TypeSubmit,
			ProcessID:     ctx.ProcessID(),
			JobID:         jobID,
			NodeName:      spec.Name,
			Data:          p,
			CorrelationID: ctx.CorrelationID(),
			ExecutionID:   "abc",
		}
		if err := t.broker.Publish(ctx, evt, t.publishQName, ctx.ProcessID()); err != nil {
			return errors.Wrapf(err, "cannot publish %s event for node %s and jobID %s", string(events.TypeSubmit), spec.Name, jobID)
		}
	}
	t.store.SetNodeStatus(ctx, ctx.ProcessID(), spec.Name, api.StatusSubmitted)
	return nil
}

func (t *triton) Stop(ctx context.Context, pid, nodename string, status api.Status, gracefully bool) error {
	if gracefully {
		return errors.New("graceful stop not yet implemented")
	}

	// Stop jobs in store
	if err := t.store.StopJobs(ctx, pid, nodename, status); err != nil {
		return errors.Wrapf(err, "cannot stop jobs for node %s", nodename)
	}

	//Delete the process queue
	if err := t.broker.DeleteQueue(ctx, qname(ctx)); err != nil {
		return errors.Wrapf(err, "cannot delete running jobs for node %s", nodename)
	}

	// Delete the workload
	if err := t.workload.Delete(ctx, nodename); err != nil {
		return errors.Wrapf(err, "cannot stop workload for node %s", nodename)
	}

	if err := t.store.SetNodeStatus(ctx, pid, nodename, status); err != nil {
		return errors.Wrapf(err, "cannot set status %s to node %s", status, nodename)
	}
	return nil
}

func (t *triton) NodeState(ctx context.Context, pid, nodename string) (api.NodeState, error) {
	return t.store.NodeState(ctx, pid, nodename)
}

func (t *triton) NodeResult(ctx context.Context, pid, nodename string) (interface{}, error) {
	return t.store.NodeResult(ctx, pid, nodename)
}

func (t *triton) JobState(ctx context.Context, pid, nodename, jobid string) (api.JobState, error) {
	return t.store.JobState(ctx, pid, nodename, jobid)
}

func (t *triton) JobResult(ctx context.Context, pid, nodename, jobid string) (interface{}, error) {
	return t.store.JobResult(ctx, pid, nodename, jobid)
}

func (t *triton) SetCallbackChan(c chan executor.NodeFinished) {
	t.callback = c
}

func (t *triton) HandleEvent(ctx context.Context, evt events.Event) error {
	ctx.Logger().Tracef("receiving %s event for node %s and job %s", evt.Type, evt.NodeName, evt.JobID)
	var status api.Status
	var payload interface{}
	switch evt.Type {
	case events.TypeRun:
		status = api.StatusRunning
		payload = nil
	case events.TypeSuccess:
		status = api.StatusCompleted
		payload = evt.Data
	case events.TypeError:
		status = api.StatusFailed
		payload = evt.Data
	default:
		//Ignoring other event types
		return nil
	}

	js, err := t.store.GetJobStatus(ctx, evt.ProcessID, evt.NodeName, evt.JobID)
	if err != nil {
		return errors.Wrapf(err, "cannot get status for job %s of node %s", evt.JobID, evt.NodeName)
	}
	if js.Finished() { //Job already has a finished status, skip event
		ctx.Logger().Tracef("job already in status %s, ignoring event", js)
		return nil
	}

	if err := t.store.SetJobStatus(ctx, evt.ProcessID, evt.NodeName, evt.JobID, status, evt.Time, payload); err != nil {
		return errors.Wrapf(err, "cannot set status %s for job %s of node %s", status, evt.JobID, evt.NodeName)
	}
	if status == api.StatusRunning {
		// Set Node Running if not already running
		if err := t.store.SetNodeRunning(ctx, ctx.ProcessID(), evt.NodeName); err != nil {
			return errors.Wrapf(err, "cannot set status %s for node %s", status, evt.NodeName)
		}
	} else if status.Finished() { // If status is final, check if the node is finished as well
		s, err := t.computeNodeStatus(ctx, ctx.ProcessID(), evt.NodeName)
		if err != nil {
			return errors.Wrap(err, "cannot compute node status")
		}

		if s.Finished() {
			if err := t.Stop(ctx, ctx.ProcessID(), evt.NodeName, s, false); err != nil {
				return err
			}

			if t.callback != nil {
				t.callback <- executor.NodeFinished{
					CorrelationID: evt.CorrelationID,
					ProcessID:     evt.ProcessID,
					Nodename:      evt.NodeName,
					Status:        status,
				}
			}
		}
	}
	return nil
}

// computeNodeStatus computes a node status from the its job statuses
func (t *triton) computeNodeStatus(ctx context.Context, pid, nodename string) (api.Status, error) {
	statuses, err := t.store.GetJobsStatuses(ctx, pid, nodename)
	if err != nil {
		return "", errors.Wrapf(err, "cannot get status for jobs of node %s", nodename)
	}
	spec, err := t.store.GetNodeSpec(ctx, pid, nodename)
	if err != nil {
		return "", errors.Wrapf(err, "cannot get specification of node %s", nodename)
	}
	hasError := false
	hasNotFinished := false
	for _, s := range statuses {
		switch s {
		case api.StatusFailed:
			hasError = true
		case api.StatusCreated, api.StatusSubmitted, api.StatusRunning:
			hasNotFinished = true
		}
	}

	if hasError && !spec.ContinueOnFail {
		return api.StatusFailed, nil
	}
	if hasNotFinished {
		return api.StatusRunning, nil
	}
	return api.StatusCompleted, nil
}

// qname compute a name for the queue for the node execution.
func qname(ctx context.Context) string {
	return fmt.Sprintf("q_%s_%s", ctx.ProcessID(), ctx.NodeName())
}
