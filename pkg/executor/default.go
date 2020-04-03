package executor

import (
	"fmt"
	"poseidon/pkg/api"
	"poseidon/pkg/broker"
	"poseidon/pkg/context"
	"poseidon/pkg/events"
	"poseidon/pkg/executor/workload"
	"poseidon/pkg/store"
	"strconv"

	"github.com/pkg/errors"
)

type exec struct {
	w            workload.Workload
	s            store.ExecutorStore
	callback     chan NodeFinished
	b            broker.Broker
	publishQName string
}

func (e *exec) Start(ctx context.Context, spec api.NodeSpec, params []interface{}) error {
	ctx.Logger().Infof("starting node %s", spec.Name)
	// Create jobs
	if err := e.s.CreateJobs(ctx, ctx.ProcessID(), spec.Name, params); err != nil {
		return errors.Wrapf(err, "cannot create jobs for node %s", spec.Name)
	}

	// Create processing queue
	if err := e.b.CreateQueue(ctx, qname(ctx), e.publishQName); err != nil {
		return errors.Wrapf(err, "cannot create processing queue %s for node %s", qname(ctx), spec.Name)
	}

	//Schedule workload
	if err := e.w.Schedule(ctx, spec, e.b.Config().ToEnv(), len(params)); err != nil {
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
		if err := e.b.Publish(ctx, evt, e.publishQName, ctx.ProcessID()); err != nil {
			return errors.Wrapf(err, "cannot publish %s event for node %s and jobID %s", string(events.TypeSubmit), spec.Name, jobID)
		}
	}
	e.s.SetNodeStatus(ctx, ctx.ProcessID(), spec.Name, api.StatusSubmitted)
	return nil
}

func (e *exec) Stop(ctx context.Context, pid, nodename string, status api.Status, gracefully bool) error {
	if gracefully {
		return errors.New("graceful stop not yet implemented")
	}

	// Stop jobs in store
	if err := e.s.StopJobs(ctx, pid, nodename, status); err != nil {
		return errors.Wrapf(err, "cannot stop jobs for node %s", nodename)
	}

	//Delete the process queue
	if err := e.b.DeleteQueue(ctx, qname(ctx)); err != nil {
		return errors.Wrapf(err, "cannot delete running jobs for node %s", nodename)
	}

	// Delete the workload
	if err := e.w.Delete(ctx, nodename); err != nil {
		return errors.Wrapf(err, "cannot stop workload for node %s", nodename)
	}

	if err := e.s.SetNodeStatus(ctx, pid, nodename, status); err != nil {
		return errors.Wrapf(err, "cannot set status %s to node %s", status, nodename)
	}
	return nil
}

func (e *exec) NodeState(ctx context.Context, pid, nodename string) (api.NodeState, error) {
	return e.s.NodeState(ctx, pid, nodename)
}

func (e *exec) NodeResult(ctx context.Context, pid, nodename string) (interface{}, error) {
	return e.s.NodeResult(ctx, pid, nodename)
}

func (e *exec) JobState(ctx context.Context, pid, nodename, jobid string) (api.JobState, error) {
	return e.s.JobState(ctx, pid, nodename, jobid)
}

func (e *exec) JobResult(ctx context.Context, pid, nodename, jobid string) (interface{}, error) {
	return e.s.JobResult(ctx, pid, nodename, jobid)
}

func (e *exec) SetCallbackChan(c chan NodeFinished) {
	e.callback = c
}

func (e *exec) HandleEvent(ctx context.Context, evt events.Event) error {
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

	js, err := e.s.GetJobStatus(ctx, evt.ProcessID, evt.NodeName, evt.JobID)
	if err != nil {
		return errors.Wrapf(err, "cannot get status for job %s of node %s", evt.JobID, evt.NodeName)
	}
	if js.Finished() { //Job already has a finished status, skip event
		ctx.Logger().Tracef("job already in status %s, ignoring event", js)
		return nil
	}

	if err := e.s.SetJobStatus(ctx, evt.ProcessID, evt.NodeName, evt.JobID, status, evt.Time, payload); err != nil {
		return errors.Wrapf(err, "cannot set status %s for job %s of node %s", status, evt.JobID, evt.NodeName)
	}
	if status == api.StatusRunning {
		// Set Node Running if not already running
		if err := e.s.SetNodeRunning(ctx, ctx.ProcessID(), evt.NodeName); err != nil {
			return errors.Wrapf(err, "cannot set status %s for node %s", status, evt.NodeName)
		}
	} else if status.Finished() { // If status is final, check if the node is finished as well
		s, err := e.computeNodeStatus(ctx, ctx.ProcessID(), evt.NodeName)
		if err != nil {
			return errors.Wrap(err, "cannot compute node status")
		}

		if s.Finished() {
			if err := e.Stop(ctx, ctx.ProcessID(), evt.NodeName, s, false); err != nil {
				return err
			}

			if e.callback != nil {
				e.callback <- NodeFinished{
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
func (e *exec) computeNodeStatus(ctx context.Context, pid, nodename string) (api.Status, error) {
	statuses, err := e.s.GetJobsStatuses(ctx, pid, nodename)
	if err != nil {
		return "", errors.Wrapf(err, "cannot get status for jobs of node %s", nodename)
	}
	spec, err := e.s.GetNodeSpec(ctx, pid, nodename)
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
