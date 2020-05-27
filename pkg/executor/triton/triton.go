package triton

import (
	"fmt"
	"os"

	"poseidon/pkg/api"
	"poseidon/pkg/broker"
	"poseidon/pkg/broker/events"
	"poseidon/pkg/executor"
	"poseidon/pkg/executor/triton/workload"
	"poseidon/pkg/util/context"
	"poseidon/pkg/util/maps"

	"github.com/pkg/errors"
)

type TritonSpec struct {
	workload.WorkloadSpec `mapstructure:",squash"`
	ContinueOnFail        bool `json:"cof"`
}

type triton struct {
	workload     workload.Workload
	broker       broker.Broker
	publishQName string
	callback     executor.CallbackFunc
}

// New returns a new triton executor with the given components
func New(ctx context.Context, broker broker.Broker, publishQName, receiveQName string, workload workload.Workload) (executor.Executor, error) {

	t := &triton{
		broker:       broker,
		publishQName: publishQName,
		workload:     workload,
	}
	go func() {
		if err := broker.Receive(ctx, t.handleEvent, nil, receiveQName); err != nil {
			ctx.Logger().Fatal(err)
			os.Exit(1)
		}
	}()

	return t, nil
}

func (t *triton) Start(ctx context.Context, spec interface{}, n int) error {
	ctx.Logger().Infof("starting node %s", ctx.NodeName())
	var s TritonSpec
	if err := maps.Decode(spec, &s); err != nil {
		return errors.Wrap(err, "cannot decode triton executor spec")
	}

	// Create processing queue
	if err := t.broker.CreateQueue(ctx, qname(ctx), t.publishQName); err != nil {
		return errors.Wrapf(err, "cannot create processing queue %s for node %s", qname(ctx), ctx.NodeName())
	}

	//Schedule workload
	if err := t.workload.Schedule(ctx, s.WorkloadSpec, n); err != nil {
		return errors.Wrapf(err, "cannot schedule workload for node %s", ctx.NodeName())
	}
	return nil
}

func (t *triton) SubmitJob(ctx context.Context, jobID string, param interface{}) error {
	evt := events.Event{
		Type:          events.TypeSubmit,
		ProcessID:     ctx.ProcessID(),
		JobID:         jobID,
		TaskID:        ctx.TaskID(),
		Data:          param,
		CorrelationID: ctx.CorrelationID(),
	}
	if err := t.broker.Publish(ctx, evt, t.publishQName, ctx.ProcessID()); err != nil {
		return errors.Wrapf(err, "cannot publish %s event for node %s and jobID %s", string(events.TypeSubmit), ctx.NodeName(), jobID)
	}
	return nil
}

func (t *triton) Stop(ctx context.Context, gracefully bool) error {
	if gracefully {
		return errors.New("graceful stop not yet implemented")
	}

	//Delete the process queue
	if err := t.broker.DeleteQueue(ctx, qname(ctx)); err != nil {
		return errors.Wrap(err, "cannot delete running jobs")
	}

	// Delete the workload
	if err := t.workload.Delete(ctx, ctx.TaskID()); err != nil {
		return errors.Wrap(err, "cannot stop workload")
	}

	return nil
}

func (t *triton) handleEvent(ctx context.Context, evt events.Event) error {
	ctx.Logger().Tracef("receiving %s event for task %s and job %s", evt.Type, evt.TaskID, evt.JobID)
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

	if err := t.callback(ctx, payload, status); err != nil {
		return errors.Wrapf(err, "cannot call callback func for job %s for task %s", ctx.JobID(), ctx.TaskID())
	}
	return nil
}

func (t *triton) SetCallbackFunc(f executor.CallbackFunc) {
	t.callback = f
}

// qname compute a name for the queue for the node execution.
func qname(ctx context.Context) string {
	return fmt.Sprintf("q_%s_%s", ctx.ProcessID(), ctx.NodeName())
}
