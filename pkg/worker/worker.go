package worker

import (
	"encoding/json"
	"fmt"
	"os"
	"poseidon/pkg/broker"
	"poseidon/pkg/broker/events"
	"poseidon/pkg/util/context"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

const (
	EnvProcessID    = "PROCESS_ID"
	EnvTaskID       = "TASK_ID"
	EnvPublishQName = "PUBLISH_QUEUE"
	envSandbox      = "SANDBOX"
)

// Func is the function to be executed by the worker for each received SUBMIT event.
type Func func(ctx context.Context, request interface{}) (response interface{}, err error)

// Start starts receiving event and execute the given function
func Start(f Func) {
	ctx := context.Background()

	startFunc := start
	//Check sandbox mode
	s := os.Getenv(envSandbox)
	if s == "true" {
		ctx.Logger().Info("SANDBOX mode activated")
		startFunc = sandbox
	}

	if err := startFunc(ctx, f); err != nil {
		ctx.Logger().Fatal(err)
		os.Exit(1)
	}
}

// start starts the worker
func start(ctx context.Context, f Func) error {
	processID := os.Getenv(EnvProcessID)
	if processID == "" {
		return errors.Errorf("missing env %s", EnvProcessID)
	}
	taskID := os.Getenv(EnvTaskID)
	if taskID == "" {
		return errors.Errorf("missing env %s", EnvTaskID)
	}
	publishQName := os.Getenv(EnvPublishQName)
	if publishQName == "" {
		return errors.Errorf("missing env %s", EnvPublishQName)
	}

	ctx.Logger().Infof("starting worker for process %s and task %s", processID, taskID)

	q, err := broker.NewFromEnv(ctx)
	if err != nil {
		return errors.Wrapf(err, "cannot create new broker")
	}

	return q.Receive(ctx, wrap(q, publishQName, f), nil, fmt.Sprintf("q_%s_%s", processID, taskID), filterEvent(taskID))
}

// filterEvent rejects events with taskID different from the given one.
func filterEvent(taskID string) broker.ReceiveOption {
	return func(ctx context.Context, evt *events.Event) error {
		// Filter out non SUBMIT event
		if evt.Type != events.TypeSubmit {
			return errors.Errorf("event is not type %s", events.TypeSubmit)
		}
		return nil
	}
}

// wrap calls f and send events to the given broker
func wrap(b broker.Broker, qname string, f Func) broker.HandleFunc {
	return func(ctx context.Context, evt events.Event) error {
		// Generate executionID
		ctx = context.WithExecutionID(ctx, uuid.New().String())

		// Send RUN event
		runEvt := newEvent(ctx, events.TypeRun, events.RunEventData{ExecutionID: ctx.ExecutionID()})
		err := b.Publish(ctx, runEvt, qname, "")
		if err != nil {
			return errors.Wrapf(err, "cannot publish RUN event %s", runEvt)
		}

		// Run WorkerFunc
		result, err := f(ctx, evt.Data)

		var e events.Event
		if err != nil {
			//TODO: Check if retryable error
			e = newEvent(ctx, events.TypeError, events.ErrorEventData{Message: err.Error()})
		} else {
			e = newEvent(ctx, events.TypeSuccess, result)
		}
		// Send event
		err = b.Publish(ctx, e, qname, "")
		if err != nil {
			return errors.Wrapf(err, "cannot publish event %s", e)
		}
		return nil
	}
}

// newEvent returns a new events.Event from the given context
func newEvent(ctx context.Context, typ events.EventType, payload interface{}) events.Event {
	return events.Event{
		Type:          typ,
		CorrelationID: ctx.CorrelationID(),
		JobID:         ctx.JobID(),
		TaskID:        ctx.NodeName(),
		ProcessID:     ctx.ProcessID(),
		Data:          payload,
		Time:          time.Now(),
	}
}

func print(ctx context.Context, evt events.Event) {
	if evt.Type == events.TypeError {
		fmt.Println("ERROR")
	} else {
		fmt.Println("SUCCESS")
	}
	enc := json.NewEncoder(os.Stdout)
	err := enc.Encode(evt.Data)
	if err != nil {
		ctx.Logger().Error(err)
	}
}
