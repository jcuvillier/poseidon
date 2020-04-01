package executor

import (
	"poseidon/pkg/api"
	"poseidon/pkg/broker"
	"poseidon/pkg/context"
	"poseidon/pkg/events"
	"poseidon/pkg/executor/workload"
	"poseidon/pkg/store"

	"github.com/pkg/errors"
)

// CallbackFunc is the function called when the node is done (either successfully or with error)
type CallbackFunc func(ctx context.Context, nodename string, s api.Status) error

// NodeFinished is the object returned into the callback chan when a node is finished
type NodeFinished struct {
	CorrelationID string
	ProcessID     string
	Nodename      string
	Status        api.Status
}

// Executor is the mechanism used to run nodes.
type Executor interface {
	Start(ctx context.Context, spec api.NodeSpec, params []interface{}) error
	Stop(ctx context.Context, pid, nodename string, status api.Status, gracefully bool) error
	NodeState(ctx context.Context, pid, nodename string) (api.NodeState, error)
	NodeResult(ctx context.Context, pid, nodename string) (interface{}, error)
	JobState(ctx context.Context, pid, nodename, jobid string) (api.JobState, error)
	JobResult(ctx context.Context, pid, nodename, jobid string) (interface{}, error)
	SetCallbackChan(chan NodeFinished)
	HandleEvent(ctx context.Context, evt events.Event) error
}

// New returns a new instance of Executor
func New(b broker.Broker, publishQName string, s store.ExecutorStore) (Executor, error) {
	w, err := workload.New()
	if err != nil {
		return nil, errors.Wrap(err, "cannot create new workload instance")
	}
	return &exec{
		s:            s,
		w:            w,
		b:            b,
		publishQName: publishQName,
	}, nil
}
