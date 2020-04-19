package executor

import (
	"poseidon/pkg/api"
	"poseidon/pkg/context"
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
	SetCallbackChan(chan NodeFinished)
}
