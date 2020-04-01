package broker

import (
	"os"
	"strings"
	"sync"

	"poseidon/pkg/context"
	"poseidon/pkg/events"

	"github.com/pkg/errors"
)

const (
	envBrokerType = "BROKER_TYPE"
)

var (
	b         Broker
	mutex     = &sync.Mutex{}
	factories map[string]factory
)

func init() {
	factories = make(map[string]factory)
	factories[rabbitmqType] = rabbitmqFactory{}
}

// HandleFunc is the function called when an event is received from the receive queue.
type HandleFunc func(ctx context.Context, evt events.Event) error

// ErrorHandler is the function called when the HandleFunc returns an error.
type ErrorHandler func(ctx context.Context, err error)

// Broker ...
type Broker interface {
	// Publish publishes the given event to the publish queue.
	Publish(ctx context.Context, evt events.Event, qname, routingkey string) error

	// Receive consumes events from the receive queue.
	// f is called for each event received.
	// this is a blocking function, should be called in a goroutine.
	Receive(ctx context.Context, f HandleFunc, ferr ErrorHandler, qname string, options ...ReceiveOption) error

	// CreateQueue creates a new queue connected to the bindTo queue.
	CreateQueue(ctx context.Context, name, bindTo string) error

	// DeleteQueue deletes the queue designated by the given name.
	DeleteQueue(ctx context.Context, name string) error

	Config() Config

	// Close closes all connections.
	Close() error
}

// factory is an internal interface as factory help for instantiating Broker.
type factory interface {
	newFromConfig(ctx context.Context, config interface{}) (Broker, error)
	newFromEnv(ctx context.Context) (Broker, error)
}

// NewFromConfig returns a new Broker instance based on configuration.
func NewFromConfig(ctx context.Context) (Broker, error) {
	return nil, errors.New("func NewFromConfig not yet implemented")
}

// NewFromEnv returns a new Broker instance based on environment.
func NewFromEnv(ctx context.Context) (Broker, error) {
	typ := os.Getenv(envBrokerType)
	if typ == "" {
		return nil, errors.Errorf("missing env %s", envBrokerType)
	}

	t := strings.ToUpper(typ)
	f, exist := factories[t]
	if !exist {
		return nil, errors.Errorf("unknown borker type %s", typ)
	}
	return f.newFromEnv(ctx)
}

// ReceiveOption is an option function for the Broker.Receive function
// If this function returns an error, the event is negatively acknowledge.
type ReceiveOption func(context.Context, *events.Event) error

// Config holds all the connection information necessary to instanciate a new broker.
type Config interface {
	// ToEnv exports all the necessary env variables necessary to instanciate a new broker.
	ToEnv() map[string]string
}
