package broker

import (
	"os"
	"strings"
	"sync"

	"poseidon/pkg/broker/events"
	"poseidon/pkg/util/config"
	"poseidon/pkg/util/context"

	"github.com/pkg/errors"
)

const (
	envBrokerType = "BROKER_TYPE"
)

var (
	factories = make(map[Type]func(context.Context, interface{}) (Broker, error))
	configs   = make(map[Type]interface{})
	mutex     = &sync.Mutex{}
)

func register(t Type, f func(context.Context, interface{}) (Broker, error), c interface{}) {
	factories[t] = f
	configs[t] = c
}

// Type is a string designing the implementation of Broker interface
type Type string

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

	// Close closes all connections.
	Close() error
}

// ReceiveOption is an option function for the Broker.Receive function
// If this function returns an error, the event is negatively acknowledge.
type ReceiveOption func(context.Context, *events.Event) error

// NewFromConfig returns a new instance of Broker based on configuration from config file and/or env variables
func NewFromConfig(ctx context.Context, configKey string) (Broker, error) {
	configTypeKey := configKey + ".type"
	// Get broker type
	var t string
	if typ := config.Get(configTypeKey); typ != nil {
		asString, isString := typ.(string)
		if !isString {
			return nil, errors.Errorf("config entry with key %s is not a string", configTypeKey)
		}
		t = asString
	} else {
		t = os.Getenv(envBrokerType)
	}
	if t == "" {
		return nil, errors.Errorf("broker type could not be found neither in config with key %s nor env %s", configTypeKey, envBrokerType)
	}

	typ := Type(strings.ToLower(t))
	v, ok := configs[typ]
	if !ok {
		return nil, errors.Errorf("unknown broker type %s", typ)
	}
	if err := config.Unmarshal(configKey, v); err != nil {
		return nil, errors.Wrap(err, "cannot unmarshal broker config")
	}

	return New(ctx, typ, v)
}

// NewFromEnv returns a new instance of Broker based on env variables
func NewFromEnv(ctx context.Context) (Broker, error) {
	//NewFromConfig fallbacks to env when necessary
	return NewFromConfig(ctx, "")
}

// New returns a new instance of Broker based on given configuration struct
func New(ctx context.Context, t Type, c interface{}) (Broker, error) {
	f, ok := factories[t]
	if !ok {
		return nil, errors.Errorf("unknown broker type %s", t)
	}

	return f(ctx, c)
}
