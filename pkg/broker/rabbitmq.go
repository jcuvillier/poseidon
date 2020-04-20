package broker

import (
	"encoding/json"
	"fmt"
	"poseidon/pkg/api"
	"poseidon/pkg/util/context"
	"poseidon/pkg/broker/events"
	"poseidon/pkg/store"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

const (
	envRabbitMQUser     = "BROKER_RABBITMQ_USER"
	envRabbitMQPassword = "BROKER_RABBITMQ_PASSWORD"
	envRabbitMQURI      = "BROKER_RABBITMQ_URI"
	// RabbitMQType Broker type RabbitMQ
	RabbitMQType Type = "rabbitmq"
)

func init() {
	f := func(ctx context.Context, c interface{}) (Broker, error) {
		asRabbitMQConf, isRabbitMQConf := c.(*RabbitMQConfig)
		if !isRabbitMQConf {
			return nil, errors.Errorf("given configuration struct is not type %v", RabbitMQConfig{})
		}
		return NewRabbitMQBroker(ctx, *asRabbitMQConf)
	}
	register(RabbitMQType, f, &RabbitMQConfig{})
}

type rabbitmq struct {
	conn   *amqp.Connection
	ch     *amqp.Channel
	config RabbitMQConfig
}

// RabbitMQConfig is configuration for rabbitmq broker implementation
type RabbitMQConfig struct {
	User     string `json:"user" env:"BROKER_RABBITMQ_USER"`
	Password string `json:"password" env:"BROKER_RABBITMQ_USER"`
	URI      string `json:"uri" env:"BROKER_RABBITMQ_URI"`
}

//NewRabbitMQBroker returns a Broker implementation based on RabbitMQ.
func NewRabbitMQBroker(ctx context.Context, conf RabbitMQConfig) (Broker, error) {
	url := fmt.Sprintf("amqp://%s:%s@%s", conf.User, conf.Password, conf.URI)
	ctx.Logger().Infof("connecting to rabbitmq with url '%s'", url)
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot connect to rabbitmq with url '%s'", url)
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "cannot open channel to rabbitmq")
	}
	err = ch.Qos(1, 0, false)
	if err != nil {
		return nil, errors.Wrap(err, "cannot set rabbitmq Qos controls")
	}
	return &rabbitmq{
		conn:   conn,
		ch:     ch,
		config: conf,
	}, nil
}

func (q *rabbitmq) Publish(ctx context.Context, evt events.Event, qname, routingkey string) error {
	ctx.Logger().Tracef("publishing event %s to exchange %s", evt, qname)
	//Headers
	headers := amqp.Table{
		api.HeaderProcessID:     evt.ProcessID,
		api.HeaderNodename:      evt.NodeName,
		api.HeaderJobID:         evt.JobID,
		api.HeaderCorrelationID: evt.CorrelationID,
		api.HeaderType:          string(evt.Type),
	}

	// Marshal body
	data := evt.Data
	if data == nil {
		data = struct{}{}
	}
	body, err := json.Marshal(evt.Data)
	if err != nil {
		return err
	}

	return q.ch.Publish(
		qname,      // exchange
		routingkey, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
			Headers:     headers,
		})
}

func (q *rabbitmq) Receive(ctx context.Context, f HandleFunc, ferr ErrorHandler, qname string, options ...ReceiveOption) error {
	ctx.Logger().Infof("receiving events from queue %s", qname)
	msgs, err := q.ch.Consume(
		qname,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return errors.Wrapf(err, "cannot register consumer to queue %s", qname)
	}

	for d := range msgs {
		// Unmarshal body
		var data interface{}
		switch d.ContentType {
		case "application/json":
			if err := json.Unmarshal(d.Body, &data); err != nil {
				d.Reject(false)
				return errors.Wrapf(err, "cannot unmarshal received event %s for job %s of node %s, droping event", d.Headers[api.HeaderType], d.Headers[api.HeaderJobID], d.Headers[api.HeaderNodename])
			}
		default:
			ctx.Logger().Warnf("received event with unsupported content-type %s, dropping event", d.ContentType)
			d.Reject(false)
			return nil
		}

		// Create event
		pid := d.Headers[api.HeaderProcessID].(string)
		correlationID := d.Headers[api.HeaderCorrelationID].(string)
		evt := events.Event{
			Type:          events.EventType(d.Headers[api.HeaderType].(string)),
			CorrelationID: correlationID,
			ProcessID:     pid,
			NodeName:      d.Headers[api.HeaderNodename].(string),
			JobID:         d.Headers[api.HeaderJobID].(string),
			Data:          data,
		}

		// Create context
		ctx := context.Background()
		ctx = context.WithProcessID(ctx, pid)
		ctx = context.WithCorrelationID(ctx, correlationID)
		ctx = context.WithNodeName(ctx, evt.NodeName)
		ctx = context.WithJobID(ctx, evt.JobID)

		//Apply options
		for _, o := range options {
			err := o(ctx, &evt)
			if err != nil {
				err = errors.Wrapf(err, "cannot handle received event %s", evt)
				ctx.Logger().Trace(err)
				nack(ctx, evt, &d)
			}
		}

		if err := f(ctx, evt); err != nil {
			// TODO: Implement reject or nack policy depending on error
			ctx.Logger().Errorf("cannot handle event %s for job %s of node %s, %s", evt.Type, evt.JobID, evt.NodeName, err)
			if errors.As(err, &store.ErrNotFound{}) {
				reject(ctx, evt, &d)
			} else {
				nack(ctx, evt, &d)
			}
			continue
		}
		ack(ctx, evt, &d)
	}
	return errors.New("delivery channel closed")
}

// ack acknowledge the event and log error if the acknowledgment returns an error.
func ack(ctx context.Context, evt events.Event, d *amqp.Delivery) {
	if err := d.Ack(false); err != nil {
		ctx.Logger().Errorf("cannot ack event %s for job %s of node %s, %s", evt.Type, evt.JobID, evt.NodeName, err)
	}
}

// nack negatively acknowledge the event, requeueing it, and log error if the acknowledgment returns an error.
func nack(ctx context.Context, evt events.Event, d *amqp.Delivery) {
	if err := d.Nack(false, true); err != nil {
		ctx.Logger().Errorf("cannot ack event %s for job %s of node %s, %s", evt.Type, evt.JobID, evt.NodeName, err)
	}
}

// nack negatively acknowledge the event and log error if the acknowledgment returns an error.
func reject(ctx context.Context, evt events.Event, d *amqp.Delivery) {
	if err := d.Reject(false); err != nil {
		ctx.Logger().Errorf("cannot ack event %s for job %s of node %s, %s", evt.Type, evt.JobID, evt.NodeName, err)
	}
}

func (q *rabbitmq) CreateQueue(ctx context.Context, name, bindTo string) error {
	ctx.Logger().Tracef("creating queue %s with routing headers %s=%s and %s=%s", name, api.HeaderProcessID, ctx.ProcessID(), api.HeaderNodename, ctx.NodeName())
	_, err := q.ch.QueueDeclare(
		name,  // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return errors.Wrapf(err, "cannot declare queue %s", name)
	}

	err = q.ch.QueueBind(
		name,   // queue name
		"",     // routing key
		bindTo, // exchange
		false,
		amqp.Table{
			"x-match":           "all", //x-match = all means all headers must match for the routing,
			api.HeaderProcessID: ctx.ProcessID(),
			api.HeaderNodename:  ctx.NodeName(),
		},
	)
	if err != nil {
		return errors.Wrapf(err, "cannot bind queue %s to exchange %s with routing headers %s=%s and %s=%s", name, bindTo, api.HeaderProcessID, ctx.ProcessID(), api.HeaderNodename, ctx.NodeName())
	}
	return nil
}

func (q *rabbitmq) DeleteQueue(ctx context.Context, name string) error {
	ctx.Logger().Tracef("deleting queue %s", name)
	q.ch.QueueDelete(
		name, //queue name
		false,
		false,
		false,
	)
	return nil
}

func (q *rabbitmq) Close() error {
	if err := q.ch.Close(); err != nil {
		return err
	}
	if err := q.conn.Close(); err != nil {
		return err
	}
	return nil
}
