package context

import (
	gocontext "context"

	"github.com/sirupsen/logrus"
)

// Context extends the regular golang context.Context interface with functionnalities such as access to logger, storage etc.
type Context interface {
	gocontext.Context
	Logger() *logrus.Entry
	ProcessID() string
	CorrelationID() string
	TaskID() string
	JobID() string
	NodeName() string
	ExecutionID() string
}

// Background returns a non-nil, empty Context.
func Background() Context {
	return ctx{
		Context: gocontext.Background(),
	}
}

// FromContext returns a new context from the given go context.
func FromContext(c gocontext.Context) Context {
	return ctx{
		Context: c,
	}
}

// WithProcessID returns a copy of the context with a processID.
func WithProcessID(c Context, pid string) Context {

	return ctx{
		c,
		pid,
		c.CorrelationID(),
		c.JobID(),
		c.TaskID(),
		c.ExecutionID(),
	}
}

// WithCorrelationID returns a copy of the context with a correlationID.
func WithCorrelationID(c Context, correlationID string) Context {
	return ctx{
		c,
		c.ProcessID(),
		correlationID,
		c.JobID(),
		c.TaskID(),
		c.ExecutionID(),
	}
}

// WithJobID returns a copy of the context with a jobID.
func WithJobID(c Context, jobID string) Context {
	return ctx{
		c,
		c.ProcessID(),
		c.CorrelationID(),
		jobID,
		c.TaskID(),
		c.ExecutionID(),
	}
}

// WithNodeName returns a copy of the context with a nodename.
func WithNodeName(c Context, nodename string) Context {
	return ctx{
		c,
		c.ProcessID(),
		c.CorrelationID(),
		c.JobID(),
		nodename,
		c.ExecutionID(),
	}
}

// WithTaskID returns a copy of the context with a taskID.
func WithTaskID(c Context, taskID string) Context {
	return ctx{
		c,
		c.ProcessID(),
		c.CorrelationID(),
		c.JobID(),
		taskID,
		c.ExecutionID(),
	}
}

// WithExecutionID returns a copy of the context with a executionID.
func WithExecutionID(c Context, executionID string) Context {
	return ctx{
		c,
		c.ProcessID(),
		c.CorrelationID(),
		c.JobID(),
		c.TaskID(),
		executionID,
	}
}

type ctx struct {
	gocontext.Context
	processID     string
	correlationID string
	jobID         string
	taskID        string
	executionID   string
}

func (c ctx) Logger() *logrus.Entry {
	l := logrus.New()
	l.SetLevel(logrus.TraceLevel)
	l.SetFormatter(&logrus.TextFormatter{
		DisableColors: true,
		FullTimestamp: true,
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyMsg: "message",
		},
	})
	e := logrus.NewEntry(l)
	if c.CorrelationID() != "" {
		e = e.WithField("correlation_id", c.CorrelationID())
	}
	if c.ProcessID() != "" {
		e = e.WithField("process_id", c.ProcessID())
	}
	if c.TaskID() != "" {
		e = e.WithField("task_id", c.TaskID())
	}
	if c.JobID() != "" {
		e = e.WithField("job_id", c.JobID())
	}
	return e
}

func (c ctx) ProcessID() string {
	return c.processID
}

func (c ctx) CorrelationID() string {
	return c.correlationID
}

func (c ctx) JobID() string {
	return c.jobID
}

func (c ctx) NodeName() string {
	return c.taskID
}

func (c ctx) TaskID() string {
	return c.taskID
}

func (c ctx) ExecutionID() string {
	return c.executionID
}
