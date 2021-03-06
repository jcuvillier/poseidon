package events

import (
	"fmt"
	"time"
)

// EventType type of event
type EventType string

const (
	TypeSubmit  EventType = "SUBMIT"
	TypeRun     EventType = "RUN"
	TypeSuccess EventType = "SUCCESS"
	TypeError   EventType = "ERROR"
)

// Event represents a message to publish/receive.
type Event struct {
	Type          EventType
	ProcessID     string
	TaskID        string
	JobID         string
	CorrelationID string
	Data          interface{}
	Time          time.Time
}

func (e Event) String() string {
	return fmt.Sprintf("%s for task %s and job %s", e.Type, e.TaskID, e.JobID)
}

// ErrorEventData is the expected data type for event with type TypeError
type ErrorEventData struct {
	Message string `json:"message"`
}

// RunEventData is the expected data type for event with type TypeRun
type RunEventData struct {
	ExecutionID string `json:"execution_id"`
}
