package api

// Status is item (pipeline, node or job) status
type Status string

const (
	// StatusCreated default status, item is created
	StatusCreated Status = "CREATED"

	// StatusSubmitted status for items submitted
	StatusSubmitted Status = "SUBMITTED"

	// StatusRunning status for items running
	StatusRunning Status = "RUNNING"

	// StatusCompleted status for items completed
	StatusCompleted Status = "COMPLETED"

	// StatusFailed status for items failed
	StatusFailed Status = "FAILED"

	// StatusCancelled status for items cancelled
	StatusCancelled Status = "CANCELLED"

	// StatusTerminated status for items automatically terminated by the system (in case of failure)
	StatusTerminated Status = "TERMINATED"

	// StatusSkipped status for items skipped due to "when" tag
	StatusSkipped Status = "SKIPPED"
)

// Finished returns true if the status is considered final
func (s Status) Finished() bool {
	for _, fs := range []Status{StatusCompleted, StatusFailed, StatusTerminated, StatusCancelled} {
		if s == fs {
			return true
		}
	}
	return false
}
