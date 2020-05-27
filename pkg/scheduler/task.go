package scheduler

import (
	"poseidon/pkg/api"
	"poseidon/pkg/executor"
	"poseidon/pkg/store"
	"poseidon/pkg/util/context"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

// TaskSchedulerCallback is the callback function for task scheduler, called when a task is finished
type TaskSchedulerCallback func(ctx context.Context, taskID string, status api.Status) error

// TaskScheduler is the component responsible for scheduling tasks.
// It splits the task into jobs and send submit them to the corresponding executor.
// It handles the job's lifecycle anc calls its callback function when the task is finished.
type TaskScheduler interface {
	// Submit submits the task
	// The task is split into jobs then submitted to the corresponding executor
	// If the spec.kind does not match any registered executor, the task failed
	Submit(ctx context.Context, spec api.TaskSpec, parameters interface{}) error

	// Stop stops the execution of the given task with the given status
	// if _graceful_ is true, the task is stops gracefully, the submitted jobs are not stopped
	Stop(ctx context.Context, status api.Status, graceful bool) error

	// RegisterExecutor adds an executor supported by the task scheduler
	RegisterExecutor(kind string, exec executor.Executor)

	// SetCallBackFunc sets the callback function
	SetCallBackFunc(callback TaskSchedulerCallback)
}

// taskScheduler is default implementation for TaskScheduler
type taskScheduler struct {
	executors        map[string]executor.Executor
	callback         TaskSchedulerCallback
	store            store.TaskSchedulerStore
	execCallbackChan chan jobEvent
}

type jobEvent struct {
	correlationID string
	processID     string
	taskID        string
	jobID         string
	status        api.Status
	payload       interface{}
}

// taskFinished is the object returned into the callback chan when a task is finished
type taskFinished struct {
	correlationID string
	processID     string
	taskID        string
	status        api.Status
}

// NewTaskScheduler creates a new TaskScheduler
func NewTaskScheduler(ctx context.Context, store store.TaskSchedulerStore) (TaskScheduler, error) {

	c := make(chan jobEvent)
	ts := &taskScheduler{
		store:            store,
		execCallbackChan: c,
		executors:        make(map[string]executor.Executor),
	}
	go func(ctx context.Context, c chan jobEvent) {
		for {
			e := <-c
			ctx = context.WithCorrelationID(ctx, e.correlationID)
			ctx = context.WithProcessID(ctx, e.processID)
			ctx = context.WithTaskID(ctx, e.taskID)
			ctx = context.WithJobID(ctx, e.jobID)
			if err := ts.handleJobEvent(ctx, e.payload, e.status); err != nil {
				ctx.Logger().Error(errors.Wrapf(err, "cannot handle event of job %s", e.jobID))
				// Terminate task
				if err := ts.Stop(ctx, api.StatusTerminated, false); err != nil {
					ctx.Logger().Error(errors.Wrapf(err, "cannot terminate task %s", e.taskID))
				}
				if err := ts.callback(ctx, e.taskID, api.StatusTerminated); err != nil {
					ctx.Logger().Error(errors.Wrapf(err, "cannot call callback function for task %s", e.taskID))
				}
			}
		}
	}(ctx, c)

	return ts, nil
}

func (ts *taskScheduler) Submit(ctx context.Context, spec api.TaskSpec, parameters interface{}) error {
	ctx = context.WithTaskID(ctx, spec.Name)
	ctx.Logger().Infof("starting task %s", ctx.TaskID())

	// Create jobs
	params := make(map[string]interface{})
	if asArray, isArray := parameters.([]interface{}); isArray {
		for i, p := range asArray {
			jobID := strconv.Itoa(i + 1)
			params[jobID] = p
		}
	} else {
		params[strconv.Itoa(1)] = parameters
	}
	if err := ts.store.CreateJobs(ctx, ctx.ProcessID(), ctx.TaskID(), params); err != nil {
		return errors.Wrapf(err, "cannot create jobs for task %s", ctx.TaskID())
	}

	//Start executor
	exec, supported := ts.executors[spec.Kind]
	if !supported {
		return errors.Errorf("task kind %s is not supported", spec.Kind)
	}
	if err := exec.Start(ctx, spec.ExecutorSpec, len(params)); err != nil {
		return errors.Wrapf(err, "cannot start executor %s", spec.Kind)
	}

	//Submit jobs to executor based on the spec.Parallelism
	p := spec.Parallelism
	if p <= 0 || p > len(params) {
		p = len(params)
	}
	k := 0
	for jobID, param := range params {
		ctx.Logger().Tracef("submitting job %s to executor %s", jobID, spec.Kind)
		if err := exec.SubmitJob(ctx, jobID, param); err != nil {
			return errors.Wrapf(err, "cannot submit job %s", jobID)
		}
		k++
		if k > p {
			break
		}
	}

	if err := ts.store.SetTaskStatus(ctx, ctx.ProcessID(), ctx.TaskID(), api.StatusSubmitted, store.TimeOption{CreateTime: time.Now()}); err != nil {
		return errors.Wrapf(err, "cannot set status %s to task %s", api.StatusSubmitted, ctx.TaskID())
	}
	ctx.Logger().Tracef("task %s submitted", ctx.TaskID())

	return nil
}

func (ts *taskScheduler) Stop(ctx context.Context, status api.Status, graceful bool) error {
	// Stop jobs in store
	if err := ts.store.StopJobs(ctx, ctx.ProcessID(), ctx.TaskID(), status); err != nil {
		return errors.Wrapf(err, "cannot stop jobs in store for task %s", ctx.TaskID())
	}

	// Stop executor
	spec, err := ts.store.GetTaskSpec(ctx, ctx.ProcessID(), ctx.TaskID())
	if err != nil {
		return errors.Wrapf(err, "cannot get specification for task %s", ctx.TaskID())
	}
	exec, ok := ts.executors[spec.Kind]
	if !ok {
		return errors.Errorf("task kind %s is not supported", spec.Kind)
	}
	if err := exec.Stop(ctx, graceful); err != nil {
		return errors.Wrapf(err, "cannot stop execution of task %s", ctx.TaskID())
	}

	// Set status
	if err := ts.store.SetTaskStatus(ctx, ctx.ProcessID(), ctx.TaskID(), status, store.TimeOption{EndTime: time.Now()}); err != nil {
		return errors.Wrapf(err, "cannot set status %s for task %s", status, ctx.TaskID())
	}
	return nil
}

func (ts *taskScheduler) handleJobEvent(ctx context.Context, payload interface{}, status api.Status) error {
	taskStatus, err := ts.store.GetTaskStatus(ctx, ctx.ProcessID(), ctx.TaskID())
	if err != nil {
		return errors.Wrapf(err, "cannot get status of task %s", ctx.TaskID())
	}
	// Ignore events for finished tasks
	if taskStatus.Finished() {
		return nil
	}

	//Set Job status with time
	timeOpt := store.TimeOption{}
	if status == api.StatusRunning {
		timeOpt.StartTime = time.Now()
	} else if status.Finished() {
		timeOpt.EndTime = time.Now()
	}
	if err := ts.store.SetJobStatus(ctx, ctx.ProcessID(), ctx.TaskID(), ctx.JobID(), status, timeOpt, payload); err != nil {
		return errors.Wrapf(err, "cannot set status %s for job %s of task %s", status, ctx.JobID(), ctx.TaskID())
	}

	// Set task running if necessary
	if status == api.StatusRunning && taskStatus != api.StatusRunning {
		if err := ts.store.SetTaskStatus(ctx, ctx.ProcessID(), ctx.TaskID(), status, store.TimeOption{StartTime: time.Now()}); err != nil {
			return errors.Wrapf(err, "cannot set status %s for task %s", status, ctx.TaskID())
		}
	}

	// TODO: retry on failed
	if status.Finished() {
		return ts.sendNextOrFinish(ctx)
	}

	return nil
}

// sendNextOrFinish checks if the task is finished
// If not it submits jobs according to parallelism defined in specification
// Otherwise it computes Task status and call the callback
func (ts *taskScheduler) sendNextOrFinish(ctx context.Context) error {
	spec, err := ts.store.GetTaskSpec(ctx, ctx.ProcessID(), ctx.TaskID())
	if err != nil {
		return errors.Wrapf(err, "cannot get specification for task %s", ctx.TaskID())
	}
	s, err := ts.computeTaskStatus(ctx, spec.ContinueOnFail)
	if err != nil {
		return errors.Wrapf(err, "cannot compute status of task %s", ctx.TaskID())
	}
	if s.Finished() {
		// Stop executor
		exec, ok := ts.executors[spec.Kind]
		if !ok {
			return errors.Errorf("task kind %s is not supported", spec.Kind)
		}
		if err := exec.Stop(ctx, false); err != nil {
			return errors.Wrapf(err, "cannot stop execution of task %s", ctx.TaskID())
		}

		// Set task final status
		if err := ts.store.SetTaskStatus(ctx, ctx.ProcessID(), ctx.TaskID(), s, store.TimeOption{EndTime: time.Now()}); err != nil {
			return errors.Wrapf(err, "cannot set status %s for task %s", s, ctx.TaskID())
		}

		// Call callback
		if err := ts.callback(ctx, ctx.TaskID(), s); err != nil {
			return errors.Wrapf(err, "cannot call callback function for task %s", ctx.TaskID())
		}
	} else {
		//Submit an new job if necessary
		jobID, param, err := ts.store.GetOneJob(ctx, ctx.ProcessID(), ctx.TaskID(), api.StatusCreated)
		if err != nil {
			return errors.Wrapf(err, "cannot get a job with status %s for task %s", api.StatusCreated, ctx.TaskID())
		}
		if jobID != "" {
			ctx.Logger().Tracef("submitting job %s to executor %s", jobID, spec.Kind)
			if err := ts.executors[spec.Kind].SubmitJob(ctx, jobID, param); err != nil {
				return errors.Wrapf(err, "cannot submit job %s", jobID)
			}
		}
	}
	return nil
}

// computeTaskStatus computes a task status from the its job statuses
func (ts *taskScheduler) computeTaskStatus(ctx context.Context, cof bool) (api.Status, error) {
	notFinished, err := ts.store.CountJobWithStatus(ctx, ctx.ProcessID(), ctx.TaskID(), []api.Status{api.StatusCreated, api.StatusRunning, api.StatusSubmitted})
	if err != nil {
		return "", errors.Wrapf(err, "cannot count non finished job for task %s", ctx.TaskID())
	}
	if notFinished != 0 {
		return api.StatusRunning, nil
	}

	// Check for failed job
	failed, err := ts.store.CountJobWithStatus(ctx, ctx.ProcessID(), ctx.TaskID(), []api.Status{api.StatusFailed})
	if err != nil {
		return "", errors.Wrapf(err, "cannot count failed job for task %s", ctx.TaskID())
	}
	if failed != 0 {
		if cof {
			// If a job has failed and continue of fail is set, check if all jobs are failed or not
			completed, err := ts.store.CountJobWithStatus(ctx, ctx.ProcessID(), ctx.JobID(), []api.Status{api.StatusCompleted})
			if err != nil {
				return "", errors.Wrapf(err, "cannot count completed job for task %s", ctx.TaskID())
			}
			if completed == 0 { // All failed
				return api.StatusFailed, nil
			}
			return api.StatusCompleted, nil
		}
		return api.StatusFailed, nil
	}
	return api.StatusCompleted, nil
}

func (ts *taskScheduler) RegisterExecutor(kind string, exec executor.Executor) {
	exec.SetCallbackFunc(func(ctx context.Context, payload interface{}, status api.Status) error {
		e := jobEvent{
			correlationID: ctx.CorrelationID(),
			processID:     ctx.ProcessID(),
			taskID:        ctx.TaskID(),
			jobID:         ctx.JobID(),
			payload:       payload,
			status:        status,
		}
		ts.execCallbackChan <- e
		return nil
	})
	ts.executors[kind] = exec
}

func (ts *taskScheduler) SetCallBackFunc(callback TaskSchedulerCallback) {
	ts.callback = callback
}
