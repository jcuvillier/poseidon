package scheduler

import (
	"poseidon/pkg/api"
	"poseidon/pkg/store"
	"poseidon/pkg/util/context"
	"poseidon/pkg/util/template"
	"strings"
	"time"

	"github.com/pkg/errors"
)

// SetupFunc is the function called when a pipeline is submitted.
type SetupFunc func(ctx context.Context) error

// TearDownFunc is the function called when a pipeline is finished. (Either success or failure)
type TearDownFunc func(ctx context.Context) error

// PipelineScheduler defines the entries of the pipeline engine.
type PipelineScheduler interface {
	// Submit the pipeline defined by the given spec with the given arguments.
	Submit(ctx context.Context, spec api.PipelineSpec, args interface{}) error

	// Terminate terminates a running pipeline and set its status to TERMINATED.
	Terminate(ctx context.Context, reason string) error

	// Cancel cancels gracefully (or not) a running pipeline.
	Cancel(ctx context.Context, gracefully bool) error

	// TaskFinished is the function called when a task is finished. (Success or failure)
	TaskFinished(ctx context.Context, taskID string, status api.Status) error

	// Set function to be called when a pipeline is submitted.
	SetSetupFunc(SetupFunc)

	// Set function to be called when a pipeline is finished. (Either success or failure)
	SetTearDownFunc(TearDownFunc)
}

// NewPipelineScheduler returns a new instance of Pipeline scheduler
func NewPipelineScheduler(ts TaskScheduler, s store.Store) (PipelineScheduler, error) {
	c := make(chan taskFinished)
	ts.SetCallBackFunc(func(ctx context.Context, taskID string, status api.Status) error {
		c <- taskFinished{
			correlationID: ctx.CorrelationID(),
			processID:     ctx.ProcessID(),
			taskID:        taskID,
			status:        status,
		}
		return nil
	})
	sc := pipelineScheduler{
		store: s,
		ts:    ts,
	}

	// The following go routine handles the taskFinished events sent by the executors
	go func(sc PipelineScheduler, ch chan taskFinished) {
		for {
			tf := <-ch
			ctx := context.Background()
			ctx = context.WithCorrelationID(ctx, tf.correlationID)
			ctx = context.WithProcessID(ctx, tf.processID)
			ctx = context.WithTaskID(ctx, tf.taskID)
			if err := sc.TaskFinished(ctx, tf.taskID, tf.status); err != nil {
				if err := sc.Terminate(ctx, err.Error()); err != nil {
					ctx.Logger().Error(errors.Wrapf(err, "cannot terminate pipeline %s", tf.processID))
				}
			}
		}
	}(&sc, c)

	return &sc, nil
}

type pipelineScheduler struct {
	store        store.PipelineSchedulerStore
	ts           TaskScheduler
	setupFunc    SetupFunc
	teardownFunc TearDownFunc
}

func (sc *pipelineScheduler) Submit(ctx context.Context, spec api.PipelineSpec, args interface{}) (e error) {
	ctx.Logger().Infof("starting pipeline %s", spec.Name)
	//Auto log errors
	defer func(ctx context.Context) {
		if e != nil {
			ctx.Logger().Error(e)
		}
	}(ctx)
	processID := ctx.ProcessID()
	// Call setup func
	if sc.setupFunc != nil {
		if err := sc.setupFunc(ctx); err != nil {
			return err
		}
	}

	// Create pipeline & tasks into store
	err := sc.store.CreatePipeline(ctx, processID, spec, args)
	if err != nil {
		return errors.Wrapf(err, "cannot create pipeline %s", spec.Name)
	}
	tasks := make([]string, len(spec.Tasks))
	for i := range spec.Tasks {
		tasks[i] = spec.Tasks[i].Name
	}
	err = sc.store.CreateTasks(ctx, processID, tasks)
	if err != nil {
		return errors.Wrapf(err, "cannot create tasks for pipeline %s", spec.Name)
	}

	if err := sc.next(ctx); err != nil {
		return err
	}

	if err := sc.store.SetPipelineStatus(ctx, ctx.ProcessID(), api.StatusRunning, store.TimeOption{StartTime: time.Now()}); err != nil {
		return errors.Wrapf(err, "cannot set pipeline status to %s", api.StatusRunning)
	}

	return nil
}

// next selects the next tasks to be submitted and submits them
func (sc *pipelineScheduler) next(ctx context.Context) error {
	processID := ctx.ProcessID()
	args, err := sc.store.GetPipelineArgs(ctx, processID)
	if err != nil {
		return errors.Wrapf(err, "cannot get pipeline arguments")
	}
	spec, err := sc.store.GetPipelineSpec(ctx, processID)
	if err != nil {
		return errors.Wrapf(err, "cannot get spec for pipeline %s", processID)
	}

	taskstatuses, err := sc.store.GetTaskStatuses(ctx, processID)
	if err != nil {
		return errors.Wrapf(err, "cannot get tasks with status for pipeline %s", processID)
	}

	// Select the tasks to submit
	tasksToSubmit, err := selectTasksForSubmission(ctx, spec, taskstatuses)
	if err != nil {
		return errors.Wrapf(err, "cannot select tasks to submit")
	}
	if len(tasksToSubmit) == 0 { // No tasks to submit, check there is at least one running task, otherwise, the pipeline is stalled.
		hasRunningTask := false
		for _, s := range taskstatuses {
			if s == api.StatusRunning {
				hasRunningTask = true
				break
			}
		}
		if !hasRunningTask {
			return errors.Errorf("no task to submit")
		}
	}

	//Submit the tasks
	for _, n := range tasksToSubmit {
		params, err := sc.taskParameters(ctx, n, args)
		if err != nil {
			return errors.Wrapf(err, "cannot compute parameters for task %s", n.Name)
		}

		if err := sc.ts.Submit(context.WithTaskID(ctx, n.Name), n, params); err != nil {
			return errors.Wrapf(err, "cannot submit task %s", n.Name)
		}
	}
	return nil
}

func (sc *pipelineScheduler) TaskFinished(ctx context.Context, taskID string, status api.Status) error {
	ctx.Logger().Infof("task %s finished with status %s", taskID, status)
	if status == api.StatusFailed {
		if err := sc.stop(ctx, api.StatusFailed, false); err != nil {
			return errors.Wrap(err, "cannot stop process")
		}
		//Call teardown func if set
		if sc.teardownFunc != nil {
			err := sc.teardownFunc(ctx)
			if err != nil {
				return errors.Wrap(err, "error calling teardown function")
			}
		}
		return nil
	}
	finished, err := sc.store.IsPipelineFinished(ctx, ctx.ProcessID())
	if err != nil {
		return errors.Wrap(err, "cannot determine if pipeline is finished")
	}
	if finished { // Pipeline finished
		if err := sc.store.SetPipelineStatus(ctx, ctx.ProcessID(), status, store.TimeOption{EndTime: time.Now()}); err != nil {
			return errors.Wrapf(err, "cannot set status %s for pipeline", status)
		}
		ctx.Logger().Infof("pipeline finished with status %s", status)

		//Call teardown func if set
		if sc.teardownFunc != nil {
			err := sc.teardownFunc(ctx)
			if err != nil {
				return errors.Wrap(err, "error calling teardown function")
			}
		}
	} else {
		if err := sc.next(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (sc *pipelineScheduler) Terminate(ctx context.Context, reason string) error {
	ctx.Logger().Infof("terminating process %s because %s", ctx.ProcessID(), reason)
	return sc.stop(ctx, api.StatusTerminated, false)
}

func (sc *pipelineScheduler) Cancel(ctx context.Context, gracefully bool) error {
	ctx.Logger().Infof("cancelling process %s", ctx.ProcessID())
	return sc.stop(ctx, api.StatusCancelled, gracefully)
}

func (sc *pipelineScheduler) SetSetupFunc(f SetupFunc) {
	sc.setupFunc = f
}

func (sc *pipelineScheduler) SetTearDownFunc(f TearDownFunc) {
	sc.teardownFunc = f
}

// stop stops the running pipeline and sets the given status (FAILED, CANCEL or TERMINATED)
// If status is FAILED, all running tasks are terminates.
// If gracefully is true, all running tasks finished normally
func (sc *pipelineScheduler) stop(ctx context.Context, status api.Status, gracefully bool) error {
	if gracefully {
		return errors.New("gracefull stop not yet implemented")
	}
	statuses := []api.Status{api.StatusSubmitted, api.StatusRunning}
	tasks, err := sc.store.GetTasksWithStatus(ctx, ctx.ProcessID(), statuses)
	if err != nil {
		return errors.Wrapf(err, "cannot get tasks with %s", statuses)
	}
	var stopErr error
	//Task status is either TERMINATED (if pipeline is failed or terminated) or CANCELLED
	taskStatus := api.StatusTerminated
	if status == api.StatusCancelled {
		taskStatus = api.StatusCancelled
	}
	for _, t := range tasks {
		ctx = context.WithTaskID(ctx, t)

		if err := sc.ts.Stop(ctx, taskStatus, gracefully); err != nil {
			// If stop fails, keep trying to stop other tasks
			stopErr = errors.Wrapf(err, "cannot stop task %s", t)
		}
	}
	if err := sc.store.SetPipelineStatus(ctx, ctx.ProcessID(), status, store.TimeOption{EndTime: time.Now()}); err != nil {
		stopErr = errors.Wrapf(err, "cannot set pipeline status to %s", status)
	}
	return stopErr
}

func selectTasksForSubmission(ctx context.Context, spec api.PipelineSpec, taskstatuses map[string]api.Status) ([]api.TaskSpec, error) {
	var toSubmit []api.TaskSpec
	for _, s := range spec.Tasks {
		if !taskstatuses[s.Name].Finished() && taskstatuses[s.Name] != api.StatusRunning && taskDepsCompleted(ctx, s, taskstatuses) {
			toSubmit = append(toSubmit, s)
		}
	}
	return toSubmit, nil
}

func taskDepsCompleted(ctx context.Context, task api.TaskSpec, statuses map[string]api.Status) bool {
	for _, dep := range task.Dependencies {
		s, exist := statuses[dep]
		if !exist {
			ctx.Logger().Errorf("missing task %s for pipeline %s", dep, ctx.ProcessID())
			return false
		}
		if s != api.StatusCompleted {
			return false
		}
	}
	return true
}

func (sc *pipelineScheduler) taskParameters(ctx context.Context, task api.TaskSpec, args interface{}) ([]interface{}, error) {
	// Map containing task results
	taskResults := make(map[string]interface{})
	taskResults[api.InputPipelineArgs] = args
	tpl := template.New(task.Input)
	for _, expr := range tpl.FindAll() {
		taskname := strings.Split(expr.Text, ".")[0]
		if taskname != api.InputPipelineArgs {
			r, err := sc.store.GetTaskResult(ctx, ctx.ProcessID(), taskname)
			if err != nil {
				return nil, errors.Wrapf(err, "cannot get result for task %s", taskname)
			}
			taskResults[taskname] = r
		}
	}
	resolved, err := tpl.Resolve(template.ResolveWithMap(taskResults))
	if err != nil {
		return nil, errors.Wrapf(err, "cannot compute parameter for task %s", task.Name)
	}
	if asArray, isArray := resolved.([]interface{}); isArray {
		return asArray, nil
	}
	return []interface{}{resolved}, nil
}
