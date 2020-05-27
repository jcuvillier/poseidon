package store

import (
	"context"
	"fmt"
	"poseidon/pkg/api"
	"time"

	"github.com/pkg/errors"
)

type pipeline struct {
	spec       api.PipelineSpec
	status     api.Status
	args       interface{}
	tasks      map[string]*task
	createTime *time.Time
	startTime  *time.Time
	endTime    *time.Time
}

func (p *pipeline) applyTimeOption(opt TimeOption) {
	if !opt.CreateTime.IsZero() {
		p.createTime = &opt.CreateTime
	}
	if !opt.StartTime.IsZero() {
		p.startTime = &opt.StartTime
	}
	if !opt.EndTime.IsZero() {
		p.endTime = &opt.EndTime
	}
}

type task struct {
	name       string
	status     api.Status
	jobs       map[string]*job
	createTime *time.Time
	startTime  *time.Time
	endTime    *time.Time
}

func (t *task) applyTimeOption(opt TimeOption) {
	if !opt.CreateTime.IsZero() {
		t.createTime = &opt.CreateTime
	}
	if !opt.StartTime.IsZero() {
		t.startTime = &opt.StartTime
	}
	if !opt.EndTime.IsZero() {
		t.endTime = &opt.EndTime
	}
}

type job struct {
	processID   string
	taskID      string
	jobID       string
	params      interface{}
	status      api.Status
	executionID string
	result      interface{}
	createTime  *time.Time
	startTime   *time.Time
	endTime     *time.Time
}

func (j *job) applyTimeOption(opt TimeOption) {
	if !opt.CreateTime.IsZero() {
		j.createTime = &opt.CreateTime
	}
	if !opt.StartTime.IsZero() {
		j.startTime = &opt.StartTime
	}
	if !opt.EndTime.IsZero() {
		j.endTime = &opt.EndTime
	}
}

// NewInMemoryStore returns a new InMemory store
func NewInMemoryStore() (Store, error) {
	return &inMemory{
		pipelines: make(map[string]*pipeline),
	}, nil
}

type inMemory struct {
	pipelines map[string]*pipeline
}

func (s *inMemory) CreatePipeline(ctx context.Context, processID string, spec api.PipelineSpec, args interface{}) error {
	now := time.Now()
	p := pipeline{
		spec:       spec,
		status:     api.StatusCreated,
		args:       args,
		tasks:      make(map[string]*task),
		createTime: &now,
	}
	s.pipelines[processID] = &p
	return nil
}

func (s *inMemory) CreateTasks(ctx context.Context, processID string, names []string) error {
	now := time.Now()
	tasks := make(map[string]*task)
	for _, n := range names {
		tasks[n] = &task{
			name:       n,
			status:     api.StatusCreated,
			jobs:       make(map[string]*job),
			createTime: &now,
		}
	}
	s.pipelines[processID].tasks = tasks
	return nil
}

func (s *inMemory) GetPipelineSpec(ctx context.Context, processID string) (api.PipelineSpec, error) {
	p, exists := s.pipelines[processID]
	if !exists {
		return api.PipelineSpec{}, NotFoundError(fmt.Sprintf("process %s", processID))
	}
	return p.spec, nil
}

func (s *inMemory) GetPipelineArgs(ctx context.Context, processID string) (interface{}, error) {
	return s.pipelines[processID].args, nil
}

func (s *inMemory) GetTaskStatuses(ctx context.Context, processID string) (map[string]api.Status, error) {
	res := make(map[string]api.Status)

	tasks := s.pipelines[processID].tasks
	for _, t := range tasks {
		res[t.name] = t.status
	}
	return res, nil
}

func (s *inMemory) SetPipelineStatus(ctx context.Context, processID string, status api.Status, opt TimeOption) error {
	p := s.pipelines[processID]
	p.status = status
	s.pipelines[processID] = p
	p.applyTimeOption(opt)
	return nil
}

func (s *inMemory) IsPipelineFinished(ctx context.Context, processID string) (bool, error) {
	p, exists := s.pipelines[processID]
	if !exists {
		return false, NotFoundError(fmt.Sprintf("process %s", processID))
	}
	for _, t := range p.tasks {
		if !t.status.Finished() {
			return false, nil
		}
	}
	return true, nil
}

func (s *inMemory) GetTaskResult(ctx context.Context, processID, taskID string) (interface{}, error) {
	p, exists := s.pipelines[processID]
	if !exists {
		return false, NotFoundError(fmt.Sprintf("process %s", processID))
	}
	t, exists := p.tasks[taskID]
	if !exists {
		return false, NotFoundError(fmt.Sprintf("task %s", taskID))
	}
	if !t.status.Finished() {
		return nil, errors.Errorf("task %s not finished", taskID)
	}

	switch len(t.jobs) {
	case 0:
		return nil, errors.Errorf("task %s has no jobs", taskID)
	case 1: // Single job
		for _, j := range t.jobs {
			return j.result, nil
		}
	default:
		var results []interface{}
		for _, j := range t.jobs {
			// Some jobs might be with Failed status in case of a continue on fail task
			if j.status == api.StatusCompleted {
				results = append(results, j.result)
			}
		}
		return results, nil
	}
	return nil, nil
}

func (s *inMemory) GetTasksWithStatus(ctx context.Context, processID string, statuses []api.Status) ([]string, error) {
	p, exists := s.pipelines[processID]
	if !exists {
		return nil, NotFoundError(fmt.Sprintf("process %s", processID))
	}
	var tasks []string
	for id, t := range p.tasks {
		for _, s := range statuses {
			if s == t.status {
				tasks = append(tasks, id)
				break
			}
		}
	}
	return tasks, nil
}

func (s *inMemory) GetTaskStatus(ctx context.Context, processID, taskID string) (api.Status, error) {
	p, exists := s.pipelines[processID]
	if !exists {
		return "", NotFoundError(fmt.Sprintf("process %s", processID))
	}
	t, exists := p.tasks[taskID]
	if !exists {
		return "", NotFoundError(fmt.Sprintf("task %s", taskID))
	}
	return t.status, nil
}

func (s *inMemory) GetTaskSpec(ctx context.Context, processID, taskID string) (api.TaskSpec, error) {
	p, exists := s.pipelines[processID]
	if !exists {
		return api.TaskSpec{}, NotFoundError(fmt.Sprintf("process %s", processID))
	}
	for _, t := range p.spec.Tasks {
		if t.Name == taskID {
			return t, nil
		}
	}
	return api.TaskSpec{}, NotFoundError(fmt.Sprintf("task %s", taskID))
}

func (s *inMemory) CreateJobs(ctx context.Context, processID string, taskID string, params map[string]interface{}) error {
	p, exists := s.pipelines[processID]
	if !exists {
		return NotFoundError(fmt.Sprintf("process %s", processID))
	}
	t, exists := p.tasks[taskID]
	if !exists {
		return NotFoundError(fmt.Sprintf("task %s", taskID))
	}

	for jobID, p := range params {
		t.jobs[jobID] = &job{
			processID: processID,
			taskID:    taskID,
			jobID:     jobID,
			params:    p,
			status:    api.StatusCreated,
		}
	}
	return nil
}

func (s *inMemory) GetJobsStatuses(ctx context.Context, processID, taskID string) (map[string]api.Status, error) {
	p, exists := s.pipelines[processID]
	if !exists {
		return nil, NotFoundError(fmt.Sprintf("process %s", processID))
	}
	task, exists := p.tasks[taskID]
	if !exists {
		return nil, NotFoundError(fmt.Sprintf("task %s", taskID))
	}

	res := make(map[string]api.Status)
	for _, j := range task.jobs {
		res[j.jobID] = j.status
	}
	return res, nil
}

func (s *inMemory) StopJobs(ctx context.Context, processID, taskID string, status api.Status) error {
	p, exists := s.pipelines[processID]
	if !exists {
		return NotFoundError(fmt.Sprintf("process %s", processID))
	}
	task, exists := p.tasks[taskID]
	if !exists {
		return NotFoundError(fmt.Sprintf("task %s", taskID))
	}

	for _, j := range task.jobs {
		if !j.status.Finished() {
			j.status = status
		}
	}
	return nil
}

func (s *inMemory) SetTaskStatus(ctx context.Context, processID, taskID string, status api.Status, opt TimeOption) error {
	p, exists := s.pipelines[processID]
	if !exists {
		return NotFoundError(fmt.Sprintf("process %s", processID))
	}
	task, exists := p.tasks[taskID]
	if !exists {
		return NotFoundError(fmt.Sprintf("task %s", taskID))
	}
	task.status = status
	task.applyTimeOption(opt)
	return nil
}

func (s *inMemory) GetJobStatus(ctx context.Context, processID, taskID, jobID string) (api.Status, error) {
	p, exists := s.pipelines[processID]
	if !exists {
		return "", NotFoundError(fmt.Sprintf("process %s", processID))
	}
	task, exists := p.tasks[taskID]
	if !exists {
		return "", NotFoundError(fmt.Sprintf("task %s", taskID))
	}
	job, exist := task.jobs[jobID]
	if !exist {
		return "", NotFoundError(fmt.Sprintf("job %s in task %s in process %s", jobID, taskID, processID))
	}
	return job.status, nil
}

func (s *inMemory) SetJobStatus(ctx context.Context, processID, taskID, jobID string, status api.Status, opt TimeOption, payload interface{}) error {
	p, exists := s.pipelines[processID]
	if !exists {
		return NotFoundError(fmt.Sprintf("process %s", processID))
	}
	task, exists := p.tasks[taskID]
	if !exists {
		return NotFoundError(fmt.Sprintf("task %s", taskID))
	}
	job, exist := task.jobs[jobID]
	if !exist {
		return NotFoundError(fmt.Sprintf("job %s in task %s in process %s", jobID, taskID, processID))
	}
	job.status = status
	job.result = payload
	job.applyTimeOption(opt)
	return nil
}

func (s *inMemory) CountJobWithStatus(ctx context.Context, processID, taskID string, statuses []api.Status) (int, error) {
	p, exists := s.pipelines[processID]
	if !exists {
		return 0, NotFoundError(fmt.Sprintf("process %s", processID))
	}
	task, exists := p.tasks[taskID]
	if !exists {
		return 0, NotFoundError(fmt.Sprintf("task %s", taskID))
	}
	count := 0
	for _, j := range task.jobs {
		for _, s := range statuses {
			if j.status == s {
				count++
				break
			}
		}
	}
	return count, nil
}

// GetOneJob returns a jobID corresponding to the given status with its parameters.
func (s *inMemory) GetOneJob(ctx context.Context, processID, taskID string, status api.Status) (string, interface{}, error) {
	p, exists := s.pipelines[processID]
	if !exists {
		return "", nil, NotFoundError(fmt.Sprintf("process %s", processID))
	}
	task, exists := p.tasks[taskID]
	if !exists {
		return "", nil, NotFoundError(fmt.Sprintf("task %s", taskID))
	}
	for _, j := range task.jobs {
		if j.status == status {
			return j.jobID, j.params, nil
		}
	}
	//No job found
	return "", nil, nil
}

func (s *inMemory) ListPipelines(ctx context.Context) (map[string]string, error) {
	res := make(map[string]string)
	for k, v := range s.pipelines {
		res[k] = v.spec.Name
	}
	return res, nil
}

func (s *inMemory) GetPipelineState(ctx context.Context, processID string) (api.PipelineState, error) {
	p, exists := s.pipelines[processID]
	if !exists {
		return api.PipelineState{}, NotFoundError(fmt.Sprintf("process %s", processID))
	}
	var tasks []api.TaskState
	for _, v := range p.tasks {
		tasks = append(tasks, api.TaskState{
			Name:      v.name,
			Status:    v.status,
			StartTime: v.startTime,
			EndTime:   v.endTime,
		})
	}
	return api.PipelineState{
		Name:       p.spec.Name,
		Status:     p.status,
		Tasks:      tasks,
		CreateTime: p.createTime,
		StartTime:  p.startTime,
		EndTime:    p.endTime,
	}, nil
}

func (s *inMemory) GetTaskState(ctx context.Context, processID, taskID string) (api.TaskState, error) {
	p, exists := s.pipelines[processID]
	if !exists {
		return api.TaskState{}, NotFoundError(fmt.Sprintf("process %s", processID))
	}
	task, exists := p.tasks[taskID]
	if !exists {
		return api.TaskState{}, NotFoundError(fmt.Sprintf("task %s", taskID))
	}
	var jobs []api.JobState
	for _, v := range task.jobs {
		jobs = append(jobs, api.JobState{
			ID:        v.jobID,
			Status:    v.status,
			StartTime: v.startTime,
			EndTime:   v.endTime,
		})
	}
	return api.TaskState{
		Name:      task.name,
		Status:    task.status,
		Jobs:      jobs,
		StartTime: task.startTime,
		EndTime:   task.endTime,
	}, nil
}

func (s *inMemory) GetJobState(ctx context.Context, processID, taskID, jobID string) (api.JobState, error) {
	p, exists := s.pipelines[processID]
	if !exists {
		return api.JobState{}, NotFoundError(fmt.Sprintf("process %s", processID))
	}
	task, exists := p.tasks[taskID]
	if !exists {
		return api.JobState{}, NotFoundError(fmt.Sprintf("task %s", taskID))
	}

	job, exists := task.jobs[jobID]
	if !exists {
		return api.JobState{}, NotFoundError(fmt.Sprintf("job %s", jobID))
	}
	return api.JobState{
		ID:        job.jobID,
		Status:    job.status,
		StartTime: job.startTime,
		EndTime:   job.endTime,
	}, nil
}

func (s *inMemory) GetJobResult(ctx context.Context, processID, taskID, jobid string) (interface{}, error) {
	p, exists := s.pipelines[processID]
	if !exists {
		return api.JobState{}, NotFoundError(fmt.Sprintf("process %s", processID))
	}
	task, exists := p.tasks[taskID]
	if !exists {
		return api.JobState{}, NotFoundError(fmt.Sprintf("task %s", taskID))
	}

	job, exists := task.jobs[jobid]
	if !exists {
		return api.JobState{}, NotFoundError(fmt.Sprintf("job %s", jobid))
	}
	return job.result, nil
}
