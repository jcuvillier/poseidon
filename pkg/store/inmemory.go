package store

import (
	"context"
	"fmt"
	"poseidon/pkg/api"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

type pipeline struct {
	spec       api.PipelineSpec
	status     api.Status
	args       interface{}
	nodes      map[string]*node
	createTime *time.Time
	startTime  *time.Time
	endTime    *time.Time
}

type node struct {
	name      string
	status    api.Status
	jobs      map[string]*job
	startTime *time.Time
	endTime   *time.Time
}

type job struct {
	processID   string
	nodename    string
	jobID       string
	params      interface{}
	status      api.Status
	executionID string
	result      interface{}
	startTime   *time.Time
	endTime     *time.Time
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

func (s *inMemory) CreatePipeline(ctx context.Context, pid string, spec api.PipelineSpec, args interface{}) error {
	now := time.Now()
	p := pipeline{
		spec:       spec,
		status:     api.StatusCreated,
		args:       args,
		nodes:      make(map[string]*node),
		createTime: &now,
	}
	s.pipelines[pid] = &p
	return nil
}

func (s *inMemory) CreateNodes(ctx context.Context, pid string, names []string) error {
	nodes := make(map[string]*node)
	for _, n := range names {
		nodes[n] = &node{
			name:   n,
			status: api.StatusCreated,
			jobs:   make(map[string]*job),
		}
	}
	s.pipelines[pid].nodes = nodes
	return nil
}

func (s *inMemory) GetPipelineSpec(ctx context.Context, pid string) (api.PipelineSpec, error) {
	p, exists := s.pipelines[pid]
	if !exists {
		return api.PipelineSpec{}, NotFoundError(fmt.Sprintf("process %s", pid))
	}
	return p.spec, nil
}

func (s *inMemory) GetNodeStatuses(ctx context.Context, pid string) (map[string]api.Status, error) {
	res := make(map[string]api.Status)

	nodes := s.pipelines[pid].nodes
	for _, n := range nodes {
		res[n.name] = n.status
	}
	return res, nil
}

func (s *inMemory) GetPipelineArgs(ctx context.Context, pid string) (interface{}, error) {
	return s.pipelines[pid].args, nil
}

func (s *inMemory) SetPipelineStatus(ctx context.Context, pid string, status api.Status) error {
	p := s.pipelines[pid]
	p.status = status
	s.pipelines[pid] = p
	now := time.Now()
	if status.Finished() {
		p.endTime = &now
	} else if status == api.StatusRunning {
		p.startTime = &now
	}
	return nil
}

func (s *inMemory) IsPipelineFinished(ctx context.Context, pid string) (bool, error) {
	p, exists := s.pipelines[pid]
	if !exists {
		return false, NotFoundError(fmt.Sprintf("process %s", pid))
	}
	for _, n := range p.nodes {
		if !n.status.Finished() {
			return false, nil
		}
	}
	return true, nil
}

func (s *inMemory) NodeResult(ctx context.Context, pid, nodename string) (interface{}, error) {
	p, exists := s.pipelines[pid]
	if !exists {
		return false, NotFoundError(fmt.Sprintf("process %s", pid))
	}
	n, exists := p.nodes[nodename]
	if !exists {
		return false, NotFoundError(fmt.Sprintf("node %s", nodename))
	}
	if !n.status.Finished() {
		return nil, errors.Errorf("node %s not finished", nodename)
	}

	switch len(n.jobs) {
	case 0:
		return nil, errors.Errorf("node %s has no jobs", nodename)
	case 1: // Single job
		for _, j := range n.jobs {
			return j.result, nil
		}
	default:
		var results []interface{}
		for _, j := range n.jobs {
			// Some jobs might be with Failed status in case of a continue on fail node
			if j.status == api.StatusCompleted {
				results = append(results, j.result)
			}
		}
		return results, nil
	}
	return nil, nil
}

func (s *inMemory) GetNodeSpec(ctx context.Context, pid, nodename string) (api.NodeSpec, error) {
	p, exists := s.pipelines[pid]
	if !exists {
		return api.NodeSpec{}, NotFoundError(fmt.Sprintf("process %s", pid))
	}
	for _, n := range p.spec.Nodes {
		if n.Name == nodename {
			return n, nil
		}
	}
	return api.NodeSpec{}, NotFoundError(fmt.Sprintf("node %s", nodename))
}

func (s *inMemory) CreateJobs(ctx context.Context, pid string, nodename string, params []interface{}) error {
	p, exists := s.pipelines[pid]
	if !exists {
		return NotFoundError(fmt.Sprintf("process %s", pid))
	}
	n, exists := p.nodes[nodename]
	if !exists {
		return NotFoundError(fmt.Sprintf("node %s", nodename))
	}

	for i, p := range params {
		jobID := strconv.Itoa(i)
		n.jobs[jobID] = &job{
			processID: pid,
			nodename:  nodename,
			jobID:     jobID,
			params:    p,
			status:    api.StatusCreated,
		}
	}
	return nil
}

func (s *inMemory) GetJobsStatuses(ctx context.Context, pid, nodename string) (map[string]api.Status, error) {
	p, exists := s.pipelines[pid]
	if !exists {
		return nil, NotFoundError(fmt.Sprintf("process %s", pid))
	}
	node, exists := p.nodes[nodename]
	if !exists {
		return nil, NotFoundError(fmt.Sprintf("node %s", nodename))
	}

	res := make(map[string]api.Status)
	for _, j := range node.jobs {
		res[j.jobID] = j.status
	}
	return res, nil
}

func (s *inMemory) StopJobs(ctx context.Context, pid, nodename string, status api.Status) error {
	p, exists := s.pipelines[pid]
	if !exists {
		return NotFoundError(fmt.Sprintf("process %s", pid))
	}
	node, exists := p.nodes[nodename]
	if !exists {
		return NotFoundError(fmt.Sprintf("node %s", nodename))
	}

	for _, j := range node.jobs {
		if !j.status.Finished() {
			j.status = status
		}
	}
	return nil
}

func (s *inMemory) SetNodeStatus(ctx context.Context, pid, nodename string, status api.Status) error {
	p, exists := s.pipelines[pid]
	if !exists {
		return NotFoundError(fmt.Sprintf("process %s", pid))
	}
	node, exists := p.nodes[nodename]
	if !exists {
		return NotFoundError(fmt.Sprintf("node %s", nodename))
	}
	node.status = status
	now := time.Now()
	if status == api.StatusSubmitted {
		node.startTime = &now
	} else if status.Finished() {
		node.endTime = &now
	}
	return nil
}

func (s *inMemory) GetJobStatus(ctx context.Context, pid, nodename, jobID string) (api.Status, error) {
	p, exists := s.pipelines[pid]
	if !exists {
		return "", NotFoundError(fmt.Sprintf("process %s", pid))
	}
	node, exists := p.nodes[nodename]
	if !exists {
		return "", NotFoundError(fmt.Sprintf("node %s", nodename))
	}
	job, exist := node.jobs[jobID]
	if !exist {
		return "", NotFoundError(fmt.Sprintf("job %s in node %s in process %s", jobID, nodename, pid))
	}
	return job.status, nil
}

func (s *inMemory) SetJobStatus(ctx context.Context, pid, nodename, jobID string, status api.Status, t time.Time, payload interface{}) error {
	p, exists := s.pipelines[pid]
	if !exists {
		return NotFoundError(fmt.Sprintf("process %s", pid))
	}
	node, exists := p.nodes[nodename]
	if !exists {
		return NotFoundError(fmt.Sprintf("node %s", nodename))
	}
	job, exist := node.jobs[jobID]
	if !exist {
		return NotFoundError(fmt.Sprintf("job %s in node %s in process %s", jobID, nodename, pid))
	}
	job.status = status
	job.result = payload

	if status.Finished() {
		job.endTime = &t
	} else if status == api.StatusRunning {
		job.startTime = &t
	}
	return nil
}

func (s *inMemory) ListPipelines(ctx context.Context) (map[string]string, error) {
	res := make(map[string]string)
	for k, v := range s.pipelines {
		res[k] = v.spec.Name
	}
	return res, nil
}

func (s *inMemory) PipelineState(ctx context.Context, pid string) (api.PipelineState, error) {
	p, exists := s.pipelines[pid]
	if !exists {
		return api.PipelineState{}, NotFoundError(fmt.Sprintf("process %s", pid))
	}
	var nodes []api.NodeState
	for _, v := range p.nodes {
		nodes = append(nodes, api.NodeState{
			Name:      v.name,
			Status:    v.status,
			StartTime: v.startTime,
			EndTime:   v.endTime,
		})
	}
	return api.PipelineState{
		Name:       p.spec.Name,
		Status:     p.status,
		Nodes:      nodes,
		CreateTime: p.createTime,
		StartTime:  p.startTime,
		EndTime:    p.endTime,
	}, nil
}

func (s *inMemory) NodeState(ctx context.Context, pid, nodename string) (api.NodeState, error) {
	p, exists := s.pipelines[pid]
	if !exists {
		return api.NodeState{}, NotFoundError(fmt.Sprintf("process %s", pid))
	}
	node, exists := p.nodes[nodename]
	if !exists {
		return api.NodeState{}, NotFoundError(fmt.Sprintf("node %s", nodename))
	}
	var jobs []api.JobState
	for _, v := range node.jobs {
		jobs = append(jobs, api.JobState{
			ID:        v.jobID,
			Status:    v.status,
			StartTime: v.startTime,
			EndTime:   v.endTime,
		})
	}
	return api.NodeState{
		Name:      node.name,
		Status:    node.status,
		Jobs:      jobs,
		StartTime: node.startTime,
		EndTime:   node.endTime,
	}, nil
}

func (s *inMemory) JobState(ctx context.Context, pid, nodename, jobID string) (api.JobState, error) {
	p, exists := s.pipelines[pid]
	if !exists {
		return api.JobState{}, NotFoundError(fmt.Sprintf("process %s", pid))
	}
	node, exists := p.nodes[nodename]
	if !exists {
		return api.JobState{}, NotFoundError(fmt.Sprintf("node %s", nodename))
	}

	job, exists := node.jobs[jobID]
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

func (s *inMemory) JobResult(ctx context.Context, pid, nodename, jobid string) (interface{}, error) {
	p, exists := s.pipelines[pid]
	if !exists {
		return api.JobState{}, NotFoundError(fmt.Sprintf("process %s", pid))
	}
	node, exists := p.nodes[nodename]
	if !exists {
		return api.JobState{}, NotFoundError(fmt.Sprintf("node %s", nodename))
	}

	job, exists := node.jobs[jobid]
	if !exists {
		return api.JobState{}, NotFoundError(fmt.Sprintf("job %s", jobid))
	}
	return job.result, nil
}

func (s *inMemory) SetNodeRunning(ctx context.Context, pid, nodename string) error {
	p, exists := s.pipelines[pid]
	if !exists {
		return NotFoundError(fmt.Sprintf("process %s", pid))
	}
	node, exists := p.nodes[nodename]
	if !exists {
		return NotFoundError(fmt.Sprintf("node %s", nodename))
	}
	if !node.status.Finished() {
		node.status = api.StatusRunning
	}
	return nil
}
