package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"poseidon/pkg/api"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/pkg/errors"
)

// TaskStateResponse is the response of the TaskState endpoint.
type TaskStateResponse api.TaskState

const (
	// TaskStateMethod is http method used for endpoint TaskState
	TaskStateMethod     = http.MethodGet
	taskStatePathFormat = "/api/pipelines/%s/tasks/%s/state"
)

var (
	// TaskStatePath is the path definition of the endpoint TaskState.
	TaskStatePath = fmt.Sprintf(taskStatePathFormat, fmt.Sprintf(":%s", ProcessIDParam), fmt.Sprintf(":%s", TaskIDParam))
)

func (cli client) TaskState(ctx context.Context, processID, taskID string) (TaskStateResponse, error) {
	req, err := retryablehttp.NewRequest(TaskStateMethod, fmt.Sprintf(cli.uri+taskStatePathFormat, processID, taskID), nil)
	if err != nil {
		return TaskStateResponse{}, errors.Wrap(err, "cannot create request")
	}
	resp, err := cli.httpcli.Do(req.WithContext(context.Background()))
	if err != nil {
		return TaskStateResponse{}, errors.Wrap(err, "cannot do request")
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return TaskStateResponse{}, ErrNotFound{fmt.Sprintf("pipeline %s or node %s", processID, taskID)}
	}

	var res TaskStateResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return TaskStateResponse{}, errors.Wrap(err, "cannot decode response")
	}
	return res, nil
}
