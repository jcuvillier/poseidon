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

// PipelineStateResponse is the response of the PipelineState endpoint.
type PipelineStateResponse api.PipelineState

const (
	// PipelineStateMethod is http method used for endpoint PipelineState
	PipelineStateMethod     = http.MethodGet
	pipelineStatePathFormat = "/api/pipelines/%s/state"
)

var (
	// PipelineStatePath is the path definition of the endpoint PipelineState.
	PipelineStatePath = fmt.Sprintf(pipelineStatePathFormat, fmt.Sprintf(":%s", ProcessIDParam))
)

func (cli client) PipelineState(ctx context.Context, pid string) (PipelineStateResponse, error) {
	req, err := retryablehttp.NewRequest(PipelineStateMethod, fmt.Sprintf(cli.uri+pipelineStatePathFormat, pid), nil)
	if err != nil {
		return PipelineStateResponse{}, errors.Wrap(err, "cannot create request")
	}
	resp, err := cli.httpcli.Do(req.WithContext(context.Background()))
	if err != nil {
		return PipelineStateResponse{}, errors.Wrap(err, "cannot do request")
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return PipelineStateResponse{}, ErrNotFound{fmt.Sprintf("pipeline %s", pid)}
	}

	var res PipelineStateResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return PipelineStateResponse{}, errors.Wrap(err, "cannot decode response")
	}
	return res, nil
}
