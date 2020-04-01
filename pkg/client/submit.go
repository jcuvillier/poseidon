package client

import (
	"context"
	"encoding/json"
	"net/http"
	"poseidon/pkg/api"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/pkg/errors"
)

const (
	// SubmitMethod is http method used for endpoint Submit
	SubmitMethod = http.MethodPost
	// SubmitPath is the path definition of the endpoint Submit.
	SubmitPath = "/pipeline"
)

// SubmitRequest is the request structure for the Submit endpoint
type SubmitRequest struct {
	api.PipelineSpec
	Args interface{} `json:"args"`
}

// SubmitResponse is the response structure for the Submit endpoint
type SubmitResponse struct {
	ProcessID string `json:"processID"`
}

func (cli client) Submit(ctx context.Context, spec api.PipelineSpec, args interface{}) (string, error) {
	body, err := json.Marshal(SubmitRequest{
		PipelineSpec: spec,
		Args:         args,
	})
	if err != nil {
		return "", errors.Wrap(err, "cannot marshal request")
	}

	req, err := retryablehttp.NewRequest(SubmitMethod, cli.uri+SubmitPath, body)
	if err != nil {
		return "", errors.Wrap(err, "cannot create request")
	}
	req.Header.Set("content-type", "application/json")

	resp, err := cli.httpcli.Do(req.WithContext(context.Background()))
	if err != nil {
		return "", errors.Wrap(err, "cannot do request")
	}
	defer resp.Body.Close()
	dec := json.NewDecoder(resp.Body)

	if resp.StatusCode == 400 {
		// TODO: Rework with a more appropriate ErrPipelineNotValid with more information
		var httpErr HTTPError
		if err := dec.Decode(&httpErr); err != nil {
			//Cannot decode error
			return "", ErrBadRequest{errors.New("bad request")}
		}
		return "", ErrBadRequest{errors.Wrap(httpErr, "pipeline is not valid")}
	}
	var res SubmitResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return "", errors.Wrap(err, "cannot decode response")
	}
	return res.ProcessID, nil
}
