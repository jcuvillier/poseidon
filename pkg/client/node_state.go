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

// NodeStateResponse is the response of the NodeState endpoint.
type NodeStateResponse api.NodeState

const (
	// NodeStateMethod is http method used for endpoint NodeState
	NodeStateMethod     = http.MethodGet
	nodeStatePathFormat = "/pipelines/%s/nodes/%s/state"
)

var (
	// NodeStatePath is the path definition of the endpoint NodeState.
	NodeStatePath = fmt.Sprintf(nodeStatePathFormat, fmt.Sprintf(":%s", ProcessIDParam), fmt.Sprintf(":%s", NodenameParam))
)

func (cli client) NodeState(ctx context.Context, pid, nodename string) (NodeStateResponse, error) {
	req, err := retryablehttp.NewRequest(NodeStateMethod, fmt.Sprintf(cli.uri+nodeStatePathFormat, pid, nodename), nil)
	if err != nil {
		return NodeStateResponse{}, errors.Wrap(err, "cannot create request")
	}
	resp, err := cli.httpcli.Do(req.WithContext(context.Background()))
	if err != nil {
		return NodeStateResponse{}, errors.Wrap(err, "cannot do request")
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return NodeStateResponse{}, ErrNotFound{fmt.Sprintf("pipeline %s or node %s", pid, nodename)}
	}

	var res NodeStateResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return NodeStateResponse{}, errors.Wrap(err, "cannot decode response")
	}
	return res, nil
}
