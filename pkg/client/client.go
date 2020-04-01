package client

import (
	"context"
	"poseidon/pkg/api"
	"strings"

	"github.com/hashicorp/go-retryablehttp"
)

const (
	// ProcessIDParam is the param definition for ProcessID
	ProcessIDParam = "pid"
)

// Client is the API client that performs all operations to a poseidon server
type Client interface {
	// Submit submits a new pipeline with the given spec and arguments.
	// It returns a process identifier.
	Submit(ctx context.Context, spec api.PipelineSpec, args interface{}) (string, error)

	// PipelineState returns the state of a pipeline.
	PipelineState(ctx context.Context, pid string) (PipelineStateResponse, error)
}

// NewClient creates a Poseidon client
func NewClient(uri string) (Client, error) {
	httpcli := retryablehttp.NewClient()
	httpcli.Logger = nil
	u := strings.TrimRight(uri, "/")
	return client{
		httpcli: httpcli,
		uri:     u,
	}, nil
}

type client struct {
	httpcli *retryablehttp.Client
	uri     string
}
