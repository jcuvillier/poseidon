package main

import (
	"net/http"
	"poseidon/pkg/api"
	"poseidon/pkg/context"

	"github.com/labstack/echo/v4"
)

// ListPipelinesResponse is the response struct for the list pipelines endpoint
type ListPipelinesResponse struct {
	Pipelines []api.PipelineInfo `json:"pipelines"`
}

func (h handlers) ListPipelines(c echo.Context) error {
	ctx := context.FromContext(c.Request().Context())
	pipelines, err := h.sc.ListPipelines(ctx)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err)
	}
	return c.JSON(http.StatusOK, ListPipelinesResponse{
		Pipelines: pipelines,
	})
}
