package main

import (
	"net/http"
	"poseidon/pkg/client"
	"poseidon/pkg/context"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
)

func (h handlers) Submit(c echo.Context) error {
	ctx := context.FromContext(c.Request().Context())
	ctx = context.WithProcessID(ctx, uuid.New().String())
	ctx = context.WithCorrelationID(ctx, uuid.New().String())

	var req client.SubmitRequest
	if err := c.Bind(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}

	if err := h.p.Submit(ctx, req.PipelineSpec, req.Args); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusAccepted, client.SubmitResponse{
		ProcessID: ctx.ProcessID(),
	})
}
