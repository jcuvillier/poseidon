package main

import (
	"net/http"
	"poseidon/pkg/client"
	"poseidon/pkg/store"
	"poseidon/pkg/util/context"

	"github.com/labstack/echo/v4"
	"github.com/pkg/errors"
)

func (h handlers) TaskState(c echo.Context) error {
	ctx := context.FromContext(c.Request().Context())
	pid := c.Param(client.ProcessIDParam)
	nodename := c.Param(client.TaskIDParam)
	state, err := h.store.GetTaskState(ctx, pid, nodename)
	if err != nil {
		if errors.As(errors.Cause(err), &store.ErrNotFound{}) {
			return echo.NewHTTPError(http.StatusNotFound, err.Error())
		}
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusOK, state)
}
