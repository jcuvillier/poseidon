package main

import (
	"net/http"
	"poseidon/pkg/store"
	"poseidon/pkg/util/context"

	"github.com/labstack/echo/v4"
	"github.com/pkg/errors"
)

func (h handlers) JobResult(c echo.Context) error {
	ctx := context.FromContext(c.Request().Context())
	pid := c.Param(processIDParam)
	nodename := c.Param(nodenameParam)
	jobid := c.Param(jobIDParam)
	result, err := h.store.GetJobResult(ctx, pid, nodename, jobid)
	if err != nil {
		if errors.As(errors.Cause(err), &store.ErrNotFound{}) {
			return echo.NewHTTPError(http.StatusNotFound, err.Error())
		}
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusOK, result)
}
