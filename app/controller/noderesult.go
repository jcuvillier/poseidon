package main

import (
	"net/http"
	"poseidon/pkg/context"
	"poseidon/pkg/store"

	"github.com/labstack/echo/v4"
	"github.com/pkg/errors"
)

func (h handlers) NodeResult(c echo.Context) error {
	ctx := context.FromContext(c.Request().Context())
	pid := c.Param(processIDParam)
	nodename := c.Param(nodenameParam)
	result, err := h.p.Executor().NodeResult(ctx, pid, nodename)
	if err != nil {
		if errors.As(errors.Cause(err), &store.ErrNotFound{}) {
			return echo.NewHTTPError(http.StatusNotFound, err.Error())
		}
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusOK, result)
}
