package main

import (
	"fmt"
	"net/http"
	"os"
	"poseidon/pkg/broker"
	"poseidon/pkg/client"
	"poseidon/pkg/context"
	"poseidon/pkg/executor"
	"poseidon/pkg/pipeline"
	"poseidon/pkg/store"

	"github.com/labstack/echo/v4"
	"github.com/neko-neko/echo-logrus/v2/log"
	"github.com/pkg/errors"
)

const (
	processIDParam = "pid"
	nodenameParam  = "nodename"
	jobIDParam     = "jobid"
)

func main() {
	// Create context, echo object and set logger
	e := echo.New()
	ctx := context.Background()
	l := log.MyLogger{Logger: ctx.Logger().Logger}
	e.Logger = &l

	//Instantiate pipeline engine
	pip, err := NewPipelineEngine(ctx)
	if err != nil {
		e.Logger.Fatal(errors.Wrap(err, "failed to instantiate pipeline engine"))
		os.Exit(1)
	}

	//Setup routes
	h := handlers{
		p: pip,
	}
	e.GET("/", func(c echo.Context) error {
		return c.String(http.StatusOK, "Hello, World!")
	})
	e.Add(client.SubmitMethod, client.SubmitPath, h.Submit)
	e.GET("/pipelines", h.ListPipelines)
	e.Add(client.PipelineStateMethod, client.PipelineStatePath, h.PipelineState)
	e.Add(client.NodeStateMethod, client.NodeStatePath, h.NodeState)
	e.GET(fmt.Sprintf("/pipelines/:%s/nodes/:%s/state", processIDParam, nodenameParam), h.NodeState)
	e.GET(fmt.Sprintf("/pipelines/:%s/nodes/:%s/result", processIDParam, nodenameParam), h.NodeResult)
	e.GET(fmt.Sprintf("/pipelines/:%s/nodes/:%s/jobs/:%s/result", processIDParam, nodenameParam, jobIDParam), h.JobResult)

	e.HideBanner = true
	e.HidePort = true

	port := "8080"
	e.Logger.Infof("http server started on 127.0.0.1:%s", port)
	e.Logger.Fatal(e.Start(fmt.Sprintf(":%s", port)))
}

// NewPipelineEngine instantiate a new pipeline engine
func NewPipelineEngine(ctx context.Context) (pipeline.Pipeline, error) {
	b, err := broker.NewFromEnv(ctx)
	if err != nil {
		return nil, err
	}

	s, err := store.NewInMemoryStore()
	if err != nil {
		return nil, err
	}

	exec, err := executor.New(b, "poseidon.ex.process", s)
	if err != nil {
		return nil, err
	}

	pip, err := pipeline.New(exec, s)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		if err := b.Receive(ctx, exec.HandleEvent, nil, "poseidon.q.events"); err != nil {
			ctx.Logger().Fatal(err)
			os.Exit(1)
		}
	}()
	return pip, nil
}

type handlers struct {
	p pipeline.Pipeline
}
