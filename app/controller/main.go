package main

import (
	"fmt"
	"net/http"
	"os"
	"poseidon/pkg/broker"
	"poseidon/pkg/client"
	"poseidon/pkg/util/context"
	"poseidon/pkg/executor/triton"
	"poseidon/pkg/executor/triton/workload"
	"poseidon/pkg/scheduler"
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

	store, err := store.NewInMemoryStore()
	if err != nil {
		e.Logger.Fatal(errors.Wrap(err, "failed to instantiate store"))
		os.Exit(1)
	}

	//Instantiate pipeline engine
	sc, err := NewScheduler(ctx, store)
	if err != nil {
		e.Logger.Fatal(errors.Wrap(err, "failed to instantiate scheduler"))
		os.Exit(1)
	}

	//Setup routes
	h := handlers{
		sc:    sc,
		store: store,
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

// NewScheduler instantiate a new pipeline scheduler
func NewScheduler(ctx context.Context, s store.Store) (scheduler.Scheduler, error) {
	b, err := broker.NewFromEnv(ctx)
	if err != nil {
		return nil, err
	}

	w, err := workload.NewDockerWorkload(workload.DockerWorkloadConfig{
		Env: map[string]string{
			"BROKER_TYPE":              "RABBITMQ",
			"BROKER_RABBITMQ_USER":     "guest",
			"BROKER_RABBITMQ_PASSWORD": "guest",
			"BROKER_RABBITMQ_URI":      "127.0.0.1:5672",
		},
	})
	if err != nil {
		return nil, err
	}

	exec, err := triton.New(ctx, b, "poseidon.ex.process", "poseidon.q.events", s, w)
	if err != nil {
		return nil, err
	}

	sc, err := scheduler.NewScheduler(exec, s)
	if err != nil {
		log.Fatal(err)
	}

	return sc, nil
}

type handlers struct {
	sc    scheduler.Scheduler
	store store.Store
}
