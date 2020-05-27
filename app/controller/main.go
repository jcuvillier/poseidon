package main

import (
	"fmt"
	"net/http"
	"os"
	"poseidon/pkg/broker"
	"poseidon/pkg/client"
	"poseidon/pkg/executor/triton"
	"poseidon/pkg/executor/triton/workload"
	"poseidon/pkg/scheduler"
	"poseidon/pkg/store"
	"poseidon/pkg/util/context"

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
	tsc, err := NewTaskScheduler(ctx, store)
	if err != nil {
		e.Logger.Fatal(errors.Wrap(err, "failed to instantiate task scheduler"))
		os.Exit(1)
	}
	psc, err := scheduler.NewPipelineScheduler(tsc, store)
	if err != nil {
		e.Logger.Fatal(errors.Wrap(err, "failed to instantiate pipeline scheduler"))
		os.Exit(1)
	}

	//Setup routes
	h := handlers{
		sc:    psc,
		store: store,
	}
	e.GET("/", func(c echo.Context) error {
		return c.String(http.StatusOK, "Hello, World!")
	})
	e.Add(client.SubmitMethod, client.SubmitPath, h.Submit)
	e.GET("/pipelines", h.ListPipelines)
	e.Add(client.PipelineStateMethod, client.PipelineStatePath, h.PipelineState)
	e.Add(client.TaskStateMethod, client.TaskStatePath, h.TaskState)

	e.HideBanner = true
	e.HidePort = true

	port := "8080"
	e.Logger.Infof("http server started on 127.0.0.1:%s", port)
	e.Logger.Fatal(e.Start(fmt.Sprintf(":%s", port)))
}

// NewTaskScheduler instantiate a new task scheduler
func NewTaskScheduler(ctx context.Context, s store.TaskSchedulerStore) (scheduler.TaskScheduler, error) {
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

	exec, err := triton.New(ctx, b, "poseidon.ex.process", "poseidon.q.events", w)
	if err != nil {
		return nil, err
	}

	tsc, err := scheduler.NewTaskScheduler(ctx, s)
	if err != nil {
		log.Fatal(err)
	}
	tsc.RegisterExecutor("triton", exec)

	return tsc, nil
}

type handlers struct {
	sc    scheduler.PipelineScheduler
	store store.Store
}
