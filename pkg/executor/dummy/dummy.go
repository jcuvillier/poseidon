package dummy

import (
	"poseidon/pkg/api"
	"poseidon/pkg/executor"
	"poseidon/pkg/util/context"

	"github.com/pkg/errors"
)

type dummy struct {
	callback executor.CallbackFunc
}

// New returns a new triton executor with the given components
func New(ctx context.Context) (executor.Executor, error) {
	return &dummy{}, nil
}

func (d *dummy) Start(ctx context.Context, spec interface{}, n int) error {
	return nil
}

func (d *dummy) Stop(ctx context.Context, graceful bool) error {
	return nil
}

func (d *dummy) SubmitJob(ctx context.Context, jobID string, param interface{}) error {
	go func() {
		ctx = context.WithJobID(ctx, jobID)
		if cerr := d.callback(ctx, struct{}{}, api.StatusRunning); cerr != nil {
			ctx.Logger().Error(errors.Wrap(cerr, "cannot call executor callback function"))
		}
		res, err := handle(ctx, param)
		if err != nil {
			if cerr := d.callback(ctx, executor.JobError{Message: err.Error()}, api.StatusFailed); cerr != nil {
				ctx.Logger().Error(errors.Wrap(cerr, "cannot call executor callback function"))
			}
		} else {
			if cerr := d.callback(ctx, res, api.StatusCompleted); cerr != nil {
				ctx.Logger().Error(errors.Wrap(cerr, "cannot call executor callback function"))
			}
		}
	}()
	return nil
}

func (d *dummy) SetCallbackFunc(f executor.CallbackFunc) {
	d.callback = f
}
