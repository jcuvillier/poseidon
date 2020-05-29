package dummy

import (
	"poseidon/pkg/util/context"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
)

type request struct {
	Error   bool        `json:"error"`
	Payload interface{} `json:"payload"`
	Delay   string      `json:"delay"`
}

func handle(ctx context.Context, req interface{}) (interface{}, error) {
	var r request
	err := mapstructure.Decode(req, &r)
	if err != nil {
		return nil, err
	}

	if r.Delay != "" {
		d, err := time.ParseDuration(r.Delay)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot parse delay '%s'", r.Delay)
		}
		ctx.Logger().Infof("sleeping for %s", d.String())
		time.Sleep(d)
	}

	if r.Error {
		return nil, errors.New("dummy error")
	}

	return r.Payload, nil
}
