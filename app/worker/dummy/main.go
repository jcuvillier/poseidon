package main

import (
	"fmt"
	"poseidon/pkg/util/context"
	"poseidon/pkg/worker"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
)

func main() {
	worker.Start(dummy)
}

type Request struct {
	Error   bool        `json:"error"`
	Payload interface{} `json:"payload"`
	Delay   string      `json:"delay"`
}

func dummy(ctx context.Context, req interface{}) (interface{}, error) {
	var r Request
	err := mapstructure.Decode(req, &r)
	if err != nil {
		return nil, err
	}
	fmt.Println(r)

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
