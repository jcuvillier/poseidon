package main

import (
	"bufio"
	"fmt"
	"io"
	"os/exec"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"poseidon/pkg/context"
	"poseidon/pkg/worker"

	"github.com/mitchellh/mapstructure"
)

func main() {
	worker.Start(command)
}

type logLevel int

const (
	levelInfo logLevel = iota
	levelError
)

type CommandRequest struct {
	Command []interface{} `json:"command"`
	Env     []EnvVar      `json:"env"`
	Output  string        `json:"output"`
}

type EnvVar struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

func command(ctx context.Context, req interface{}) (interface{}, error) {
	var r CommandRequest
	err := mapstructure.Decode(req, &r)
	if err != nil {
		return nil, err
	}

	// Create command to execute
	var name string
	var args []string
	for i, c := range r.Command {
		if i == 0 {
			name = fmt.Sprintf("%v", c)
		} else {
			args = append(args, fmt.Sprintf("%v", c))
		}
	}
	cmd := exec.CommandContext(ctx, name, args...)
	ctx.Logger().Infof("starting command '%s'", cmd.String())

	// Get stdout and stderr readers
	stdoutIn, err := cmd.StdoutPipe()
	if err != nil {
		return nil, errors.Wrap(err, "cannot read stdout")
	}
	stderrIn, err := cmd.StderrPipe()
	if err != nil {
		return nil, errors.Wrap(err, "cannot read stderr")
	}

	// Start command
	err = cmd.Start()
	if err != nil {
		return nil, errors.Wrap(err, "cannot start command")
	}

	// cmd.Wait() should be called only after we finish reading
	// from stdoutIn and stderrIn.
	// wg ensures that we finish
	var errStdout, errStderr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		errStdout = log(ctx, stdoutIn, logrus.InfoLevel)
		wg.Done()
	}()
	errStderr = log(ctx, stderrIn, logrus.ErrorLevel)
	wg.Wait()

	err = cmd.Wait()
	if err != nil {
		return nil, errors.Wrap(err, "command failed")
	}
	if errStdout != nil || errStderr != nil {
		return nil, errors.Wrap(err, "cannot capture stdout or stderr")
	}

	return nil, nil
}

func log(ctx context.Context, r io.ReadCloser, level logrus.Level) error {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		ctx.Logger().Log(level, line)
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}
