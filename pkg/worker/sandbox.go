package worker

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"poseidon/pkg/api"
	"poseidon/pkg/util/context"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

const (
	envSandboxPort = "PORT" //http port used in sandbox mode, if not set 8080 is used
)

// Start starts an http server to handle the given function.
// This mode is for development/testing purpose.
// Route is POST /
// Response code 200 is equivalent to type SUCCESS, 500 to type ERROR
func sandbox(ctx context.Context, f Func) error {
	// Http port to use
	port := os.Getenv(envSandboxPort)
	if port == "" {
		port = "8080"
	}

	r := mux.NewRouter()
	r.HandleFunc("/", handleSandbox(ctx, f)).Methods(http.MethodPost)
	ctx.Logger().Infof("Sandbox server started on 127.0.0.1:%s", port)
	return http.ListenAndServe(fmt.Sprintf(":%s", port), r)
}

func handleSandbox(ctx context.Context, f Func) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		req, err := decode(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		ctx = contextFromHeaders(ctx, r.Header)

		resp, err := f(ctx, req)
		if err != nil {
			ctx.Logger().Error(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if asByteArray, isByteArray := resp.([]byte); isByteArray {
			// Byte array, respond as octet-stream
			w.Header().Set("content-type", "application/octet-stream")
			w.WriteHeader(http.StatusOK)
			w.Write(asByteArray)
		} else {
			// JSON
			w.Header().Set("content-type", "application/octet-stream")
			w.WriteHeader(http.StatusOK)
			enc := json.NewEncoder(w)
			err = enc.Encode(resp)
			if err != nil {
				ctx.Logger().Error(err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}

	}
}

func decode(r *http.Request) (interface{}, error) {
	var res interface{}
	switch r.Header.Get("content-type") {
	case "application/json", "": //if no content-type, assuming json
		defer r.Body.Close()
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&res)
		if err != nil {
			return nil, errors.Wrap(err, "cannot decode request body")
		}
	default:
		return nil, errors.New("only 'content-type: application/json' is supported")
	}
	return res, nil
}

func contextFromHeaders(ctx context.Context, headers http.Header) context.Context {
	// ProcessID
	pid := headers.Get(api.HeaderProcessID)
	if pid == "" {
		pid = "pid"
	}
	c := context.WithProcessID(ctx, pid)

	// TaskID
	taskID := headers.Get(api.HeaderTaskID)
	if taskID == "" {
		taskID = "A"
	}
	c = context.WithTaskID(c, taskID)

	// JobID
	jobID := headers.Get(api.HeaderJobID)
	if jobID == "" {
		jobID = "0"
	}
	c = context.WithJobID(c, jobID)

	// ExecutionID
	execID := headers.Get(api.HeaderExecutionID)
	if execID == "" {
		execID = uuid.New().String()
	}
	c = context.WithExecutionID(c, execID)
	c = context.WithCorrelationID(c, uuid.New().String())
	return c
}
