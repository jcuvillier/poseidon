# TODO

This is not ordered by priority...

## Pipeline Engine

- [ ] Postgres or Mysql store
- [ ] Volume management (pipeline level, worker level)
- [ ] Worker retries/timeout
- [ ] Artifacts
- [ ] Cancel pipeline
- [ ] Pause/Resume pipeline
- [ ] Event hooks
- [ ] Command Worker
- [x] K8s workload
- [ ] Continue on Fail
- [ ] Conditional nodes
- [ ] Pipeline in pipeline (aka pip)
- [ ] TTL
- [x] Enhance Node's input syntax to support multiple batch and dependency string in other string
- [x] Add time info in store
- [ ] Plug a tracing library
- [ ] Log management
- [ ] Add workload host info to jobs
- [ ] Allow each node to be executed with a different executor
- [ ] HTTP executor

## Resilience

- [ ] Resilience with store and broker (retries, auto reconnect etc.)
- [ ] Workload healthcheck
- [ ] Broker nack/reject policy depending on error
- [ ] Error management
- [ ] Purge mechanism to ensure there is no leaks (broker and workload artifacts)

## UX

- [x] Pipeline/Node/Job state endpoints in controller
- [x] Cli
- [ ] GUI
- [ ] OpenAPI doc
- [ ] Write an actual README.md
- [ ] Quickstart with docker workload, inmemory store, rabbitmq broker package in a docker compose
- [ ] Examples

## Technical stuffs

- [ ] Unit Tests (Naaaan !!! Tests are for looser)
- [ ] Continuous Integration (well it needs tests first)
- [ ] Pipeline/Executor/Broker/Store factory with config (file, env)
- [ ] Rework pkg/context implementation
- [ ] Event transport (maybe something like the echo.Bind)
- [ ] Rework event's data (more flexible content-type management)
