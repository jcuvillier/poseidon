![https://github.com/jcuvillier/poseidon/actions?query=workflow%3AGo](https://github.com/jcuvillier/poseidon/workflows/Go/badge.svg)

# ![Poseidon](docs/assets/poseidon.png)

## What is Poseidon ?

## Quick Start

#### Start RabbitMQ
```
docker run -d --hostname my-rabbit --name some-rabbit -p 5672:5672 -p 9080:15672 rabbitmq:3-management
```
#### Build Dummy worker
```
docker build -t poseidon/worker/dummy -f app/worker/dummy/Dockerfile .
```
#### Start Controller
```
BROKER_TYPE=RABBITMQ BROKER_RABBITMQ_USER=guest BROKER_RABBITMQ_PASSWORD=guest BROKER_RABBITMQ_URI=localhost:5672 go run app/controller/*.go
```
#### Build Poseidon CLI
```
go build -o poseidon poseidon/app/cli
```
#### Submit Workflow
```
poseidon submit --watch examples/dummy-seq.json
```

## Key concepts

### Scalable task execution

### Powerful task input system

## Built-in workers

### Command

### HTTP