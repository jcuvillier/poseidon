# Poseidon

Poseidon is a cloud-native framework to run pipeline over kubernetes.

## Rabbimq

```
docker run -d --hostname my-rabbit --name some-rabbit -p 5672:5672 -p 9080:15672 rabbitmq:3-management
```

## Curls

```
curl -v -X POST 127.0.0.1:8080/pipeline -d '{"name":"foo","nodes":[{"name":"NodeA","image":"poseidon/worker/sleep:latest","input":"#args", "parallelism":2},{"name":"NodeB","image":"poseidon/worker/sleep:latest","input":"toto","dependencies":["NodeA"]}],"args":["a","b","c","d"]}' -H "content-type: application/json"
```

```
curl -v 127.0.0.1:8080/pipelines/2f6bee0c-6d6b-4ae9-95cb-d567022cbca0/state
```

## Worker

To build workers

```
docker build -t poseidon/worker/sleep -f app/worker/sleep/Dockerfile .
```