FROM golang:1.14-buster AS build-env

ADD . /src
RUN cd /src && CGO_ENABLED=0 go build -o controller poseidon/app/controller

FROM busybox

COPY --from=build-env /src/controller /app/
COPY --from=build-env /etc/ssl/certs /etc/ssl/certs

CMD ["/app/controller"]