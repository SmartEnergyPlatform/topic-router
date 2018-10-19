FROM golang:1.11


COPY . /go/src/topic-router
WORKDIR /go/src/topic-router

ENV GO111MODULE=on

RUN go build

CMD ./topic-router