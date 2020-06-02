FROM golang:1.14

WORKDIR /go/src/app
COPY . .

RUN go env -w GOPROXY=https://goproxy.io,direct && \
    go get -d -v ./... && \
    go install -v ./...

CMD [ "dkv" ]
