package rpc

import (
	"net"
	"net/http"
	"net/rpc"

	log "github.com/sirupsen/logrus"
)

type Endpoint interface {
	Call(string, interface{}, interface{}) bool
}

type endpoint struct {
	addr string
	cli *rpc.Client
}

func (e *endpoint) Call(method string, args interface{}, reply interface{}) bool {
	var err error
	for e.cli == nil {
		e.cli, err = rpc.DialHTTP("tcp", e.addr)
		if err != nil {
			log.WithError(err).WithField("addr", e.addr).Warnf("retry connecting to server")
			// TODO
		}
	}

	err = e.cli.Call(method, args, reply)
	if err != nil {
		log.WithError(err).Error("rpc error")
		return false
	}
	return true
}

func MakeEndpoint(peer string) Endpoint {
	return &endpoint{addr:peer}
}

func MakeEndpoints(peers []string) ([]Endpoint, error) {
	var endpoints []Endpoint
	for _, p := range peers {
		endpoints = append(endpoints, &endpoint{addr:p})
	}
	return endpoints, nil
}

func Register(rcvr interface{}) error {
	return rpc.Register(rcvr)
}

func Start(addr string) error {
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	go http.Serve(l, nil)
	return nil
}
