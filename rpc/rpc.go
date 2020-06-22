package rpc

import (
	"net"
	"net/http"
	"net/rpc"
)

type Endpoint interface {
	Call(string, interface{}, interface{}) bool
}

type endpoint struct {
	cli *rpc.Client
}

func (e *endpoint) Call(method string, args interface{}, reply interface{}) bool {
	err := e.cli.Call(method, args, reply)
	if err != nil {
		// TODO
		return false
	}
	return true
}

func MakeEndpoint(peer string) Endpoint {
	cli, err := rpc.DialHTTP("tcp", peer)
	if err != nil {
		panic(err)
	}
	return &endpoint{cli: cli}
}

func MakeEndpoints(peers []string) ([]Endpoint, error) {
	var endpoints []Endpoint
	for _, p := range peers {
		cli, err := rpc.DialHTTP("tcp", p) // TODO: should retry here
		if err != nil {
			return nil, err
		}
		endpoints = append(endpoints, &endpoint{cli: cli})
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
