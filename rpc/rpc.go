package rpc

import (
	"net"
	"net/http"
	"net/rpc"

	"github.com/artor1os/dkv/zookeeper"
	log "github.com/sirupsen/logrus"
)

type Endpoint interface {
	Call(string, interface{}, interface{}) bool
}

type endpoint struct {
	addr string

	path string
	gid int
	me int
	zk zookeeper.Controller
}

func (e *endpoint) Call(method string, args interface{}, reply interface{}) bool {
	if e.addr == "" {
		for {
			addr, err := e.zk.Find(e.path, e.gid, e.me)
			if err != nil {
				log.WithError(err).
					WithField("path", e.path).
					WithField("gid", e.gid).
					WithField("me", e.me).
					Info("failed to find")
				continue
			}
			e.addr = addr
			break
		}
	}

	cli, err := rpc.DialHTTP("tcp", e.addr)
	if err != nil {
		log.WithError(err).WithField("addr", e.addr).Error("failed to connect rpc server")
		return false
	}

	err = cli.Call(method, args, reply)
	if err != nil {
		log.WithError(err).Error("rpc error")
		return false
	}
	return true
}

func MakeEndpoints(zk zookeeper.Controller, path string, gid int, peers int) []Endpoint {
	var endpoints []Endpoint
	for p := 0; p < peers; p++ {
		endpoints = append(endpoints, &endpoint{zk: zk, path: path, gid: gid, me: p})
	}
	return endpoints
}

func MakeEndpointsReplica(zk zookeeper.Controller, gid int, peers int) []Endpoint {
	return MakeEndpoints(zk, zookeeper.GroupPath, gid, peers)
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
