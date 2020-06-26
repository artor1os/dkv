package command

import (
	"net"
	"strconv"

	"github.com/artor1os/dkv/persist"
	"github.com/artor1os/dkv/replica"
	"github.com/artor1os/dkv/rpc"
	"github.com/artor1os/dkv/util"
	"github.com/artor1os/dkv/zookeeper"
)

var (
	r ReplicaOptions
)

type ReplicaOptions struct {
	port    *int
	ip      *string
	peers   *string
	masters *string
	me      *int
	gid     *int
	dataDir *string
	zk      *string
	isr     *int
}

func init() {
	cmdReplica.Run = runReplica
	r.port = cmdReplica.Flag.Int("port", 9111, "rpc listen port")
	r.ip = cmdReplica.Flag.String("ip", util.DetectedHostAddress(), "replica <ip>|<server> address")
	r.peers = cmdReplica.Flag.String("peers", "", "all replica nodes in comma separated ip:port list, example: 127.0.0.1:9093,127.0.0.1:9094,127.0.0.1:9095")
	r.masters = cmdReplica.Flag.String("masters", "", "all master nodes")
	r.me = cmdReplica.Flag.Int("me", 0, "my id")
	r.gid = cmdReplica.Flag.Int("gid", 100, "my group id")
	r.dataDir = cmdReplica.Flag.String("dataDir", "/var/lib/dkv", "data directory")
	r.zk = cmdReplica.Flag.String("zk", "", "zk servers")
	r.isr = cmdReplica.Flag.Int("isr", 2, "minimum in-sync replica to agree")
}

var cmdReplica = &Command{
	UsageLine: "replica -port=9333",
	Short:     "start a replica server",
}

func runReplica(cmd *Command, args []string) bool {
	startReplica(r)
	return true
}

func startReplica(options ReplicaOptions) {
	servers, err := rpc.MakeEndpoints(util.ParsePeers(*options.peers))
	if err != nil {
		panic(err)
	}
	masters, err := rpc.MakeEndpoints(util.ParsePeers(*options.masters))
	if err != nil {
		panic(err)
	}
	if *options.zk == "" {
		replica.NewServer(servers, *options.me, persist.New(*options.dataDir), 1000, *options.gid, masters, rpc.MakeEndpoint)
	} else {
		zk, err := zookeeper.New(util.ParsePeers(*options.zk))
		if err != nil {
			panic(err)
		}
		replica.NewServerZK(servers, *options.me, persist.New(*options.dataDir), *options.gid, masters, rpc.MakeEndpoint, zk, *options.isr)
	}
	if err := rpc.Start(net.JoinHostPort(*options.ip, strconv.Itoa(*options.port))); err != nil {
		panic(err)
	}
	select {}
}
