package command

import (
	"net"
	"strconv"

	"github.com/artor1os/dkv/master"
	"github.com/artor1os/dkv/persist"
	"github.com/artor1os/dkv/rpc"
	"github.com/artor1os/dkv/util"
	"github.com/artor1os/dkv/zookeeper"
)

var (
	m MasterOptions
)

type MasterOptions struct {
	port    *int
	ip      *string
	peers   *string
	me      *int
	dataDir *string
	zk      *string
}

func init() {
	cmdMaster.Run = runMaster // break init cycle
	m.port = cmdMaster.Flag.Int("port", 8111, "rpc listen port")
	m.ip = cmdMaster.Flag.String("ip", util.DetectedHostAddress(), "master <ip>|<server> address")
	m.peers = cmdMaster.Flag.String("peers", "", "all master nodes in comma separated ip:port list, example: 127.0.0.1:9093,127.0.0.1:9094,127.0.0.1:9095")
	m.me = cmdMaster.Flag.Int("me", 0, "my id")
	m.dataDir = cmdMaster.Flag.String("dataDir", "/var/lib/dkv", "data directory")
	m.zk = cmdMaster.Flag.String("zk", "", "zk servers")
}

var cmdMaster = &Command{
	UsageLine: "master -port=9333",
	Short:     "start a master server",
}

func runMaster(cmd *Command, args []string) bool {
	startMaster(m)
	return true
}

func startMaster(options MasterOptions) {
	if *options.zk == "" {
		eps, err := rpc.MakeEndpoints(util.ParsePeers(*options.peers))
		if err != nil {
			panic(err)
		}
		master.NewServer(eps, *options.me, persist.New(*options.dataDir))
	} else {
		zk, err := zookeeper.New(util.ParsePeers(*options.zk))
		if err != nil {
			panic(err)
		}
		master.NewServerWithZK(zk)
	}
	if err := rpc.Start(net.JoinHostPort(*options.ip, strconv.Itoa(*options.port))); err != nil {
		panic(err)
	}
	select {}
}
