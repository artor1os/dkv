package command

import (
	"net"
	"strconv"

	"github.com/artor1os/dkv/master"
	"github.com/artor1os/dkv/persist"
	"github.com/artor1os/dkv/rpc"
	"github.com/artor1os/dkv/util"
	"github.com/artor1os/dkv/zookeeper"
	log "github.com/sirupsen/logrus"
)

var (
	m MasterOptions
)

type MasterOptions struct {
	port    *int
	ip      *string
	peers   *int
	me      *int
	dataDir *string
	zk      *string
	schema  *string
	debug   *bool
}

func init() {
	cmdMaster.Run = runMaster // break init cycle
	m.port = cmdMaster.Flag.Int("port", 8111, "rpc listen port")
	m.ip = cmdMaster.Flag.String("ip", util.DetectedHostAddress(), "master <ip>|<server> address")
	m.peers = cmdMaster.Flag.Int("peers", 3, "number of peers")
	m.me = cmdMaster.Flag.Int("me", 0, "my id")
	m.dataDir = cmdMaster.Flag.String("dataDir", "/var/lib/dkv", "data1 directory")
	m.zk = cmdMaster.Flag.String("zk", "", "zk servers")
	m.schema = cmdMaster.Flag.String("schema", "raft", "raft or zk")
	m.debug = cmdMaster.Flag.Bool("debug", true, "debug log")
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
	if *options.debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}
	zk, err := zookeeper.New(util.ParsePeers(*options.zk))
	if err != nil {
		panic(err)
	}
	if *options.schema == "raft" {
		eps := rpc.MakeEndpoints(zk, zookeeper.MasterPath, 0, *options.peers)
		master.NewServer(eps, *options.me, persist.New(*options.dataDir))
	} else {
		master.NewServerZK(zk)
	}
	addr := net.JoinHostPort(*options.ip, strconv.Itoa(*options.port))
	if err := rpc.Start(addr); err != nil {
		panic(err)
	}
	util.WaitSuccess(func() error {
		return zk.Register(zookeeper.MasterPath, 0, *options.me, addr)
	}, func(err error) {
		log.WithError(err).
			WithField("me", *options.me).
			Info("failed to register master")
	}, func() {
		log.WithField("me", *options.me).Info("successfully registered master")
	})
	select {}
}
