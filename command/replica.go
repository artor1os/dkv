package command

import (
	"net"
	"strconv"

	"github.com/artor1os/dkv/master"
	"github.com/artor1os/dkv/persist"
	"github.com/artor1os/dkv/replica"
	"github.com/artor1os/dkv/rpc"
	"github.com/artor1os/dkv/util"
	"github.com/artor1os/dkv/zookeeper"
	log "github.com/sirupsen/logrus"
)

var (
	r ReplicaOptions
)

type ReplicaOptions struct {
	port    *int
	ip      *string
	peers   *int
	masters *int
	me      *int
	gid     *int
	dataDir *string
	zk      *string
	isr     *int
	schema *string
}

func init() {
	cmdReplica.Run = runReplica
	r.port = cmdReplica.Flag.Int("port", 9111, "rpc listen port")
	r.ip = cmdReplica.Flag.String("ip", util.DetectedHostAddress(), "replica <ip>|<server> address")
	r.peers = cmdReplica.Flag.Int("peers", 3, "number of peers")
	r.masters = cmdReplica.Flag.Int("masters", 3, "number of masters")
	r.me = cmdReplica.Flag.Int("me", 0, "my id")
	r.gid = cmdReplica.Flag.Int("gid", 100, "my group id")
	r.dataDir = cmdReplica.Flag.String("dataDir", "/var/lib/dkv", "data directory")
	r.zk = cmdReplica.Flag.String("zk", "", "zk servers")
	r.isr = cmdReplica.Flag.Int("isr", 2, "minimum in-sync replica to agree")
	r.schema = cmdReplica.Flag.String("schema", "raft", "replica schema, raft or zk")
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
	zk, err := zookeeper.New(util.ParsePeers(*options.zk))
	if err != nil {
		panic(err)
	}
	servers := rpc.MakeEndpoints(zk, zookeeper.GroupPath, *options.gid, *options.peers)
	masters := rpc.MakeEndpoints(zk, zookeeper.MasterPath, 0, *options.masters)
	if *options.schema == "raft" {
		replica.NewServer(servers, *options.me, persist.New(*options.dataDir), 1000, *options.gid, masters, rpc.MakeEndpointsReplica, zk)
	} else {
		replica.NewServerZK(servers, *options.me, persist.New(*options.dataDir), *options.gid, masters, rpc.MakeEndpointsReplica, zk, *options.isr)
	}
	addr := net.JoinHostPort(*options.ip, strconv.Itoa(*options.port))
	if err := rpc.Start(addr); err != nil {
		panic(err)
	}
	initJoin := func() error {
		m := master.NewClient(masters)
		m.Join(map[int]int{*options.gid: *options.peers})
		return nil
	}
	initZK := func() error {
		if *options.schema == "zk" {
			util.WaitSuccess(func() error {
				return zk.CreateIfNotExist(zookeeper.ISRPath)
			}, nil, nil)
			util.WaitSuccess(func() error {
				return zk.CreateIfNotExist(zookeeper.CommitIndexPath)
			}, nil, nil)
			isrPath := zookeeper.MakeGroupPath(zookeeper.ISRPath, *options.gid)
			commitPath := zookeeper.MakeGroupPath(zookeeper.CommitIndexPath, *options.gid)
			util.WaitSuccess(func() error {
				return zk.CreateIfNotExist(isrPath, func() error {
					for p := 0; p < *options.peers; p++ {
						go util.WaitSuccess(func() error {
							return zk.Add(isrPath, p)
						}, nil, nil)
					}
					return nil
				})
			}, nil, nil)
			util.WaitSuccess(func() error {
				return zk.CreateIfNotExist(commitPath, func() error {
					util.WaitSuccess(func() error {
						return zk.SetData(commitPath, []byte(strconv.Itoa(0)))
					}, nil, nil)
					return nil
				})
			}, nil, nil)
		}
		return nil
	}
	util.WaitSuccess(func() error {
		return zk.Register(zookeeper.GroupPath, *options.gid, *options.me, addr, initJoin, initZK)
	}, func() {
		log.WithError(err).
			WithField("gid", *options.gid).
			WithField("me", *options.me).
			Info("failed to register replica")
	}, func() {
		log.WithField("gid", *options.gid).
			WithField("me", *options.me).
			Info("successfully registered replica")
	})
	select {}
}
