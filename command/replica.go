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
	debug *bool
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
	r.debug = cmdReplica.Flag.Bool("debug", true, "debug log")
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
	if *options.debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

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
	logger := log.WithField("gid", *options.gid).WithField("me", *options.me)
	initJoin := func() error {
		m := master.NewClient(masters)
		m.Join(map[int]int{*options.gid: *options.peers})
		return nil
	}
	initZK := func() error {
		if *options.schema == "zk" {
			util.WaitSuccess(func() error {
				return zk.CreateIfNotExist(zookeeper.ISRPath, nil)
			}, func(err error) {
				logger.WithError(err).Info("failed to create ISR root")
			}, func() {
				logger.Info("successfully create ISR root")
			})
			util.WaitSuccess(func() error {
				return zk.CreateIfNotExist(zookeeper.CommitIndexPath, nil)
			}, func(err error) {
				logger.WithError(err).Info("failed to create commit root")
			}, func() {
				logger.Info("successfully create commit root")
			})
			util.WaitSuccess(func() error {
				return zk.CreateIfNotExist(zookeeper.ElectionPath, nil)
			}, func(err error) {
				logger.WithError(err).Info("failed to create election root")
			}, func() {
				logger.Info("successfully create election root")
			})
			isrPath := zookeeper.MakeGroupPath(zookeeper.ISRPath, *options.gid)
			commitPath := zookeeper.MakeGroupPath(zookeeper.CommitIndexPath, *options.gid)
			electionPath := zookeeper.MakeGroupPath(zookeeper.ElectionPath, *options.gid)
			util.WaitSuccess(func() error {
				return zk.CreateIfNotExist(isrPath, nil, func() error {
					for i := 0; i < *options.peers; i++ {
						p := i
						go util.WaitSuccess(func() error {
							return zk.Add(isrPath, p)
						}, func(err error) {
							logger.WithError(err).WithField("peer", p).Info("failed to add isr")
						}, func() {
							logger.WithField("peer", p).Info("successfully add isr")
						})
					}
					return nil
				})
			}, func(err error) {
				logger.WithError(err).Info("failed to create ISR path")
			}, func() {
				logger.Info("successfully create ISR path")
			})
			util.WaitSuccess(func() error {
				return zk.CreateIfNotExist(commitPath, []byte(strconv.Itoa(0)))
			}, func(err error) {
				logger.WithError(err).Info("failed to create commit path")
			}, func() {
				logger.Info("successfully create commit path")
			})
			util.WaitSuccess(func() error {
				return zk.CreateIfNotExist(electionPath, nil)
			}, func(err error) {
				logger.WithError(err).Info("failed to create election path")
			}, func() {
				logger.Info("successfully create election path")
			})
		}
		return nil
	}
	util.WaitSuccess(func() error {
		return zk.Register(zookeeper.GroupPath, *options.gid, *options.me, addr, initJoin, initZK)
	}, func(err error) {
		logger.WithError(err).
			Info("failed to register replica")
	}, func() {
		logger.Info("successfully registered replica")
	})
	select {}
}
