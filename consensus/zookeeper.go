package consensus

import (
	"context"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/artor1os/dkv/persist"
	"github.com/artor1os/dkv/rpc"
	"github.com/artor1os/dkv/util"
	"github.com/artor1os/dkv/zookeeper"
	log "github.com/sirupsen/logrus"
)

type ZK struct {
	mu        sync.Mutex
	peers     []rpc.Endpoint
	me        int
	persister persist.Persister
	applyCh   chan ApplyMsg
	zk        zookeeper.Controller
	isr       int

	log []interface{}

	highWaterMark int
	isLeader      bool

	resetCh          []chan struct{}
	lastFetchedIndex []int
	SyncedIndex      []int
	lastApplied      int
	isrSet           util.Set

	applyCond *sync.Cond

	electionPath    string
	isrPath         string
	commitIndexPath string
	logger          *log.Entry
}

type SyncArgs struct {
	ID            int
	HighWaterMark int
	LastLogIndex  int
}

type SyncReply struct {
	Log                 []interface{}
	LastRetain          int
	LeaderHighWaterMark int
}

func (z *ZK) lastLogIndex() int {
	return len(z.log) - 1
}

func (z *ZK) Sync(args *SyncArgs, reply *SyncReply) (err error) {
	err = nil
	z.mu.Lock()
	defer z.mu.Unlock()
	if !z.isLeader {
		return
	}
	logger := z.logger.WithField("from", args.ID).
		WithField("highWaterMark", args.HighWaterMark).
		WithField("lastLogIndex", args.LastLogIndex)

	if args.LastLogIndex == z.lastLogIndex() {
		logger.Debug("lastLogIndex equals leader's, add to ISR")
		z.addISR(args.ID)
	}

	if args.LastLogIndex == z.lastFetchedIndex[args.ID] {
		reply.LastRetain = args.LastLogIndex
		logger.WithField("lastRetain", reply.LastRetain).
			Debug("lastLogIndex equals lastFetchedIndex, update syncedIndex")
		z.updateSyncedIndex(args.ID, args.LastLogIndex)
	} else {
		reply.LastRetain = args.HighWaterMark
		logger.WithField("lastRetain", reply.LastRetain).
			Debug("not expected lastLogIndex, retain only before highWaterMark")
	}

	reply.Log = z.log[reply.LastRetain+1:]
	z.lastFetchedIndex[args.ID] = z.lastLogIndex()
	reply.LeaderHighWaterMark = z.highWaterMark
	return
}

func (z *ZK) updateSyncedIndex(server int, index int) {
	if index <= z.SyncedIndex[server] {
		return
	}
	z.SyncedIndex[server] = index

	var si []int
	for isr := range z.isrSet {
		si = append(si, z.SyncedIndex[isr])
	}

	sort.Ints(si)
	z.logger.WithField("si", si).Debug("synced index in ISR")

	// NOTE: correct?
	i := len(si) - z.isr

	z.logger.WithField("i", i).WithField("isr", z.isr).
		Debug("ith synced index has been agreed by at least isrNum replica")

	if i >= 0 && len(si) > 0 && si[i] > z.highWaterMark {
		z.logger.WithField("i", i).WithField("si[i]", si[i]).Info("leader update highWaterMark")
		if err := z.zk.SetData(z.commitIndexPath, []byte(strconv.Itoa(si[i]))); err != nil {
			panic(err)
		}
		z.updateHighWaterMark(si[i])
	}
}

func (z *ZK) updateHighWaterMark(hw int) {
	z.highWaterMark = hw
	z.applyCond.Signal()
}

func (z *ZK) sync() {
	z.mu.Lock()
	if z.isLeader {
		return
	}
	args := SyncArgs{}
	args.ID = z.me
	args.HighWaterMark = z.highWaterMark
	args.LastLogIndex = z.lastLogIndex()
	reply := SyncReply{}
	z.mu.Unlock()

	leader, err := z.getLeader()
	if err != nil {
		z.logger.WithError(err).Info("failed to get leader")
		return
	}
	if leader == z.me {
		z.logger.Info("leader is me")
		return
	}
	ok := z.peers[leader].Call("ZK.Sync", &args, &reply)

	z.mu.Lock()
	if z.isLeader {
		z.logger.Info("leader is me")
		z.mu.Unlock()
		return
	}
	if ok {
		z.logger.
			WithField("log", reply.Log).
			WithField("lastRetain", reply.LastRetain).
			WithField("leaderHighWaterMark", reply.LeaderHighWaterMark).
			Debug("successfully sync with leader")
		z.log = append(z.log[:reply.LastRetain+1], reply.Log...)
		z.updateHighWaterMark(reply.LeaderHighWaterMark)
	}
	z.mu.Unlock()
}

func (z *ZK) apply() {
	for {
		z.mu.Lock()
		for !(z.highWaterMark > z.lastApplied) {
			z.applyCond.Wait()
		}

		z.lastApplied++
		msg := ApplyMsg{}
		msg.CommandValid = true
		msg.CommandIndex = z.lastApplied
		msg.Command = z.log[z.lastApplied]
		z.applyCh <- msg
		z.logger.WithField("command", msg.Command).WithField("index", msg.CommandIndex).Info("msg applied")
		z.mu.Unlock()
	}
}

func (z *ZK) getLeader() (int, error) {
	return z.zk.First(z.electionPath)
}

func (z *ZK) addISR(server int) {
	if err := z.zk.Add(z.isrPath, server); err != nil && err != zookeeper.ErrNodeExist {
		panic(err)
	}
	z.isrSet.Add(server)
}

const replicaMaxSyncTimeout = time.Millisecond * 1000

func (z *ZK) isrChecker() {
	for i := range z.peers {
		if i == z.me {
			continue
		}
		z.resetCh[i] = make(chan struct{})
		go func(i int) {
			timer := time.NewTimer(replicaMaxSyncTimeout)
			for {
				select {
				case <-timer.C:
					timer.Reset(replicaMaxSyncTimeout)
					z.removeFromISR(i)
				case <-z.resetCh[i]:
					timer = time.NewTimer(replicaMaxSyncTimeout)
				}
			}
		}(i)
	}
}

func (z *ZK) removeFromISR(server int) {
	z.mu.Lock()
	defer z.mu.Unlock()
	if err := z.zk.Remove(z.isrPath, server); err != nil && err != zookeeper.ErrNodeNotExist {
		panic(err)
	}
	z.isrSet.Delete(server)
}

const followerFetchInterval = time.Millisecond * 100

func (z *ZK) fetch(ctx context.Context) {
	ticker := time.NewTicker(followerFetchInterval)
	for {
		select {
		case <-ticker.C:
			z.sync()
		case <-ctx.Done():
			return
		}
	}
}

func (z *ZK) wait() {
	elected := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())
	go z.fetch(ctx)
	for {
		node, err := z.zk.ElectLeader(z.electionPath, z.me, elected)
		if err != nil {
			z.logger.WithError(err).Info("failed to elect leader")
			continue
		}
		z.logger.WithField("node", node).Info("try to elect leader")
		select {
		case r := <-elected:
			if r == nil {
				if in, err := z.zk.In(z.isrPath, z.me); err == nil && in {
					z.logger.Info("in ISR, and elected")

					b, err := z.zk.Data(z.commitIndexPath)
					if err != nil {
						panic(err)
					}
					hwm, err := strconv.Atoi(string(b))
					if err != nil {
						panic(err)
					}
					z.highWaterMark = hwm
					z.logger.WithField("hwm", z.highWaterMark).Info("set highWaterMark")
					z.log = z.log[:z.highWaterMark+1]

					isr, err := z.zk.All(z.isrPath)
					if err != nil {
						panic(err)
					}
					for _, r := range isr {
						z.isrSet.Add(r)
					}

					z.SyncedIndex[z.me] = z.lastLogIndex()
					z.isLeader = true
					cancel()
					go z.isrChecker()
					return
				}
			}
			util.WaitSuccess(func() error {
				if err := z.zk.Delete(node); err != zookeeper.ErrNodeNotExist {
					return err
				}
				return nil
			}, func(err error) {
				z.logger.WithError(err).WithField("node", node).Info("failed to delete election node")
			}, func() {
				z.logger.Info("successfully delete election node")
			})
		}
	}
}

func (z *ZK) Start(command interface{}) (int, bool) {
	z.mu.Lock()
	defer z.mu.Unlock()
	if !z.isLeader {
		return 0, false
	}
	z.log = append(z.log, command)
	z.SyncedIndex[z.me] = z.lastLogIndex()
	return z.lastLogIndex(), true
}

func (z *ZK) DiscardOldLog(index int, snapshot []byte) {

}

func NewZK(peers []rpc.Endpoint, me int, persister persist.Persister, applyCh chan ApplyMsg,
	controller zookeeper.Controller, isr int, electionPath string, isrPath string, commitIndexPath string) *ZK {
	zk := &ZK{}
	zk.zk = controller
	zk.me = me
	zk.peers = peers
	zk.persister = persister
	zk.applyCh = applyCh
	zk.isr = isr
	zk.electionPath = electionPath
	zk.isrPath = isrPath
	zk.commitIndexPath = commitIndexPath

	zk.log = make([]interface{}, 1)

	zk.resetCh = make([]chan struct{}, len(peers))
	zk.lastFetchedIndex = make([]int, len(peers))
	zk.SyncedIndex = make([]int, len(peers))
	zk.isrSet = util.NewSet()

	zk.applyCond = sync.NewCond(&zk.mu)
	zk.logger = log.WithField("me", zk.me)
	go zk.apply()
	go zk.wait()

	if err := rpc.Register(zk); err != nil {
		panic(err)
	}

	return zk
}
