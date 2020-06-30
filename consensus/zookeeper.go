package consensus

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
	"time"

	"github.com/artor1os/dkv/persist"
	"github.com/artor1os/dkv/rpc"
	"github.com/artor1os/dkv/util"
	"github.com/artor1os/dkv/zookeeper"
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

	if args.LastLogIndex == z.lastLogIndex() {
		z.addISR(args.ID)
	}

	if args.LastLogIndex == z.lastFetchedIndex[args.ID] {
		reply.LastRetain = args.LastLogIndex
		go z.updateSyncedIndex(args.ID, args.LastLogIndex)
	} else {
		reply.LastRetain = args.HighWaterMark
	}

	reply.Log = z.log[reply.LastRetain:]
	z.lastFetchedIndex[args.ID] = z.lastLogIndex()
	reply.LeaderHighWaterMark = z.highWaterMark
	return
}

func (z *ZK) updateSyncedIndex(server int, index int) {
	z.mu.Lock()
	defer z.mu.Unlock()

	if index <= z.SyncedIndex[server] {
		return
	}
	z.SyncedIndex[server] = index

	var si []int
	for isr := range z.isrSet {
		si = append(si, z.SyncedIndex[isr])
	}

	sort.Ints(si)

	// NOTE: correct?
	i := len(si) - z.isr

	if i >= 0 && len(si) > 0 && si[i] > z.highWaterMark {
		b, err := json.Marshal(si[i])
		if err != nil {
			panic(err)
		}
		if err := z.zk.SetData(z.commitIndexPath, b); err != nil {
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
	defer z.mu.Unlock()
	if z.isLeader {
		return
	}
	args := SyncArgs{}
	args.ID = z.me
	args.HighWaterMark = z.highWaterMark
	reply := SyncReply{}
	z.mu.Unlock()

	leader, err := z.getLeader()
	if err != nil {
		// TODO
		return
	}
	if leader == z.me {
		return
	}
	ok := z.peers[leader].Call("ZK.Sync", &args, &reply)

	z.mu.Lock()
	if z.isLeader {
		return
	}
	if ok {
		z.log = append(z.log[:reply.LastRetain+1], reply.Log...)
		z.updateHighWaterMark(reply.LeaderHighWaterMark)
	}
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

const replicaMaxSyncTimeout = time.Millisecond * 500

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
	if err := z.zk.Delete(z.isrPath, server); err != nil && err != zookeeper.ErrNodeNotExist {
		panic(err)
	}
	z.isrSet.Delete(server)
}

const followerFetchInterval = time.Millisecond * 300

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
		z.zk.ElectLeader(z.electionPath, elected)
		select {
		case r := <-elected:
			if r == nil {
				if in, err := z.zk.In(z.isrPath, z.me); err == nil && in {
					b, err := z.zk.Data(z.commitIndexPath)
					if err != nil {
						panic(err)
					}
					if err := json.Unmarshal(b, &z.highWaterMark); err != nil {
						panic(err)
					}
					z.log = z.log[:z.highWaterMark+1]
					z.SyncedIndex[z.me] = z.lastLogIndex()
					z.isLeader = true
					cancel()
					go z.isrChecker()
					return
				}
			}
			// Delete ephemeral node
			// TODO
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
	go zk.apply()
	go zk.wait()

	return zk
}
