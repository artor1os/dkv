package consensus

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

func (z *ZK) persist(snapshot ...[]byte) {
	w := new(bytes.Buffer)
	e := json.NewEncoder(w)

	_ = e.Encode(z.log)

	data := w.Bytes()
	if len(snapshot) > 0 {
		z.persister.SaveStateAndSnapshot(data, snapshot[0])
	} else {
		z.persister.SaveState(data)
	}
}

func (z *ZK) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := json.NewDecoder(r)

	var l []interface{}

	if d.Decode(&l) != nil {
		panic("failed to read persist")
	} else {
		z.log = l
	}
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
	if index < z.SyncedIndex[server] {
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
		util.WaitSuccess(func() error {
			return z.zk.SetData(z.commitIndexPath, []byte(strconv.Itoa(si[i])))
		}, func(err error) {
			z.logger.WithError(err).Error("failed to update commitIndex on zk")
		}, nil)
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
		z.logger.Info("leader is me")
		z.mu.Unlock()
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
		z.persist()
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
	util.WaitSuccess(func() error {
		if err := z.zk.Add(z.isrPath, server); err != nil && !errors.Is(err, zookeeper.ErrNodeExist) {
			return err
		}
		return nil
	}, func(err error) {
		z.logger.WithError(err).WithField("server", server).Error("failed to add isr")
	}, nil)
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
	util.WaitSuccess(func() error {
		if err := z.zk.Remove(z.isrPath, server); err != nil && !errors.Is(err, zookeeper.ErrNodeNotExist) {
			return err
		}
		return nil
	}, func(err error) {
		z.logger.WithError(err).WithField("server", server).Error("failed to remove isr")
	}, nil)
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
		r := <-elected
		if r == nil {
			if in, err := z.zk.In(z.isrPath, z.me); err == nil && in {
				z.logger.Info("in ISR, and elected")

				var b []byte
				util.WaitSuccess(func() error {
					var err error
					b, err = z.zk.Data(z.commitIndexPath)
					return err
				}, func(err error) {
					z.logger.WithError(err).Error("failed to update commitIndex on zk")
				}, nil)
				hwm, err := strconv.Atoi(string(b))
				if err != nil {
					panic(err)
				}
				z.updateHighWaterMark(hwm)
				z.logger.WithField("hwm", z.highWaterMark).Info("set highWaterMark")
				z.log = z.log[:z.highWaterMark+1]

				var isr []int
				util.WaitSuccess(func() error {
					var err error
					isr, err = z.zk.All(z.isrPath)
					return err
				}, func(err error) {
					z.logger.WithError(err).Error("failed to get all isr")
				}, nil)
				z.isrSet = util.NewSet()
				for _, r := range isr {
					z.isrSet.Add(r)
				}

				z.SyncedIndex[z.me] = z.lastLogIndex()
				z.isLeader = true
				cancel()
				go z.isrChecker()
				return
			}
			z.logger.Info("elected but not in ISR")
		}
		z.logger.WithError(r).Info("error occurs while watching")
		util.WaitSuccess(func() error {
			if err := z.zk.Delete(node); err != nil && !errors.Is(err, zookeeper.ErrNodeNotExist) {
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

func (z *ZK) Start(command interface{}) (int, bool) {
	z.mu.Lock()
	defer z.mu.Unlock()
	if !z.isLeader {
		return 0, false
	}
	z.log = append(z.log, command)
	z.SyncedIndex[z.me] = z.lastLogIndex()
	z.persist()
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
	zk.readPersist(zk.persister.ReadState())

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
