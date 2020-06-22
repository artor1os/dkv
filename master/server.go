package master

import (
	"encoding/gob"
	"sync"
	"time"

	"github.com/artor1os/dkv/consensus"
	"github.com/artor1os/dkv/persist"
	"github.com/artor1os/dkv/rpc"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *consensus.Raft
	applyCh chan consensus.ApplyMsg

	configs []Config // indexed by config num

	indexCh       map[int]chan *Op
	lastCommitted map[int64]int
}

type Op struct {
	Type string

	RID int
	CID int64

	WrongLeader bool

	Join  *JoinInfo
	Leave *LeaveInfo
	Move  *MoveInfo
	Query *QueryInfo

	QueryResult *QueryResult
}

type JoinInfo struct {
	Servers map[int][]string
}

type LeaveInfo struct {
	GIDs []int
}

type MoveInfo struct {
	Shard int
	GID   int
}

type QueryInfo struct {
	Num int
}

type QueryResult struct {
	Config Config
}

const (
	JoinOp  = "Join"
	LeaveOp = "Leave"
	MoveOp  = "Move"
	QueryOp = "Query"
)

func (sm *ShardMaster) apply() {
	for am := range sm.applyCh {
		sm.mu.Lock()
		op := am.Command.(Op)
		var ch chan *Op
		var ok bool
		if ch, ok = sm.indexCh[am.CommandIndex]; ok {
			select {
			case <-ch:
			default:
			}
		} else {
			ch = make(chan *Op, 1)
			sm.indexCh[am.CommandIndex] = ch
		}
		sm.applyOp(&op)
		ch <- &op
		sm.mu.Unlock()
	}
}

func (sm *ShardMaster) waitIndexCommit(index int, cid int64, rid int) *Op {
	sm.mu.Lock()
	var ch chan *Op
	var ok bool
	if ch, ok = sm.indexCh[index]; !ok {
		ch = make(chan *Op, 1)
		sm.indexCh[index] = ch
	}
	sm.mu.Unlock()
	select {
	case op := <-ch:
		if op.CID != cid || op.RID != rid {
			return &Op{WrongLeader: true}
		}
		return op
	case <-time.After(time.Millisecond * 300):
		return &Op{WrongLeader: true}
	}
}

func (sm *ShardMaster) isDup(op *Op) bool {
	lastCommitted, ok := sm.lastCommitted[op.CID]
	if !ok {
		return false
	}
	return op.RID <= lastCommitted
}

func (sm *ShardMaster) newConfig() *Config {
	config := sm.configs[len(sm.configs)-1]
	newConf := &Config{}
	newConf.Num = config.Num + 1
	newConf.Groups = make(map[int][]string)
	for i := 0; i < NShards; i++ {
		newConf.Shards[i] = config.Shards[i]
	}
	for k, v := range config.Groups {
		newConf.Groups[k] = v
	}

	return newConf
}

func rebalance(config *Config) {
	var gids []int
	for gid := range config.Groups {
		gids = append(gids, gid)
	}
	if len(gids) == 0 {
		for i := range config.Shards {
			config.Shards[i] = 0
		}
		return
	}

	shardsEachGroup := make(map[int]int)

	for _, gid := range gids {
		shardsEachGroup[gid] = 0
	}

	minShardsGroup := func(m map[int]int) int {
		min := NShards + 1
		ret := -1
		for gid, nShards := range m {
			if nShards < min {
				ret = gid
				min = nShards
			}
		}
		return ret
	}

	for i, gid := range config.Shards {
		if _, ok := config.Groups[gid]; !ok || gid == 0 {
			config.Shards[i] = gids[0]
			shardsEachGroup[gids[0]]++
		} else {
			shardsEachGroup[gid]++
		}
	}

	avg := NShards / len(gids)

	for i, gid := range config.Shards {
		if shardsEachGroup[gid] > avg {
			min := minShardsGroup(shardsEachGroup)
			config.Shards[i] = min
			shardsEachGroup[gid]--
			shardsEachGroup[min]++
		}
	}
}

func (sm *ShardMaster) commit(op *Op) {
	sm.lastCommitted[op.CID] = op.RID
}

func (sm *ShardMaster) applyOp(op *Op) {
	switch op.Type {
	case JoinOp:
		if !sm.isDup(op) {
			config := sm.newConfig()
			for k, v := range op.Join.Servers {
				config.Groups[k] = v
			}
			rebalance(config)
			sm.configs = append(sm.configs, *config)
		}
	case LeaveOp:
		if !sm.isDup(op) {
			config := sm.newConfig()
			for _, k := range op.Leave.GIDs {
				delete(config.Groups, k)
			}
			rebalance(config)
			sm.configs = append(sm.configs, *config)
		}
	case MoveOp:
		if !sm.isDup(op) {
			config := sm.newConfig()
			config.Shards[op.Move.Shard] = op.Move.GID
			sm.configs = append(sm.configs, *config)
		}
	case QueryOp:
		index := op.Query.Num
		if op.Query.Num < 0 || op.Query.Num >= len(sm.configs) {
			index = len(sm.configs) - 1
		}
		op.QueryResult = &QueryResult{Config: sm.configs[index]}
	}
	if !sm.isDup(op) {
		sm.commit(op)
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	servers := make(map[int][]string)
	for k, v := range args.Servers {
		servers[k] = v
	}
	newOp := Op{Type: JoinOp, RID: args.RID, CID: args.CID, Join: &JoinInfo{Servers: servers}}
	index, _, isLeader := sm.rf.Start(newOp)
	if !isLeader {
		reply.WrongLeader = true
		return nil
	}

	op := sm.waitIndexCommit(index, args.CID, args.RID)

	reply.WrongLeader = op.WrongLeader
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	gids := make([]int, len(args.GIDs))
	copy(gids, args.GIDs)
	newOp := Op{Type: LeaveOp, RID: args.RID, CID: args.CID, Leave: &LeaveInfo{GIDs: gids}}
	index, _, isLeader := sm.rf.Start(newOp)
	if !isLeader {
		reply.WrongLeader = true
		return nil
	}

	op := sm.waitIndexCommit(index, args.CID, args.RID)

	reply.WrongLeader = op.WrongLeader
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	newOp := Op{Type: MoveOp, RID: args.RID, CID: args.CID, Move: &MoveInfo{Shard: args.Shard, GID: args.GID}}
	index, _, isLeader := sm.rf.Start(newOp)
	if !isLeader {
		reply.WrongLeader = true
		return nil
	}

	op := sm.waitIndexCommit(index, args.CID, args.RID)

	reply.WrongLeader = op.WrongLeader
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	newOp := Op{Type: QueryOp, RID: args.RID, CID: args.CID, Query: &QueryInfo{Num: args.Num}}
	index, _, isLeader := sm.rf.Start(newOp)
	if !isLeader {
		reply.WrongLeader = true
		return nil
	}

	op := sm.waitIndexCommit(index, args.CID, args.RID)

	reply.WrongLeader = op.WrongLeader
	if !reply.WrongLeader {
		reply.Config = op.QueryResult.Config
	}
	return nil
}

func NewServer(servers []rpc.Endpoint, me int, persister persist.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me
	gob.Register(Op{})

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	sm.applyCh = make(chan consensus.ApplyMsg, 1)
	sm.rf = consensus.NewRaft(servers, me, persister, sm.applyCh)
	if err := rpc.Register(sm.rf); err != nil {
		panic(err)
	}
	sm.lastCommitted = make(map[int64]int)
	sm.indexCh = make(map[int]chan *Op)

	go sm.apply()

	return sm
}
