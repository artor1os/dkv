package consensus

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/artor1os/dkv/persist"
	"github.com/artor1os/dkv/rpc"
	log "github.com/sirupsen/logrus"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	RaftStateSize int
	Snapshot      []byte
}

type LogEntry struct {
	Command interface{}
	Term    int
}

type Logs struct {
	Entries []*LogEntry

	Base int

	LastIncludedIndex int
	LastIncludedTerm  int
}

func (log *Logs) adjust(index int) int {
	index -= log.Base
	index -= log.LastIncludedIndex
	return index
}

func (log *Logs) Include(index int) bool {
	index = log.adjust(index)
	return index >= 0 && index < len(log.Entries)
}

func (log *Logs) At(index int) *LogEntry {
	index = log.adjust(index)
	if index == -1 {
		return &LogEntry{Command: nil, Term: log.LastIncludedTerm}
	}
	return log.Entries[index]
}

func (log *Logs) Len() int {
	return log.Base + log.LastIncludedIndex + len(log.Entries)
}

func (log *Logs) Back() *LogEntry {
	return log.At(log.Len() - 1)
}

func (log *Logs) Append(e ...*LogEntry) {
	log.Entries = append(log.Entries, e...)
}

type truncateDirection int

const (
	front truncateDirection = iota
	back
)

// Truncate truncates log after or before(dir is back or front) index (inclusively)
func (log *Logs) Truncate(index int, dir truncateDirection) {
	index = log.adjust(index)
	if dir == back {
		if index < 0 {
			index = 0
		}
		log.Entries = log.Entries[:index]
	} else if dir == front && index >= 0 {
		log.LastIncludedTerm = log.Entries[index].Term
		log.LastIncludedIndex += index + 1
		log.Entries = log.Entries[index+1:]
	}
}

// RangeFrom returns all LogEntry after index(inclusively)
func (log *Logs) RangeFrom(index int) []*LogEntry {
	index = log.adjust(index)
	return log.Entries[index:]
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex        // Lock to protect shared access to this peer's state
	peers     []rpc.Endpoint    // RPC end points of all peers
	persister persist.Persister // Object to hold this peer's persisted state
	me        int               // this peer's index into peers[]
	dead      int32             // set by Kill()

	currentTerm int // latest term server has seen(initialized to 0 on first boot, increases monotonically)
	votedFor    int // candidateId that received vote in current term(or null if none)

	log *Logs // log entries; each entry contains command for state machine, and term when entry was received by leader(first index is 1)

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be commited(initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine(initialized to 0, increases monotonically)

	// Volatile state on leaders
	// Reinitialized after election
	nextIndex  []int // for each server, index of the next log entry to send to that server(initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server(initialized to 0, increases monotonically)

	applyCh chan ApplyMsg

	role role

	resetCh  chan struct{}
	resignCh chan struct{}
	notifyCh chan struct{}

	applyCond *sync.Cond
	logger *log.Entry
}

type role string

const (
	follower  role = "fol"
	candidate role = "can"
	leader    role = "lea"
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = rf.role == leader
	return term, isLeader
}

func (rf *Raft) persist(snapshot ...[]byte) {
	w := new(bytes.Buffer)
	e := json.NewEncoder(w)

	_ = e.Encode(rf.currentTerm)
	_ = e.Encode(rf.votedFor)
	_ = e.Encode(rf.log)

	data := w.Bytes()
	if len(snapshot) > 0 {
		rf.persister.SaveStateAndSnapshot(data, snapshot[0])
	} else {
		rf.persister.SaveRaftState(data)
	}
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := json.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log *Logs

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		panic("failed to read persist")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

type RequestVoteArgs struct {
	Term        int // candidate's term
	CandidateId int // candidate requesting vote

	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

func (rf *Raft) acceptNewerTerm(inTerm int, vote int) {
	rf.mu.Lock()
	isNolongerLeader := false
	// [raft-paper, all-servers]
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if inTerm > rf.currentTerm {
		if rf.role == leader {
			isNolongerLeader = true
		}
		rf.currentTerm = inTerm
		rf.role = follower
		rf.votedFor = vote // NOTE: very important
		// rf.persist()
	}
	rf.mu.Unlock()
	if isNolongerLeader {
		rf.resignCh <- struct{}{}
	}
}

func (rf *Raft) isCandidateUptodate(lastLogIndex int, lastLogTerm int) bool {
	// [raft-paper, up-to-date]
	// Raft determines which of two logs is more up-to-date by comparing the index and temr of
	// the last entries in the logs. If the logs have last entries with different terms, then the log
	// with the later term is more up-to-date. If the logs end with the same term, then whichever log is longer
	// is more up-to-date
	var ret bool
	myLastLog := rf.log.Back()
	if myLastLog.Term < lastLogTerm {
		ret = true
	}
	if myLastLog.Term == lastLogTerm {
		ret = rf.log.Len()-1 <= lastLogIndex
	}
	if myLastLog.Term > lastLogTerm {
		ret = false
	}
	return ret
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) (err error) {
	err = nil
	rf.acceptNewerTerm(args.Term, args.CandidateId)
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	// [raft-paper, RequestVoteRPC receiver]
	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}

	// [raft-paper, RequestVoteRPC receiver]
	// If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if rf.isCandidateUptodate(args.LastLogIndex, args.LastLogTerm) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		} else {
			reply.VoteGranted = false
			rf.votedFor = -1
		}
		rf.persist()
	}

	isVoted := rf.role == follower && reply.VoteGranted
	rf.mu.Unlock()
	if isVoted {
		// [raft-paper, follower]
		// If election timeout elapses without
		// receiving AppendEntries RPC from current leader
		// or granting vote to candidate: convert to candidate
		rf.resetCh <- struct{}{}
	}
	return
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.acceptNewerTerm(reply.Term, server)
	}
	return ok
}

type AppendEntriesArgs struct {
	Term     int // leader's term
	LeaderId int // so follower can redirect clients

	PrevLogIndex int         // index of log entry immediately preceding new ones
	PrevLogTerm  int         // term of prevLogIndex entry
	Entries      []*LogEntry // log entries to store(empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int         // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	FirstConflictIndex int
	ConflictTerm       int
	LastIndex          int
}

// apply goroutine
func (rf *Raft) apply() {
	// [raft-paper, all-servers]
	// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
	for {
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		for !(rf.commitIndex > rf.lastApplied) {
			rf.applyCond.Wait()
		}

		rf.lastApplied++
		msg := ApplyMsg{}
		msg.CommandValid = true
		msg.CommandIndex = rf.lastApplied
		msg.Command = rf.log.At(rf.lastApplied).Command
		msg.RaftStateSize = rf.persister.RaftStateSize()
		rf.applyCh <- msg
		rf.mu.Unlock()
	}
}

// lock should be held by caller
func (rf *Raft) updateCommitIndex(index int) {
	rf.commitIndex = index
	rf.applyCond.Signal()
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) (err error) {
	err = nil
	rf.acceptNewerTerm(args.Term, args.LeaderId)
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	// [raft-paper, AppendEntriesRPC receiver]
	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	// this rpc call is from valid leader
	if rf.role == candidate {
		// [raft-paper, candidate]
		// If AppendEntries RPC received from new leader: convert to follower
		rf.role = follower
	}
	if rf.role == follower {
		// [raft-paper, follower]
		// If eleciton timeout elapses without
		// receiving AppendEntries RPC from current leader
		// or granting vote to candidate: convert to candidate
		rf.mu.Unlock()
		rf.resetCh <- struct{}{}
		rf.mu.Lock()
	}

	if args.PrevLogIndex >= rf.log.Len() || (args.PrevLogIndex >= rf.log.LastIncludedIndex && rf.log.At(args.PrevLogIndex).Term != args.PrevLogTerm) {
		// [raft-paper, AppendEntriesRPC receiver]
		// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
		reply.Success = false

		// [raft-paper, page 8, quick decrement nextIndex]
		last := Min(args.PrevLogIndex, rf.log.Len()-1)
		term := rf.log.At(last).Term
		reply.ConflictTerm = term
		reply.LastIndex = rf.log.Len() - 1

		if last != rf.log.LastIncludedIndex {
			for ; last > rf.log.LastIncludedIndex && rf.log.At(last).Term == term; last-- {
			}
			last++
		}
		reply.FirstConflictIndex = last
		rf.mu.Unlock()
		return
	}

	if args.Entries != nil { // not a heartbeat
		l := rf.log.Len()
		start := args.PrevLogIndex + 1
		i := Max(start, rf.log.LastIncludedIndex+1)
		dirty := false
		for ; i < l && i-start < len(args.Entries); i++ {
			// [raft-paper, AppendEntriesRPC receiver]
			// If an existing entry conflicts with a new one(same index but different term),
			// delete the existing entry and all that follow it
			if rf.log.At(i).Term != args.Entries[i-start].Term {
				rf.log.Truncate(i, back)
				dirty = true
				break
			}
		}
		for ; i-start < len(args.Entries); i++ {
			dirty = true
			// [raft-paper, AppendEntriesRPC receiver]
			// Append any new entries not already in the log
			rf.log.Append(args.Entries[i-start])
		}
		if dirty {
			rf.persist()
		}
	}

	// [raft-paper, AppendEntriesRPC receiver]
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.updateCommitIndex(Min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries)))
	}

	reply.Success = true
	rf.mu.Unlock()
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.acceptNewerTerm(reply.Term, server)
	}
	return ok
}

type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) applySnapshot(snapshot []byte) {
	rf.applyCh <- ApplyMsg{CommandValid: false, Snapshot: snapshot}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) (err error) {
	err = nil
	rf.acceptNewerTerm(args.Term, args.LeaderId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	// [raft-paper, InstallSnapshot receiver]
	// Reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	// this rpc call is from valid leader
	if rf.role == candidate {
		// [raft-paper, candidate]
		// If AppendEntries RPC received from new leader: convert to follower
		rf.role = follower
	}
	if rf.role == follower {
		// [raft-paper, follower]
		// If eleciton timeout elapses without
		// receiving AppendEntries RPC from current leader
		// or granting vote to candidate: convert to candidate
		rf.mu.Unlock()
		rf.resetCh <- struct{}{}
		rf.mu.Lock()
	}

	// [raft-paper, InstallSnapshot receiver]
	// Create new snapshot file if first chunk (offset is 0)
	// Write data into snapshot file at given offset
	// Reply and wait for more data chunks if done is false
	// Save snapshot file, discard any existing or partial snapshot with a smaller index

	// In this lab, no partition, simply save snapshot

	// If existing log entry has same index and term as snapshot's last included entry, retain log entries following it and reply
	if args.LastIncludedIndex <= rf.log.LastIncludedIndex {
		return
	}
	if rf.log.Include(args.LastIncludedIndex) && rf.log.At(args.LastIncludedIndex).Term == args.LastIncludedTerm {
		rf.log.Truncate(args.LastIncludedIndex, front)
		if args.LastIncludedIndex > rf.lastApplied {
			rf.applySnapshot(args.Data)
		}
		rf.commitIndex = Max(args.LastIncludedIndex, rf.commitIndex)
		rf.lastApplied = Max(args.LastIncludedIndex, rf.lastApplied)
	} else {
		// Discard the entire log
		// Reset state machine using snapshot contents(and load snapshot's cluster configuration)
		rf.log.Entries = nil
		rf.commitIndex = args.LastIncludedIndex
		rf.lastApplied = args.LastIncludedIndex
		rf.log.LastIncludedIndex = args.LastIncludedIndex
		rf.log.LastIncludedTerm = args.LastIncludedTerm
		rf.applySnapshot(args.Data)
	}
	rf.persist(args.Data)
	return
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if ok {
		rf.acceptNewerTerm(reply.Term, server)
	}
	return ok
}

func (rf *Raft) majority() int {
	return (len(rf.peers) + 1) / 2
}

// updateMatchIndex goroutine will set matchIndex and may set commitIndex
func (rf *Raft) updateMatchIndex(server int, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() || rf.role != leader || rf.matchIndex[server] >= index {
		return
	}
	rf.matchIndex[server] = index

	// [raft-paper, Leader]
	// If there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N,
	// and log[N].term == currentTerm: set commitIndex = N

	// Implementation:
	// 1. Copy and sort matchIndex in ascending order
	// 2. So all values in sorted matchIndex before index majority-1(inclusively) meet the requirement "a majority of matchIndex[i] >= N"
	// 3. Count from majority-1 to 0, find the largest N that subject to "log[N].term == currentTerm"
	// 4. Finally test if the largest N greater than commitIndex
	//
	// Notes for Log compaction:
	// If N in step 3 <= rf.log.LastIncludedIndex, there is no need to continue
	// Because commitIndex > LastIncludedIndex, break immediately to avoid out of range indexing
	mi := make([]int, len(rf.matchIndex))
	copy(mi, rf.matchIndex)
	sort.Ints(mi)
	maxN := -1
	for i := rf.majority() - 1; i >= 0; i-- {
		n := mi[i]
		if n <= rf.log.LastIncludedIndex {
			break
		}
		if rf.log.At(n).Term == rf.currentTerm {
			maxN = n
			break
		}
	}
	if maxN > rf.commitIndex {
		rf.updateCommitIndex(maxN)
	}
}

func (rf *Raft) installSnapshot(server int) bool {
	rf.mu.Lock()
	args := InstallSnapshotArgs{}
	args.LeaderId = rf.me
	args.Term = rf.currentTerm
	args.LastIncludedIndex = rf.log.LastIncludedIndex
	args.LastIncludedTerm = rf.log.LastIncludedTerm
	args.Data = rf.persister.ReadSnapshot()
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()

	ok := rf.sendInstallSnapshot(server, &args, &reply)

	rf.mu.Lock()
	if !ok || args.Term != rf.currentTerm {
		rf.mu.Unlock()
		return false
	}
	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.mu.Unlock()
	return true
}

// appendEntries goroutine
// [raft-paper]If last log index >= nextIndex for a follower:
// send AppendEntries RPC with log entries starting at nextIndex
// - If successful: update nextIndex and matchIndex for follower
// - If AppendEntries fails because of log inconsistency:
//   decrement nextIndex and retry
func (rf *Raft) appendEntries(server int) {
	for {
		rf.mu.Lock()
		// No longer valid AppendEntries
		if rf.killed() || rf.role != leader {
			rf.mu.Unlock()
			return
		}
		for rf.log.LastIncludedIndex >= rf.nextIndex[server] {
			rf.mu.Unlock()
			if !rf.installSnapshot(server) {
				return
			}
			rf.mu.Lock()
		}
		args := AppendEntriesArgs{}
		args.LeaderId = rf.me
		args.Term = rf.currentTerm
		args.LeaderCommit = rf.commitIndex
		args.PrevLogIndex = rf.nextIndex[server] - 1
		args.PrevLogTerm = rf.log.At(args.PrevLogIndex).Term
		// If last log index >= nextIndex, then args.Entries will not be nil
		// Otherwise, args.Entries == nil, and AppendEntries will be a heartbeat
		args.Entries = rf.log.RangeFrom(rf.nextIndex[server])
		reply := AppendEntriesReply{}
		rf.mu.Unlock()

		ok := rf.sendAppendEntries(server, &args, &reply)

		rf.mu.Lock()
		// Valid rpc call and not stale
		if ok && args.Term == rf.currentTerm {
			if reply.Success {
				// If successful: update nextIndex and matchIndex for follower
				match := args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[server] = match + 1
				go rf.updateMatchIndex(server, match)
			} else {
				// If AppendEntries fails because of log inconsistency:
				//   decrement nextIndex and retry
				// use fast decrement rather than decrement by 1

				// find the index in range[firstConflictIndex, min(prevLogIndex, lastIndex)] that matches
				index := Min(args.PrevLogIndex, reply.LastIndex)
				for ; index >= rf.log.LastIncludedIndex && index >= reply.FirstConflictIndex && rf.log.At(index).Term != reply.ConflictTerm; index-- {
				}

				rf.nextIndex[server] = index + 1

				rf.mu.Unlock()
				continue
			}
		}
		rf.mu.Unlock()
		return
	}
}

const heartbeatInterval = 150 * time.Millisecond
const eTimeoutLeft = 400
const eTimeoutRight = 600

// notify goroutine
func (rf *Raft) notify() {
	ticker := time.NewTicker(heartbeatInterval)
	for {
		select {
		case <-rf.resetCh:
			continue
		case <-rf.resignCh:
			go rf.wait()
			return
		case <-rf.notifyCh:
		case <-ticker.C:
		}
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		if rf.role == leader {
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				go rf.appendEntries(i)
			}
		}
		rf.mu.Unlock()
	}
}

// campaign is for Candidate to campaign for Leader,
// returns a channel indicating whether elected
// goroutines created will not be halted when start another round of election
// but this will be correct, because whenever another round of election is started
// elected channel of this round will be abandoned.
func (rf *Raft) campaign() (elected chan struct{}) {
	// [raft-paper]On conversion to candidate, start election:
	// - Increment currentTerm
	// - Vote for self
	// - Reset election timer * in wait()
	// - Send RequestVote RPCs to all other servers
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()

	voteCh := make(chan struct{})
	elected = make(chan struct{})
	go func() {
		votes := 1
		for range voteCh {
			votes++
			// [raft-paper]If votes received from majority of servers: become leader
			if votes == rf.majority() {
				close(elected)
			}
		}
	}()
	go func() {
		var wg sync.WaitGroup
		rf.mu.Lock()
		args := RequestVoteArgs{}
		args.Term = rf.currentTerm
		args.CandidateId = rf.me
		args.LastLogIndex = rf.log.Len() - 1
		args.LastLogTerm = rf.log.At(args.LastLogIndex).Term
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			wg.Add(1)
			go func(server int) {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(server, &args, &reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if ok && args.Term == rf.currentTerm && reply.VoteGranted {
					voteCh <- struct{}{}
				}
				wg.Done()
			}(i)
		}
		wg.Wait()
		close(voteCh)
	}()
	return
}

func RandIntRange(left int, right int) int {
	return left + rand.Intn(right-left)
}

// wait goroutine is for Follower to wait for campaign
func (rf *Raft) wait() {
	elected := make(chan struct{})
	for {
		eTimeout := time.Duration(RandIntRange(eTimeoutLeft, eTimeoutRight)) * time.Millisecond
		timer := time.NewTimer(eTimeout)
		select {
		case <-rf.notifyCh:
			continue
		case <-rf.resignCh:
			continue
		case <-timer.C:
			rf.mu.Lock()
			if rf.killed() || rf.role == leader {
				rf.mu.Unlock()
				return
			}
			if rf.role == follower {
				// [raft-paper]If election timeout elapses without
				// receiving AppendEntries RPC from current leader
				// or granting vote to candidate: convert to candidate
				rf.role = candidate
			}
			if rf.role == candidate {
				rf.logger.Info("election timeout elapses, start new election")
				// [raft-paper]If election timeout elapses: start new election
				elected = rf.campaign()
			}
			rf.mu.Unlock()
		case <-rf.resetCh:
			elected = make(chan struct{})
			continue
		case <-elected:
			rf.mu.Lock()
			if rf.role != candidate {
				rf.mu.Unlock()
				continue
			}
			rf.role = leader
			rf.logger.Info("elected to be leader")

			rf.nextIndex = make([]int, len(rf.peers))
			for i := range rf.nextIndex {
				rf.nextIndex[i] = rf.log.Len()
			}
			rf.matchIndex = make([]int, len(rf.peers))
			for i := range rf.matchIndex {
				rf.matchIndex[i] = 0
			}
			// [raft-paper]Upon election: send initial empty AppendEntries RPCs(heartbeat) to each server;
			// repeat during idle periods to prevent election timeout
			go rf.notify()
			rf.mu.Unlock()
			rf.notifyCh <- struct{}{}
			return
		}
	}
}

func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	index = -1
	term = -1
	isLeader = true

	rf.mu.Lock()
	if rf.role != leader {
		isLeader = false
		rf.mu.Unlock()
		return
	}

	index = rf.log.Len()
	term = rf.currentTerm
	isLeader = true
	// [raft-paper]If command received from client: append entry to local log,
	// respond after entry applied to state machine
	rf.log.Append(&LogEntry{Command: command, Term: rf.currentTerm})

	rf.persist()

	rf.matchIndex[rf.me] = index // NOTE: important

	rf.mu.Unlock()

	rf.notifyCh <- struct{}{}

	return index, term, isLeader
}

// DiscardOldLog discards all log entries before index(exclusively)
// index must >= commitIndex, usually it should <= lastApplied <= commitIndex
func (rf *Raft) DiscardOldLog(index int, snapshot []byte) {
	rf.mu.Lock()
	if index > rf.log.LastIncludedIndex {
		rf.log.Truncate(index-1, front)
		rf.persist(snapshot)
	}
	rf.mu.Unlock()
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func NewRaft(peers []rpc.Endpoint, me int,
	persister persist.Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.role = follower

	rf.logger = log.WithField("me", rf.me)

	rf.resignCh = make(chan struct{})
	rf.resetCh = make(chan struct{})

	rf.log = &Logs{
		Entries:           nil,
		Base:              1,
		LastIncludedIndex: 0,
		LastIncludedTerm:  -1,
	}

	rf.notifyCh = make(chan struct{})

	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.commitIndex = rf.log.LastIncludedIndex
	rf.lastApplied = rf.log.LastIncludedIndex
	if persister.SnapshotSize() > 0 {
		rf.applySnapshot(persister.ReadSnapshot())
	}

	rf.applyCond = sync.NewCond(&rf.mu)
	go rf.wait()
	go rf.apply()

	if err := rpc.Register(rf); err != nil {
		panic(err)
	}

	return rf
}
