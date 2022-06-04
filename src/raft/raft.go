package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

const (
	LEADER    = 0
	CANDIDATE = 1
	FOLLOWER  = 2
)

var roler_string map[int]string

const (
	ELECTION_TIMER_RESOLUTION = 5 // check whether timer expire every 5 millisecond.
	// vote expire time range. (millsecond)
	ELECTION_EXPIRE_LEFT  = 200
	ELECTION_EXPIRE_RIGHT = 400
	// heartbeat time (millsecond)
	APPEND_EXPIRE_TIME      = 100
	APPEND_TIMER_RESOLUTION = 2
)

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu sync.Mutex // Lock to protect shared access to this peer's state
	cv *sync.Cond // the cv for sync producer and consumer

	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm    int
	votedFor       int // -1 represent null
	receiveVoteNum int
	log            []LogEntry

	roler int

	ElectionExpireTime time.Time   // election expire time
	AppendExpireTime   []time.Time // next send append time

	// index of highest log entry known to be commited
	// (initialized to 0, increase monotonically)
	commitIdx int

	// AppendEntries RPC should send peer_i the nextIndex[peer_i] log
	// to peer_i
	nextIndex []int

	// matchIndex[i] means the matched log of peer_i and
	// this leader is [1-matchIndex[i]]
	// the matchIndex changes with nextIndex
	// and it influences the update of commitIndex
	matchIndex []int

	// async apply committed log or snapshot!
	commitQueue []ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.roler == LEADER
	DebugGetInfo(rf)
	return term, isleader
}

// get the first dummy log index
func (rf *Raft) getFirstIndex() int {
	return rf.log[0].Index
}

// get the first dummy log term
func (rf *Raft) getFirstTerm() int {
	return rf.log[0].Term
}

// get the last log term
func (rf *Raft) getLastTerm() int {
	return rf.log[len(rf.log)-1].Term
}

// get the last log index
func (rf *Raft) getLastIndex() int {
	return rf.log[len(rf.log)-1].Index
}

// get the Term of index
// compute the location in log and return the result
func (rf *Raft) getTermForIndex(index int) int {
	return rf.log[index-rf.getFirstIndex()].Term
}

// get the command of index
// compute the location in log and return the result
func (rf *Raft) getCommand(index int) interface{} {
	return rf.log[index-rf.getFirstIndex()].Command
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return -1, -1, false
	}
	if rf.roler != LEADER {
		return -1, -1, false
	}

	// this is a live leader,
	// append this command to its log and return
	// the HBT timer will sync this log to other peers
	DebugNewCommand(rf)
	rf.log = append(rf.log, LogEntry{rf.currentTerm, rf.getLastIndex() + 1, command})
	rf.persist()
	// send appendEntry immediately
	for i := 0; i < len(rf.peers); i++ {
		rf.ResetAppendTimer(i, true)
	}
	return rf.getLastIndex(), rf.currentTerm, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	roler_string = map[int]string{
		LEADER:    "L",
		CANDIDATE: "C",
		FOLLOWER:  "F",
	}

	num_servers := len(peers)

	rf := &Raft{
		peers:            peers,
		persister:        persister,
		me:               me,
		currentTerm:      0,
		votedFor:         -1,
		log:              make([]LogEntry, 0),
		roler:            FOLLOWER,
		AppendExpireTime: make([]time.Time, num_servers),
		nextIndex:        make([]int, num_servers),
		matchIndex:       make([]int, num_servers),
		commitQueue:      make([]ApplyMsg, 0),
	}
	rf.cv = sync.NewCond(&rf.mu)
	// add a dummy log
	rf.log = append(rf.log, LogEntry{
		Index:   0,
		Term:    0,
		Command: nil,
	})
	rf.ResetElectionTimer()
	for i := 0; i < num_servers; i++ {
		rf.ResetAppendTimer(i, false)
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// initialize commitIndex after recover
	rf.commitIdx = rf.getFirstIndex()

	// start ticker goroutine to check Election and appendEntry
	go rf.election_ticker()
	go rf.append_ticker()

	// start async apply goroutine
	go rf.Applier(applyCh)

	Debug(dInfo, "Start S%d", me)

	return rf
}
