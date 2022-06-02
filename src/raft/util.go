package raft

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"6.824/labgob"
)

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

// send vote
func DebugGrantVote(s1, s2, term int) {
	Debug(dVote, "S%d -> S%d at T%d", s1, s2, term)
}

// receive vote
func DebugGetVote(s1, s2, term int) {
	Debug(dVote, "S%d <- S%d at T%d", s1, s2, term)
}

// become to leader
func DebugToLeader(s, term, num int) {
	Debug(dLeader, "S%d Receive Majority for T%d (%d), converting to Leader", s, term, num)
}

// become to follower
func DebugToFollower(rf *Raft, new_term int) {
	Debug(dTrace, "S%d Change State From [%s:%d] To [F:%d]", rf.me, roler_string[rf.roler], rf.currentTerm, new_term)
}

// election timeout
func DebugELT(s, term int) {
	Debug(dTimer, "S%d Election Timeout, Begin Election for T%d", s, term)
}

func DebugReceiveHB(s1, s2, term int) {
	Debug(dTimer, "S%d <--HB-- S%d at T%d", s1, s2, term)
}

func DebugGetInfo(rf *Raft) {
	Debug(dInfo, "S%d T%d Roler: %s Log:%v", rf.me, rf.currentTerm, roler_string[rf.roler], rf.log)
}

func DebugReceiveAppendEntries(rf *Raft, entry *AppendEntriesArgs) {
	Debug(dLog, "S%d <- S%d at T%d PLI:%d PLT: %d LC:%d - %v", rf.me, entry.LeaderId, entry.Term, entry.PrevLogIndex, entry.PrevLogTerm, entry.LeaderCommit, entry.Entries)
	Debug(dLog, "S%d at T%d Before Reply AppendEntries. CI:%d, log is - %v", rf.me, rf.currentTerm, rf.commitIdx, rf.log)
}

func DebugResetELT(rf *Raft) {
	Debug(dTimer, "S%d at T%d Reset ELT to %06d", rf.me, rf.currentTerm, rf.ElectionExpireTime.Sub(debugStart).Microseconds()/100)
}

func DebugResetHBT(rf *Raft, idx int) {
	Debug(dTimer, "S%d at T%d Reset HBT to %06d", rf.me, rf.currentTerm, rf.AppendExpireTime[idx].Sub(debugStart).Microseconds()/100)
}

func DebugNewCommand(rf *Raft) {
	Debug(dLeader, "S%d T%d Roler: %s Receive New Command, After Log:%v", rf.me, rf.currentTerm, roler_string[rf.roler], rf.log)
}

func DebugAfterReceiveAppendEntries(rf *Raft, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	Debug(dLog, "S%d at T%d Reply AppendEntries From S%d at T%d With [OK: %v T:%v CFT: %d CFI: %d]", rf.me, rf.currentTerm, args.LeaderId, args.Term, reply.Success, reply.Term, reply.ConflictTerm, reply.ConflictIndex)
	Debug(dLog, "S%d at T%d After Reply AppendEntries. CI:%d, log is - %v", rf.me, rf.currentTerm, rf.commitIdx, rf.log)
}

//
// get a random expire time range [cur_time + left, cur_time + right]
//
func GetRandomExpireTime(left, right int32) time.Time {
	t := rand.Int31n(right - left)
	// log.Printf("%d", t)
	return time.Now().Add(time.Duration(t+left) * time.Millisecond)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func SerilizeState(rf *Raft) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	return w.Bytes()
}
