package raft

import "time"

func (rf *Raft) heartbeat_ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(TIMER_RESOLUTION * time.Millisecond)
		rf.mu.Lock()
		if !time.Now().After(rf.HeartBeatExpireTime) || rf.roler != LEADER {
			rf.mu.Unlock()
			continue
		}

		// heartbeat timeout, send!
		DebugSendHB(rf.me, rf.currentTerm)
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}

			var logEntrySend LogEntry
			if rf.nextIndex[i] == len(rf.log) {
				logEntrySend = LogEntry{-1, nil}
			} else {
				logEntrySend = rf.log[rf.nextIndex[i]]
			}
			go rf.CallAppendEntries(i, rf.currentTerm, rf.me, rf.nextIndex[i]-1, rf.log[rf.nextIndex[i]-1].Term, logEntrySend, rf.commitIdx)
		}
		rf.ResetHBTimer(false)
		rf.mu.Unlock()
	}
}

func (rf *Raft) CallAppendEntries(idx int, term int, me int, prevLogIndex int, prevLogTerm int, entry LogEntry, leaderCommit int) {
	args := AppendEntriesArgs{}
	reply := AppendEntriesReply{}
	args.Term = term
	args.LeaderId = me
	args.PrevLogIndex = prevLogIndex
	args.PrevLogTerm = prevLogTerm
	args.Entries = make([]LogEntry, 0)
	if entry.Term != -1 {
		args.Entries = append(args.Entries, entry)
	}
	args.LeaderCommit = leaderCommit
	ok := rf.sendAppendEntries(idx, &args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// check term
		if reply.Term > rf.currentTerm {
			rf.changeToFollower(reply.Term)
			return
		}
		if reply.Success {
			// update nextIndex
			if rf.nextIndex[idx] == prevLogIndex+1 {
				rf.nextIndex[idx] = min(rf.nextIndex[idx]+1, len(rf.log))
				rf.matchIndex[idx] = rf.nextIndex[idx] - 1
			}
			// else means this is out of date reply
		} else {
			// does not match
			if rf.nextIndex[idx] == prevLogIndex+1 {
				rf.nextIndex[idx] -= 1
			}
		}

		// check whether can update commitIndex
		diff := make([]int, len(rf.log)+5)
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			diff[0] += 1
			diff[rf.matchIndex[i]+1] -= 1
		}
		ok_idx := 0
		for i := 1; i < len(diff); i++ {
			diff[i] += diff[i-1]
			if diff[i] > len(diff)/2 {
				ok_idx = i
			}
		}
		if rf.log[ok_idx].Term == rf.currentTerm {
			rf.commitIdx = ok_idx
		}
	}
}
