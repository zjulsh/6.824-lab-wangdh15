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

			logs := make([]LogEntry, len(rf.log)-rf.nextIndex[i])
			copy(logs, rf.log[rf.nextIndex[i]:])
			go rf.CallAppendEntries(i, rf.currentTerm, rf.me, rf.nextIndex[i]-1, rf.log[rf.nextIndex[i]-1].Term, logs, rf.commitIdx)
		}
		rf.ResetHBTimer(false)
		rf.mu.Unlock()
	}
}

func (rf *Raft) CallAppendEntries(idx int, term int, me int, prevLogIndex int, prevLogTerm int, logs []LogEntry, leaderCommit int) {

	args := AppendEntriesArgs{}
	reply := AppendEntriesReply{}
	args.Term = term
	args.LeaderId = me
	args.PrevLogIndex = prevLogIndex
	args.PrevLogTerm = prevLogTerm
	args.Entries = logs
	args.LeaderCommit = leaderCommit

	Debug(dLog, "S%d T%d Send AppendEntries to S%d, Args is %v", me, term, idx, args)

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

			if len(args.Entries) == 0 {
				// this is a hearbeat, no porcess.
				// if process, after send this rpc, leader
				// receive new command, the leader whill think alreay
				// receive the new command.
				return
			}

			// update nextIndex
			if rf.nextIndex[idx] == prevLogIndex+1 {
				rf.nextIndex[idx] = min(rf.nextIndex[idx]+len(logs), len(rf.log))
				rf.matchIndex[idx] = rf.nextIndex[idx] - 1
			}
			// else means this is out of date reply
		} else {
			// does not match
			// check this is not a out of date request
			if rf.nextIndex[idx] == prevLogIndex+1 {

				// optimized method
				// from https://thesquareplanet.com/blog/students-guide-to-raft/
				if reply.ConflictTerm == -1 {
					rf.nextIndex[idx] = reply.ConflictIndex
				} else {
					findIdx := -1
					for i := len(rf.log); i > 0; i-- {
						if rf.log[i-1].Term == reply.ConflictTerm {
							findIdx = i
							break
						}
					}
					if findIdx != -1 {
						// if find a log of conflict term,
						// set to the one beyond the index
						rf.nextIndex[idx] = findIdx
					} else {
						// set the nextIdx to the conflict firstLog index of peer's logs
						rf.nextIndex[idx] = reply.ConflictIndex
					}
				}

				// no oprimized method
				// rf.nextIndex[idx] -= 1
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
			if diff[i]+1 > len(rf.peers)/2 {
				ok_idx = i
			}
		}

		if ok_idx > rf.commitIdx && rf.log[ok_idx].Term == rf.currentTerm {
			// if there is new commit Log, notify the applier to send
			Debug(dLog, "S%d at T%d Reset it CI From %d to %d", rf.me, rf.currentTerm, rf.commitIdx, ok_idx)
			rf.commitIdx = ok_idx
			// rf.cv.Broadcast()
		}
		if rf.commitIdx > rf.lastApplied {
			rf.cv.Broadcast()
		}
	}
}
