package raft

import "time"

func (rf *Raft) election_ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(TIMER_RESOLUTION * time.Millisecond)
		rf.mu.Lock()
		if time.Now().After(rf.ElectionExpireTime) && (rf.roler == FOLLOWER || rf.roler == CANDIDATE) {
			// start new election
			DebugELT(rf.me, rf.currentTerm+1)
			rf.changeToCandidate()
		}
		rf.mu.Unlock()
	}
	Debug(dInfo, "S%d ELT Goroutine Exist!", rf.me)
}

func (rf *Raft) CallForVote(idx, term int) {
	args := RequestVoteArgs{}
	reply := RequestVoteReply{}
	args.Term = term
	args.CandidateId = rf.me
	ok := rf.sendRequestVote(idx, &args, &reply)

	// following the instruction of TA
	// put the rpc process to every goroutine
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// check current roler
		if rf.roler != CANDIDATE {
			// have change to leader or follower,
			// then the requestVote reply is ignored
			return
		}
		// check whether term change
		if rf.currentTerm > term {
			// out of date reply
			return
		}

		// compare reply term and current term
		if reply.Term > rf.currentTerm {
			// receive bigger term,
			// change self to follower
			rf.changeToFollower(reply.Term)
			return
		}

		if reply.VoteGranted {
			DebugGetVote(rf.me, idx, term)
			rf.receiveVoteNum += 1
			if rf.receiveVoteNum > len(rf.peers)/2 {
				// get enough vote, so change to leader
				rf.changeToLeader()
			}
		}
	}
	// } else {
	// 	Debug(dVote, "S%d <--x-- S%d at T%d", rf.me, idx, term)
	// }
}
