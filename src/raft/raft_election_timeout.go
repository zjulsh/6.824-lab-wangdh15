package raft

import "time"

func (rf *Raft) election_ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(ELECTION_TIMER_RESOLUTION * time.Millisecond)
		rf.mu.Lock()
		if time.Now().After(rf.ElectionExpireTime) && (rf.roler == FOLLOWER || rf.roler == CANDIDATE) {
			// start new election
			DebugELT(rf.me, rf.currentTerm+1)
			// change to candidate and reset ele timers
			rf.changeToCandidate()
			rf.ResetElectionTimer()
			// request vote from other peers
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go rf.CallForVote(i, rf.currentTerm, len(rf.log)-1, rf.log[len(rf.log)-1].Term)
			}
		}
		rf.mu.Unlock()
	}
	Debug(dInfo, "S%d ELT Goroutine Exist!", rf.me)
}

func (rf *Raft) CallForVote(idx, term, lastLogIndex, lastLogTerm int) {
	args := RequestVoteArgs{}
	reply := RequestVoteReply{}
	args.Term = term
	args.CandidateId = rf.me
	args.LastLogIndex = lastLogIndex
	args.LastLogTerm = lastLogTerm
	ok := rf.sendRequestVote(idx, &args, &reply)

	// following the instruction of TA
	// put the rpc process to every goroutine
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		// check relyTerm and curTem
		if reply.Term < rf.currentTerm {
			return
		}

		if reply.Term > rf.currentTerm {
			// receive bigger term,
			// change self to follower
			rf.changeToFollower(reply.Term)
			rf.ResetElectionTimer()
			return
		}

		// check current roler
		if rf.roler != CANDIDATE {
			// have change to leader or follower,
			// then the requestVote reply is ignored
			return
		}

		// check reply term and args term
		if reply.Term != args.Term {
			return
		}

		// reach here means
		// reply.Term == rf.currentTerm == args.Term
		// rf.roler == Candiadate
		if reply.VoteGranted {
			DebugGetVote(rf.me, idx, term)
			rf.receiveVoteNum += 1
			if rf.receiveVoteNum > len(rf.peers)/2 {
				// get enough vote, so change to leader
				rf.changeToLeader()
				// reset appendEntry timer
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					// send appendEntry immediately
					rf.ResetAppendTimer(i, true)
				}
			}
		}
	}
}
