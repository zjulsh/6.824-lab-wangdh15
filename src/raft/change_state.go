package raft

// this function need rf.mu is hold
func (rf *Raft) changeToFollower(new_term int) {
	rf.roler = FOLLOWER
	rf.currentTerm = new_term
	// reset election timeout timer
	rf.ResetElectionTimer()
}

// this funciton need rf.mu is hold
func (rf *Raft) changeToLeader() {

	DebugToLeader(rf.me, rf.currentTerm, rf.receiveVoteNum)

	rf.roler = LEADER
	// candidate to leader no need to change term

	// TODO initialize some state? nextId MatchID etc.
	// initialized the nextIndex[] and matchIndex[]
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	// reset all hearbeat timer to send heartbeat immediately
	rf.ResetHBTimer(true)
}

// this function need rf.mu is hole
// changeToCandidate alse mean start new election
func (rf *Raft) changeToCandidate() {
	rf.roler = CANDIDATE
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.receiveVoteNum = 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.CallForVote(i, rf.currentTerm)
	}
	rf.ResetElectionTimer()
}
