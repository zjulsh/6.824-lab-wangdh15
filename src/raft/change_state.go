package raft

// this function need rf.mu is hold
func (rf *Raft) changeToFollower(new_term int) {
	DebugToFollower(rf, new_term)
	rf.roler = FOLLOWER
	if new_term > rf.currentTerm {
		rf.votedFor = -1
	}
	rf.currentTerm = new_term
	rf.persist()
}

// this funciton need rf.mu is hold
func (rf *Raft) changeToLeader() {

	DebugToLeader(rf.me, rf.currentTerm, rf.receiveVoteNum)

	rf.roler = LEADER
	// candidate to leader no need to change term

	// TODO initialize some state? nextId MatchID etc.
	// initialized the nextIndex[] and matchIndex[]
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.getLastIndex() + 1
		rf.matchIndex[i] = 0
	}
}

// this function need rf.mu is hole
func (rf *Raft) changeToCandidate() {
	rf.roler = CANDIDATE
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.receiveVoteNum = 1
	rf.persist()
}
