package raft

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	DebugReceiveAppendEntries(rf, args)

	curLastLogIndex := len(rf.log) - 1

	if args.PrevLogIndex > curLastLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = args.Term
	} else {
		reply.Success = true
		reply.Term = args.Term
		// prev match!
		if len(args.Entries) > 0 && curLastLogIndex >= args.PrevLogIndex+1 && rf.log[args.PrevLogIndex+1].Term != args.Entries[0].Term {
			// new one done match, delete remain log
			rf.log = rf.log[0 : args.PrevLogIndex+1]
			rf.log = append(rf.log, args.Entries...)
		}
		// no new one or new one is matched, whichi means the following also match, do nothing.
	}
	if args.LeaderCommit > rf.commitIdx {
		rf.commitIdx = min(args.LeaderCommit, curLastLogIndex)
	}

	if args.Term > rf.currentTerm || rf.roler == CANDIDATE {
		// change self to follower
		rf.changeToFollower(args.Term)
	} else {
		// reset vote expire time
		rf.ResetElectionTimer()
	}

	// if reply.Success {
	// 	DebugReceiveHB(rf.me, args.LeaderId, args.Term)
	// }

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
