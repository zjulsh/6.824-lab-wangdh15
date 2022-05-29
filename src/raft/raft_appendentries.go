package raft

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DebugReceiveAppendEntries(rf, args)
	defer DebugAfterReceiveAppendEntries(rf, args, reply)

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	curLastLogIndex := len(rf.log) - 1

	if args.PrevLogIndex > curLastLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// if prev doesn't match, return false immediately.
		// no need to check commitIndex.
		reply.Success = false
		reply.Term = args.Term
	} else {
		// prev match!
		reply.Success = true
		reply.Term = args.Term
		if len(args.Entries) > 0 && (curLastLogIndex < args.PrevLogIndex+1 || rf.log[args.PrevLogIndex+1].Term != args.Entries[0].Term) {
			// new one not match, delete remain log and append all entries
			// the requirements is:
			// 1. have new entry
			// 2. the new entry is not in this logs or the log.Term doesn't match
			rf.log = rf.log[0 : args.PrevLogIndex+1]
			rf.log = append(rf.log, args.Entries...)
		}
		// no new one or new one is matched, do nothing.
		// this can  happen when receive the out of date RPC
		// 1. send this rpc1 first
		// 2. this rpc1 timeout, resend rpc2
		// 3. receive rpc2, process
		// 4. receive rpc1, the prev match and the new one match(rpc2 update the newont)

		if args.LeaderCommit > rf.commitIdx {
			rf.commitIdx = min(args.LeaderCommit, len(rf.log)-1)
		}

		if rf.commitIdx > rf.lastApplied {
			// nofity applier
			rf.cv.Broadcast()
		}
	}

	if args.Term > rf.currentTerm || rf.roler != FOLLOWER {
		// change self to follower
		rf.changeToFollower(args.Term)
	} else {
		// reset vote expire time
		rf.ResetElectionTimer()
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
