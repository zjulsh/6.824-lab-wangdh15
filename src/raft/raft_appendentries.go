package raft

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DebugReceiveAppendEntries(rf, args)
	defer DebugAfterReceiveAppendEntries(rf, args, reply)

	// check args.Term and curTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	// if args.Term >= curTerm, can do the following things.
	curLastLogIndex := len(rf.log) - 1

	// check prevIndex and prevTerm
	if args.PrevLogIndex > curLastLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// if prev doesn't match, return false immediately.
		// no need to check commitIndex.
		reply.Success = false
		reply.Term = args.Term

		// optimzed method from https://thesquareplanet.com/blog/students-guide-to-raft/
		if args.PrevLogIndex > curLastLogIndex {
			reply.ConflictIndex = len(rf.log)
			reply.ConflictTerm = -1
		} else {
			reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
			findIdx := args.PrevLogIndex
			// find the index of the log of conflictTerm
			for i := args.PrevLogIndex; i > 0; i-- {
				if rf.log[i-1].Term != reply.ConflictTerm {
					findIdx = i
					break
				}
			}
			reply.ConflictIndex = findIdx
		}
	} else {
		// prev match!
		reply.Success = true
		reply.Term = args.Term
		// check whether match all log in args
		last_match_idx := args.PrevLogIndex
		for i := 0; i < len(args.Entries); i++ {
			if args.PrevLogIndex+1+i > curLastLogIndex {
				break
			}
			if rf.log[args.PrevLogIndex+1+i].Term != args.Entries[i].Term {
				break
			}
			last_match_idx = args.PrevLogIndex + 1 + i
		}

		if last_match_idx-args.PrevLogIndex != len(args.Entries) {
			// partially match
			rf.log = rf.log[0 : last_match_idx+1]
			rf.log = append(rf.log, args.Entries[last_match_idx-args.PrevLogIndex:]...)
			rf.persist()
		}
		// if args.Entries all match, do notiong!, donot delete the remain log
		// is rf.logs, because this request may be a out of date request!

		// if len(args.Entries) > 0 {
		// 	var last_match_idx = args.PrevLogIndex
		// 	for i := 1; i <= len(args.Entries) && args.PrevLogIndex+i < len(rf.log); i++ {
		// 		if args.Entries[i-1].Term == rf.log[args.PrevLogIndex+i].Term {
		// 			last_match_idx = args.PrevLogIndex + i
		// 		} else {
		// 			break
		// 		}
		// 	}
		// 	// new one not match, delete remain log and append all entries
		// 	// the requirements is:
		// 	// 1. have new entry
		// 	// 2. the new entry is not in this logs or the log.Term doesn't match
		// 	rf.log = rf.log[0 : last_match_idx+1]
		// 	rf.log = append(rf.log, args.Entries[last_match_idx-args.PrevLogIndex:]...)
		// 	rf.persist()
		// }
		// no new one or new one is matched, do nothing.
		// this can  happen when receive the out of date RPC
		// 1. send this rpc1 first
		// 2. this rpc1 timeout, resend rpc2
		// 3. receive rpc2, process
		// 4. receive rpc1, the prev match and the new one match(rpc2 update the newont)

		// this must check, because may receive a out of data request with small commitIdx
		if args.LeaderCommit > rf.commitIdx {
			rf.commitIdx = min(args.LeaderCommit, len(rf.log)-1)
		}

		if rf.commitIdx > rf.lastApplied {
			// nofity applier
			rf.cv.Broadcast()
		}
	}

	// if args.Term bigger than rf.currTerm or term equal but this is not a follower,
	// change self to follower
	if args.Term > rf.currentTerm || rf.roler != FOLLOWER {
		// change self to follower
		rf.changeToFollower(args.Term)
	}
	// reset vote expire time
	rf.ResetElectionTimer()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
