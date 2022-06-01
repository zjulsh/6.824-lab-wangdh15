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
	curLastLogIndex := rf.getLastIndex()
	curFirstLogIndex := rf.getFirstIndex()

	if args.PrevLogIndex < curFirstLogIndex {
		// want to match a log in snapshot
		// normally this will not happen,
		// but if the latency is high, this may happen
		// TODO how to process!
	} else if args.PrevLogIndex > curLastLogIndex || rf.getTermByIndex(args.PrevLogIndex) != args.PrevLogTerm { // check prevIndex and prevTerm
		// if prev doesn't match, return false immediately.
		// no need to check commitIndex.
		reply.Success = false
		reply.Term = args.Term

		// optimzed method from https://thesquareplanet.com/blog/students-guide-to-raft/
		if args.PrevLogIndex > curLastLogIndex {
			reply.ConflictIndex = rf.getLastIndex() + 1
			reply.ConflictTerm = -1
		} else {
			reply.ConflictTerm = rf.getTermByIndex(args.PrevLogIndex)
			findIdx := args.PrevLogIndex
			// find the index of the log of conflictTerm
			for i := args.PrevLogIndex; i > rf.getFirstIndex(); i-- {
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
			if rf.getTermByIndex(args.PrevLogIndex+1+i) != args.Entries[i].Term {
				break
			}
			last_match_idx = args.PrevLogIndex + 1 + i
		}

		if last_match_idx-args.PrevLogIndex != len(args.Entries) {
			// partially match
			rf.log = rf.log[0 : last_match_idx+1-rf.getFirstIndex()]
			rf.log = append(rf.log, args.Entries[last_match_idx-args.PrevLogIndex:]...)
			rf.persist()
		}

		// this must check, because may receive a out of data request with small commitIdx
		old_commit_index := rf.commitIdx
		if args.LeaderCommit > rf.commitIdx {
			rf.commitIdx = min(args.LeaderCommit, rf.getLastIndex())
		}
		if rf.commitIdx > old_commit_index {
			for i := old_commit_index + 1; i <= rf.commitIdx; i++ {
				rf.applyQueue = append(rf.applyQueue, ApplyMsg{
					CommandValid: true,
					CommandIndex: i,
					Command:      rf.getCommandByIndex(i),
				})
			}
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
