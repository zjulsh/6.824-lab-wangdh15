package raft

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// check term
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	Debug(dSnap, "S%d Receive Snapshot From S%d T%d, LII: %d, LIT:%d, Snap:%v",
		rf.me, args.LeaderId, args.Term,
		args.LastIncludedIndex, args.LastIncludedTerm,
		args.Snapshot)
	Debug(dSnap, "S%d Beform Process, Log is: %v", rf.me, rf.log)

	// check the snapshot with the peer's snapshot
	curSnapLastIndex := rf.getFirstIndex()
	curLogLastIndex := rf.getLastIndex()
	if args.LastIncludedIndex <= curSnapLastIndex {
		// peer's log contain more logs, ignore this rpc
		reply.Term = args.Term
	} else {
		rf.commitQueue = append(rf.commitQueue, ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			SnapshotIndex: args.LastIncludedIndex,
			SnapshotTerm:  args.LastIncludedTerm,
			Snapshot:      args.Snapshot,
		})
		if args.LastIncludedIndex < curLogLastIndex {
			// the log has some extra entry
			old_log := rf.log
			rf.log = make([]LogEntry, curLogLastIndex-args.LastIncludedIndex+1)
			copy(rf.log, old_log[args.LastIncludedIndex-curSnapLastIndex:])
			rf.log[0].Command = nil
			// if there is some entry between lastIncludeEntry and origin commitIndex
			// need to recommit them
			if args.LastIncludedIndex < rf.commitIdx {
				for i := args.LastIncludedIndex; i <= rf.commitIdx; i++ {
					rf.commitQueue = append(rf.commitQueue, ApplyMsg{
						CommandValid: true,
						CommandIndex: i,
						Command:      rf.getCommand(i),
					})
				}
			} else {
				rf.commitIdx = args.LastIncludedIndex
			}
		} else {
			// add one dummy entry
			rf.log = make([]LogEntry, 0)
			rf.log = append(rf.log, LogEntry{
				Index:   args.LastIncludedIndex,
				Term:    args.LastIncludedTerm,
				Command: nil,
			})
			rf.commitIdx = args.LastIncludedIndex
		}
		state_seri_result := SerilizeState(rf)
		rf.persister.SaveStateAndSnapshot(state_seri_result, args.Snapshot)
		rf.cv.Broadcast()
	}
	if args.Term > rf.currentTerm || rf.roler != FOLLOWER {
		rf.changeToFollower(args.Term)
	}
	rf.ResetElectionTimer()

	Debug(dSnap, "S%d After Process, Log is: %v", rf.me, rf.log)
}

func (rf *Raft) CallInstallSnapshot(idx, term, me, lastIncludedIndex, lastIncludedTerm int, data []byte) {
	args := InstallSnapshotArgs{
		Term:              term,
		LeaderId:          me,
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Snapshot:          data,
	}
	reply := InstallSnapshotReply{}
	Debug(dSnap, "S%d Send Snapshot to S%d. Args is : %v", me, idx, args)
	ok := rf.sendInstallSnapshot(idx, &args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			// get bigger term
			rf.changeToFollower(reply.Term)
			rf.ResetElectionTimer()
		} else {
			Debug(dSnap, "S%d Receive Snap Reply from S%d", rf.me, idx)
			if lastIncludedIndex > rf.matchIndex[idx] {
				rf.matchIndex[idx] = lastIncludedIndex
				rf.nextIndex[idx] = lastIncludedIndex + 1
			}
		}
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
