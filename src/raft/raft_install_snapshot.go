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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dSnap, "S%d at T%d GetAPP IDX:%d, SnapShot: %v", rf.me, rf.currentTerm, index, snapshot)
	// check the parameters
	if index <= rf.getFirstIndex() {
		Debug(dError, "S%d Application Set out of date snapshot!, Index: %d, FirstIndex: %d", rf.me, index, rf.getFirstIndex())
		return
	}

	firstLogIndex := rf.getFirstIndex()
	lastLogIndex := rf.getLastIndex()
	old_log := rf.log
	rf.log = make([]LogEntry, lastLogIndex-index+1)
	copy(rf.log, old_log[index-firstLogIndex:])
	rf.log[0].Command = nil // delete the first dummy command

	state_serilize_result := SerilizeState(rf)

	rf.persister.SaveStateAndSnapshot(state_serilize_result, snapshot)
	Debug(dPersist, "S%d Persiste Snapshot Before Index: %d", rf.me, index)
	Debug(dSnap, "S%d at T%d After GetAPP, log is %v", rf.me, rf.currentTerm, rf.log)
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
		reply.Term = args.Term
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
				for i := args.LastIncludedIndex + 1; i <= rf.commitIdx; i++ {
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
		rf.changeToFollower(args.Term, -1)
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
		// check term
		if reply.Term < rf.currentTerm {
			return
		}
		if reply.Term > rf.currentTerm {
			// get bigger term
			rf.changeToFollower(reply.Term, -1)
			rf.ResetElectionTimer()
			return
		}

		// check args.Term and curTerm
		if args.Term != rf.currentTerm {
			// outofdate reply!
			return
		}

		// when reach hear
		// args.Term == rf.currentTerm == reply.Term
		Debug(dSnap, "S%d Receive Snap Reply from S%d", rf.me, idx)
		if lastIncludedIndex > rf.matchIndex[idx] {
			Debug(dTrace, "S%d Change the MIX and NIX of S%d, MID: %d->%d, NIX: %d->%d", rf.me, idx, rf.matchIndex[idx], lastIncludedIndex, rf.nextIndex[idx], lastIncludedIndex+1)
			rf.matchIndex[idx] = lastIncludedIndex
			rf.nextIndex[idx] = lastIncludedIndex + 1
		}
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}
