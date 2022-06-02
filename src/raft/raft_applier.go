package raft

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// async send commit to ApplyCh
// this is a producer and consumer model
// the return result of AppendEntries modify the commitIdx, means push element to queue
// this is a consumer, which add lastApplied Index to consume.
func (rf *Raft) Applier(applyCh chan ApplyMsg) {
	for !rf.killed() {
		rf.mu.Lock()
		for len(rf.commitQueue) == 0 {
			rf.cv.Wait()
		}
		msgs := rf.commitQueue
		rf.commitQueue = make([]ApplyMsg, 0)
		rf.mu.Unlock()
		for _, msg := range msgs {
			if msg.CommandValid {
				Debug(dLog2, "S%d Apply Commnd IDX%d CMD: %v", rf.me, msg.CommandIndex, msg.Command)
			} else if msg.SnapshotValid {
				Debug(dLog2, "S%d Apply Snapshot. LII: %d, %d LIT: %d, snapShot: %v", rf.me, msg.SnapshotIndex, msg.SnapshotTerm, msg.Snapshot)
			} else {
				Debug(dError, "S%d, Apply unknown Command!", rf.me)
			}
			// this may block
			applyCh <- msg
		}
	}
}
