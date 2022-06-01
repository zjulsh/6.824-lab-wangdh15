package raft

// async send commit to ApplyCh
// this is a producer and consumer model
// the return result of AppendEntries modify the commitIdx, means push element to queue
// this is a consumer, which add lastApplied Index to consume.
func (rf *Raft) Applier(applyCh chan ApplyMsg) {
	for !rf.killed() {

		rf.mu.Lock()
		for len(rf.applyQueue) == 0 {
			rf.cv.Wait()
		}
		msgs := rf.applyQueue
		rf.applyQueue = make([]ApplyMsg, 0)
		rf.mu.Unlock()

		for _, msg := range msgs {
			applyCh <- msg
			if msg.CommandValid {
				Debug(dLog2, "S%d Apply Commnd IDX%d CMD: %v", rf.me, msg.CommandIndex, msg.Command)
			} else if msg.SnapshotValid {
				Debug(dSnap, "S%d Apply Snap. LII: %d, LIT: %d, DATA: %v", rf.me, msg.SnapshotIndex, msg.SnapshotTerm, msg.Snapshot)
			} else {
				Debug(dError, "S%d Apply Unkonwn Mssage!", rf.me)
			}
		}
	}
}
