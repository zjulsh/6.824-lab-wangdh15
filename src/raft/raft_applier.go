package raft

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
		// get all logs and send to allyCh
		for _, msg := range msgs {
			if msg.CommandValid {
				Debug(dLog2, "S%d Apply Commnd T%d IDX%d CMD: %v", rf.me, rf.log[rf.lastApplied+1].Term, msg.CommandIndex, msg.CommandIndex)
			} else if msg.SnapshotValid {
				Debug(dLog2, "S%d Apply Snapshot. LastIndex: %d, snapShot: %v", rf.me, msg.SnapshotIndex, msg.Snapshot)
			} else {
				Debug(dError, "S%d, Apply unknown Command!", rf.me)
			}
			// this may block
			applyCh <- msg
		}
	}
}
