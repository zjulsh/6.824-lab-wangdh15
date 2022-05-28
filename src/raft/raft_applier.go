package raft

// async send commit to ApplyCh
// this is a producer and consumer model
// the return result of AppendEntries modify the commitIdx, means push element to queue
// this is a consumer, which add lastApplied Index to consume.
func (rf *Raft) Applier(applyCh chan ApplyMsg) {
	msg := ApplyMsg{}
	for !rf.killed() {

		rf.mu.Lock()
		for rf.lastApplied == rf.commitIdx {
			rf.cv.Wait()
		}

		msg.Command = rf.log[rf.lastApplied+1].Command
		msg.CommandIndex = rf.lastApplied + 1
		msg.CommandValid = true

		Debug(dLog2, "S%d Apply Commnd T%d IDX%d CMD: %v", rf.me, rf.log[rf.lastApplied+1].Term, msg.CommandIndex, msg.CommandIndex)

		rf.lastApplied += 1
		rf.mu.Unlock()
		// this my block
		applyCh <- msg

	}
}
