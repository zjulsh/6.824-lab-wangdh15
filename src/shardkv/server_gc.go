package shardkv

import "time"

type GCArgs struct {
	CfgNum  int
	ShardId int
}

type GCReply struct {
	Error Err
}

func (kv *ShardKV) sendGc(servers []string, cfg_num, shardId int) {

	args := GCArgs{
		CfgNum:  cfg_num,
		ShardId: shardId,
	}
	for {
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			reply := GCReply{}
			ok := srv.Call("ShardKV.GC", &args, &reply)
			if ok && reply.Error == OK {
				return
			} else {
				continue
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) GC(args *GCArgs, reply *GCReply) {
	kv.mu.Lock()
	if kv.cur_config.Num <= args.CfgNum {
		reply.Error = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	if kv.allData[args.CfgNum][args.ShardId].Status != EXPIRED {
		// 已经被GC了，直接返回OK
		reply.Error = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	// 加入到raft层进行GC
	op := Op{
		Type:      GC,
		GCCfgNum:  args.CfgNum,
		GCShardID: args.ShardId,
		ServerSeq: nrand(),
	}
	rec_chan := make(chan OpResult, 1)
	kv.mu.Lock()
	kv.chans[op.ServerSeq] = rec_chan
	kv.mu.Unlock()
	if _, _, ok1 := kv.rf.Start(op); ok1 {
		select {
		case <-time.After(TIMEOUT * time.Millisecond):
			// timeout!
			reply.Error = "TIMEOUT"
		case res := <-rec_chan:
			// this op has be processed!
			reply.Error = res.Error
		}
	} else {
		reply.Error = ErrWrongLeader
	}
	kv.mu.Lock()
	delete(kv.chans, op.ServerSeq)
	kv.mu.Unlock()
}
