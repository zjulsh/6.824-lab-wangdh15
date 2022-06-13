package shardkv

import "time"

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	Debug(dKVGet, "S%d GID:%d Receive Get Request. args: %v", kv.me, kv.gid, args)
	shardId := key2shard(args.Key)
	kv.mu.Lock()
	if kv.allData[kv.cur_config.Num][shardId].Status != WORKING {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	// the sharding status is working
	if last_process_seq, ok := kv.allData[kv.cur_config.Num][shardId].Client_to_last_req[args.ClientId]; ok {
		if last_process_seq == args.ClientSeq {
			reply.Err = kv.allData[kv.cur_config.Num][shardId].Client_to_last_res[args.ClientId].Error
			reply.Value = kv.allData[kv.cur_config.Num][shardId].Client_to_last_res[args.ClientId].Value
			kv.mu.Unlock()
			return
		} else if last_process_seq > args.ClientSeq {
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()

	op := Op{
		Type:      GET,
		Key:       args.Key,
		ClientId:  args.ClientId,
		ClientSeq: args.ClientSeq,
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
			reply.Err = "TIMEOUT"
		case res := <-rec_chan:
			// this op has be processed!
			reply.Err = res.Error
			reply.Value = res.Value
		}
	} else {
		reply.Err = ErrWrongLeader
	}
	kv.mu.Lock()
	delete(kv.chans, op.ServerSeq)
	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	Debug(dKVAppend, "S%d GID:%d Receive PutAppend Request. args: %v", kv.me, kv.gid, args)
	shardId := key2shard(args.Key)
	kv.mu.Lock()
	if kv.allData[kv.cur_config.Num][shardId].Status != WORKING {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	// the sharding status is working
	if last_process_seq, ok := kv.allData[kv.cur_config.Num][shardId].Client_to_last_req[args.ClientId]; ok {
		if last_process_seq == args.ClientSeq {
			reply.Err = kv.allData[kv.cur_config.Num][shardId].Client_to_last_res[args.ClientId].Error
			kv.mu.Unlock()
			return
		} else if last_process_seq > args.ClientSeq {
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()

	op := Op{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		ClientSeq: args.ClientSeq,
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
			reply.Err = "TIMEOUT"
		case res := <-rec_chan:
			// this op has be processed!
			reply.Err = res.Error
		}
	} else {
		reply.Err = ErrWrongLeader
	}
	kv.mu.Lock()
	delete(kv.chans, op.ServerSeq)
	kv.mu.Unlock()
}
