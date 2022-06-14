package shardkv

func (kv *ShardKV) process_get(op Op) {
	shardId := key2shard(op.Key)
	// need to check the machine state
	res := OpResult{}
	if kv.allData[kv.cur_config.Num][shardId].Status != WORKING {
		res.Error = ErrWrongLeader
		return
	} else {
		if val, ok2 := kv.allData[kv.cur_config.Num][shardId].Data[op.Key]; ok2 {
			res.Value = val
			res.Error = OK
		} else {
			res.Error = ErrNoKey
		}
	}
	kv.update(op, res)
	kv.replyChan(op.ServerSeq, res)
	Debug(dKVGet, "S%d GID:%d Process Get. Key: %s, Val: %s, CLIID: %d, CLISQ: %d", kv.me, kv.gid, op.Key, op.Value, op.ClientId, op.ClientSeq)
}

func (kv *ShardKV) process_put(op Op) {
	shardId := key2shard(op.Key)
	res := OpResult{}
	if kv.allData[kv.cur_config.Num][shardId].Status != WORKING {
		res.Error = ErrWrongLeader
		return
	} else {
		kv.allData[kv.cur_config.Num][shardId].Data[op.Key] = op.Value
		res.Error = OK
	}
	kv.update(op, res)
	kv.replyChan(op.ServerSeq, res)
	Debug(dKVPut, "S%d GID:%d Process Put. Key: %s, Val: %s, CLIID: %d, CLISQ: %d", kv.me, kv.gid, op.Key, op.Value, op.ClientId, op.ClientSeq)
}

func (kv *ShardKV) process_append(op Op) {
	shardId := key2shard(op.Key)
	res := OpResult{}
	if kv.allData[kv.cur_config.Num][shardId].Status != WORKING {
		res.Error = ErrWrongLeader
		return
	} else {
		if val, ok2 := kv.allData[kv.cur_config.Num][shardId].Data[op.Key]; ok2 {
			kv.allData[kv.cur_config.Num][shardId].Data[op.Key] = val + op.Value
		} else {
			kv.allData[kv.cur_config.Num][shardId].Data[op.Key] = op.Value
		}
		res.Error = OK
	}
	kv.update(op, res)
	kv.replyChan(op.ServerSeq, res)
	Debug(dKVAppend, "S%d GID:%d Process APPEND. Key: %s, Val: %s, CLIID: %d, CLISQ: %d", kv.me, kv.gid, op.Key, op.Value, op.ClientId, op.ClientSeq)
}

func (kv *ShardKV) process_change_config(op Op) {
	kv.mu.Lock()
	// still need to check the requirement.
	// 因为有可能有重复的命令被加入到raft的log中。
	// 因为对状态的修改需要经过如下步骤：
	// 1. 将命令加入到raft的队列中。
	// 2. log同步之后，从chan中拿到命令，并执行对应的状态修改操作。
	// 所以加入一个命令到这个命令真正其效果中间是有延迟的，且有可能会发生易主等多种情况
	// 所以一个命令从log中拿出来的环境和它被加入的环境不一定相同，所以这里需要进行重新判断
	if kv.cur_config.Num+1 == op.NewConfig.Num {
		// 这里只需要判断Num的关系，这是因为当这个命令被加入到队列的时候，就说明cur_config的所有
		// shard的状态都被OK了
		kv.last_config = kv.cur_config
		kv.cur_config = op.NewConfig
		kv.ResetStatus()
		Debug(dKVConfig, "S%d GID:%d change config! last_config: %v, cur_config: %v, cur_all_data: %v", kv.me, kv.gid, kv.last_config, kv.cur_config, kv.allData[kv.cur_config.Num])
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) process_change_shard(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// check the configNum match and the Shard has not be set
	if kv.cur_config.Num != op.NewShardCfgNum || kv.allData[kv.cur_config.Num][op.NewShardId].Status != ACQUING {
		return
	}
	// set the shard and change the status]
	kv.allData[kv.cur_config.Num][op.NewShardId] = op.NewShard.Copy()
	kv.allData[kv.cur_config.Num][op.NewShardId].Status = WORKING
	Debug(dKVShard, "S%d GID:%d chang shard!, cur_all_data: %v", kv.me, kv.gid, kv.allData[kv.cur_config.Num])
	// send gc
	go kv.sendGc(kv.last_config.Groups[kv.last_config.Shards[op.NewShardId]], kv.last_config.Num, op.NewShardId)
}

func (kv *ShardKV) process_gc(op Op) {
	res := OpResult{}
	// check config num
	kv.mu.Lock()
	if kv.cur_config.Num <= op.GCCfgNum {
		kv.mu.Unlock()
		res.Error = ErrWrongLeader
		kv.replyChan(op.ServerSeq, res)
		return
	}
	// check whether has been gced
	if kv.allData[op.GCCfgNum][op.GCShardID].Status == INVALID {
		res.Error = OK
	} else if kv.allData[op.GCCfgNum][op.GCShardID].Status == EXPIRED {
		kv.allData[op.GCCfgNum][op.GCShardID].Client_to_last_req = nil
		kv.allData[op.GCCfgNum][op.GCShardID].Client_to_last_res = nil
		kv.allData[op.GCCfgNum][op.GCShardID].Data = nil
		kv.allData[op.GCCfgNum][op.GCShardID].Status = INVALID
		res.Error = OK
		Debug(dKVGC, "S%d GID:%d GC! CFGNUM: %d, SHARDNUM: %d. new all data: %v", kv.me, kv.gid, op.GCCfgNum, op.GCShardID, kv.allData)
	} else {
		res.Error = ErrWrongLeader
		Debug(dError, "S%d GID:%d GC Err!", kv.me, kv.gid)
	}
	kv.mu.Unlock()
	kv.replyChan(op.ServerSeq, res)
}

func (kv *ShardKV) process() {
	for command := range kv.applyCh {
		if command.CommandValid {
			op := command.Command.(Op)
			Debug(dKVApply, "S%d GID:%d Applier Receive OP: %v", kv.me, kv.gid, op)
			if op.Type == GET || op.Type == PUT || op.Type == APPEND {
				need_process, res := kv.check_dup(op)
				if !need_process {
					kv.replyChan(op.ServerSeq, res)
					continue
				}
			}
			switch op.Type {
			case GET:
				kv.process_get(op)
			case PUT:
				kv.process_put(op)
			case APPEND:
				kv.process_append(op)
			case CHANGE_CONFIG:
				kv.process_change_config(op)
			case CHANGE_SHARD:
				kv.process_change_shard(op)
			case GC:
				kv.process_gc(op)
			default:
			}
			// check whether need to snapshot
			if kv.maxraftstate != -1 && 8*kv.maxraftstate-kv.persister.RaftStateSize() <= 500 {
				Debug(dKVSnapshot, "S%d GID:%d SnapShot!, cur_data: %v", kv.me, kv.gid, kv.allData)
				snapshot := kv.serilizeState()
				kv.rf.Snapshot(command.CommandIndex, snapshot)
			}
		} else if command.SnapshotValid {
			// update self state
			kv.DeSerilizeState(command.Snapshot)
		} else {
			Debug(dError, "S%d GID:%d Applier Receive Unkonwn Command!, %v", kv.me, kv.gid, command)
		}
	}
}

// check whether need to process
func (kv *ShardKV) check_dup(op Op) (bool, OpResult) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shardId := key2shard(op.Key)
	if client_last_process_seq, ok := kv.allData[kv.cur_config.Num][shardId].Client_to_last_req[op.ClientId]; ok {
		if op.ClientSeq <= client_last_process_seq {
			return false, kv.allData[kv.cur_config.Num][shardId].Client_to_last_res[op.ClientId]
		}
	}
	return true, OpResult{}
}

func (kv *ShardKV) update(op Op, res OpResult) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shardId := key2shard(op.Key)
	kv.allData[kv.cur_config.Num][shardId].Client_to_last_req[op.ClientId] = op.ClientSeq
	kv.allData[kv.cur_config.Num][shardId].Client_to_last_res[op.ClientId] = res
}

func (kv *ShardKV) replyChan(server_seq int64, res OpResult) {
	kv.mu.Lock()
	send_chan, ok := kv.chans[server_seq]
	kv.mu.Unlock()
	if ok {
		send_chan <- res
	}
}
