package shardkv

import "time"

// 周期性根据当前状态检查是否需要从其他group拉取shard
func (kv *ShardKV) CheckAcqShard() {

	for {
		time.Sleep(100 * time.Millisecond)
		kv.mu.Lock()
		for i, sd := range kv.allData[kv.cur_config.Num] {
			if sd.Status == ACQUING {
				// send rpc to get the shard
				go kv.SendAcqShard(i, kv.last_config.Groups[kv.last_config.Shards[i]], kv.last_config.Num)
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) SendAcqShard(shardId int, servers []string, cfgNum int) {
	for {
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			args := GetShardArgs{
				ShardId:   shardId,
				ConfigNum: cfgNum,
			}
			reply := GetShardReply{}
			ok := srv.Call("ShardKV.AcquireShard", &args, &reply)
			if ok && reply.Error == OK {
				// chek the state first
				kv.mu.Lock()
				// need the configNum match and the status is acquiring
				need_add := cfgNum == kv.last_config.Num && kv.allData[kv.cur_config.Num][shardId].Status == ACQUING
				kv.mu.Unlock()

				if need_add {
					op := Op{
						Type:           CHANGE_SHARD,
						NewShardCfgNum: cfgNum + 1,
						NewShardId:     shardId,
						NewShard:       reply.Shard,
					}
					kv.rf.Start(op)
				}
				Debug(dKVShard, "S%d GID:%d Get Shard from %s. req: %v, reply: %v", kv.me, kv.gid, servers[si], args, reply)
				return
			} else {
				continue
			}
		}
	}
}

func (kv *ShardKV) AcquireShard(args *GetShardArgs, reply *GetShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// check parameters
	if kv.cur_config.Num <= args.ConfigNum {
		reply.Error = ErrWrongLeader
		return
	}
	// need to check, because the data may be gced
	// when it is been gced, means this data has been received by other servers.
	// so this is a out ot date gc, reply ok is fine.
	if kv.allData[args.ConfigNum][args.ShardId].Status == EXPIRED {
		reply.Shard = kv.allData[args.ConfigNum][args.ShardId].Copy()
	}
	reply.Error = OK
}
