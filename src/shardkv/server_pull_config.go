package shardkv

import "time"

// pull new config from master periodically
func (kv *ShardKV) PullConfig() {
	for {

		time.Sleep(80 * time.Millisecond)
		// check all shard is working or expired
		kv.mu.Lock()
		if !kv.CheckStatus() {
			kv.mu.Unlock()
			continue
		}
		cur_config_num := kv.cur_config.Num + 1
		kv.mu.Unlock()
		new_config := kv.mck.Query(cur_config_num)
		kv.mu.Lock()
		has_new_config := new_config.Num > kv.cur_config.Num
		kv.mu.Unlock()
		if has_new_config {
			op := Op{
				Type:      CHANGE_CONFIG,
				NewConfig: new_config,
			}
			Debug(dKVConfig, "S%d GID:%d Send %v to Raft", kv.me, kv.gid, op)
			kv.rf.Start(op)
		}
	}
}

// check whethe change from last_config to cur_config is all working
func (kv *ShardKV) CheckStatus() bool {
	for _, v := range kv.allData[kv.cur_config.Num] {
		if v.Status == ACQUING {
			return false
		}
	}
	return true
}

func (kv *ShardKV) ResetStatus() {
	kv.allData = append(kv.allData, CfgiData{})
	for i := 0; i < len(kv.cur_config.Shards); i++ {
		if kv.last_config.Shards[i] != kv.gid && kv.cur_config.Shards[i] == kv.gid {
			kv.allData[kv.cur_config.Num][i] = Shard{}
			kv.allData[kv.cur_config.Num][i].Data = make(map[string]string)
			kv.allData[kv.cur_config.Num][i].Client_to_last_req = make(map[int64]uint64)
			kv.allData[kv.cur_config.Num][i].Client_to_last_res = make(map[int64]OpResult)
			if kv.last_config.Shards[i] != 0 {
				// the data is in other group
				kv.allData[kv.cur_config.Num][i].Status = ACQUING
			} else {
				kv.allData[kv.cur_config.Num][i].Status = WORKING
			}
		} else if kv.last_config.Shards[i] == kv.gid && kv.cur_config.Shards[i] != kv.gid {
			kv.allData[kv.last_config.Num][i].Status = EXPIRED
			kv.allData[kv.cur_config.Num][i].Status = INVALID
		} else if kv.last_config.Shards[i] == kv.gid && kv.cur_config.Shards[i] == kv.gid {
			kv.allData[kv.cur_config.Num][i] = kv.allData[kv.last_config.Num][i].Copy()
			kv.allData[kv.last_config.Num][i].Client_to_last_req = nil
			kv.allData[kv.last_config.Num][i].Client_to_last_res = nil
			kv.allData[kv.last_config.Num][i].Data = nil
			kv.allData[kv.last_config.Num][i].Status = INVALID
		} else {
			kv.allData[kv.cur_config.Num][i].Status = INVALID
		}
	}
}
