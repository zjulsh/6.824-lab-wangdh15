package shardkv

import (
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

type OpResult struct {
	Error Err
	Value string
}

const (
	WORKING = "woring"
	ACQUING = "acquing"
	SENDING = "sending"
	EXPIRED = "expired"
)

const (
	GET           = "GET"
	PUT           = "PUT"
	APPEND        = "APPEND"
	PUTAPP        = "PUTAPPEND"
	CHANGE_CONFIG = "CHANGE_CONFIG"
	GET_NEW_SHARD = "GET_NEW_SHARD"
)

const (
	TIMEOUT = 100 // 100 millseconds
)

type Shard struct {
	Data               map[string]string
	Client_to_last_req map[int64]uint64
	Client_to_last_res map[int64]OpResult
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type string

	Key   string
	Value string

	NewConfig shardctrler.Config

	ClientId  int64
	ClientSeq uint64
	ServerSeq int64
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mck         *shardctrler.Clerk
	shards      map[int]Shard
	shardStatus map[int]string
	chans       map[int64]chan OpResult
	persister   *raft.Persister
	last_config shardctrler.Config
	cur_config  shardctrler.Config
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	shardId := key2shard(args.Key)
	kv.mu.Lock()
	if status, ok := kv.shardStatus[shardId]; ok {
		if status != WORKING {
			reply.Err = ErrWrongGroup
			kv.mu.Unlock()
			return
		}
	} else {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	// the sharding status is working
	last_process_seq, ok := kv.shards[shardId].Client_to_last_req[args.ClientId]
	if ok {
		if last_process_seq == args.ClientSeq {
			reply.Err = kv.shards[shardId].Client_to_last_res[args.ClientId].Error
			reply.Value = kv.shards[shardId].Client_to_last_res[args.ClientId].Value
			kv.mu.Unlock()
			return
		} else if last_process_seq > args.ClientSeq {
			// this is a out of date request, return immediately
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
		timer := time.After(TIMEOUT * time.Millisecond)
		select {
		case <-timer:
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
	shardId := key2shard(args.Key)
	kv.mu.Lock()
	if status, ok := kv.shardStatus[shardId]; ok {
		if status != WORKING {
			reply.Err = ErrWrongGroup
			kv.mu.Unlock()
			return
		}
	} else {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	// the sharding status is working
	last_process_seq, ok := kv.shards[shardId].Client_to_last_req[args.ClientId]
	if ok {
		if last_process_seq == args.ClientSeq {
			reply.Err = kv.shards[shardId].Client_to_last_res[args.ClientId].Error
			kv.mu.Unlock()
			return
		} else if last_process_seq > args.ClientSeq {
			// this is a out of date request, return immediately
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
		timer := time.After(TIMEOUT * time.Millisecond)
		select {
		case <-timer:
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

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) replyChan(server_seq int64, res OpResult) {
	kv.mu.Lock()
	send_chan, ok := kv.chans[server_seq]
	kv.mu.Unlock()
	if ok {
		send_chan <- res
	}
}

// func (kv *ShardKV) serilizeState() []byte {
// 	// w := new(bytes.Buffer)
// 	// e := labgob.NewEncoder(w)
// 	// e.Encode(kv.data)
// 	// kv.mu.Lock()
// 	// e.Encode(kv.client_to_last_process_seq)
// 	// e.Encode(kv.client_to_last_process_result)
// 	// kv.mu.Unlock()
// 	// return w.Bytes()

// }

func (kv *ShardKV) DeSerilizeState(snapshot []byte) {
	// if len(snapshot) == 0 {
	// 	return
	// }
	// r := bytes.NewBuffer(snapshot)
	// d := labgob.NewDecoder(r)
	// var data map[string]string
	// var client_to_last_process_seq map[int64]uint64
	// var client_to_last_process_result map[int64]OpResult
	// if d.Decode(&data) != nil ||
	// 	d.Decode(&client_to_last_process_seq) != nil ||
	// 	d.Decode(&client_to_last_process_result) != nil {
	// 	Debug(dError, "S%d KVServer Read Persist Error!", kv.me)
	// } else {
	// 	kv.data = data
	// 	kv.mu.Lock()
	// 	kv.client_to_last_process_seq = client_to_last_process_seq
	// 	kv.client_to_last_process_result = client_to_last_process_result
	// 	kv.mu.Unlock()
	// 	Debug(dPersist, "S%d KVServer ReadPersist. Data: %v, Seq: %v, Res: %v", kv.me,
	// 		kv.data, kv.client_to_last_process_seq, kv.client_to_last_process_result)
	// }
}

// check whether need to process
func (kv *ShardKV) check_dup(op Op) (bool, OpResult) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shardId := key2shard(op.Key)
	client_last_process_seq, ok := kv.shards[shardId].Client_to_last_req[op.ClientId]
	if ok {
		if op.ClientSeq <= client_last_process_seq {
			return false, kv.shards[shardId].Client_to_last_res[op.ClientId]
		}
	}
	return true, OpResult{}
}

func (kv *ShardKV) update(op Op, res OpResult) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shardId := key2shard(op.Key)
	kv.shards[shardId].Client_to_last_req[op.ClientId] = op.ClientSeq
	kv.shards[shardId].Client_to_last_res[op.ClientId] = res
}

func (kv *ShardKV) process() {
	for command := range kv.applyCh {
		if command.CommandValid {
			op := command.Command.(Op)
			need_process, res := kv.check_dup(op)
			if !need_process {
				kv.replyChan(op.ServerSeq, res)
				continue
			}
			shardId := key2shard(op.Key)
			switch op.Type {
			case GET:
				val, ok := kv.shards[shardId].Data[op.Key]
				res := OpResult{}
				if ok {
					res.Value = val
					res.Error = OK
				} else {
					res.Error = ErrNoKey
				}
				kv.update(op, res)
				kv.replyChan(op.ServerSeq, res)
			case PUT:
				kv.shards[shardId].Data[op.Key] = op.Value
				res := OpResult{
					Error: OK,
				}
				kv.update(op, res)
				kv.replyChan(op.ServerSeq, res)
			case APPEND:
				origin_val, ok := kv.shards[shardId].Data[op.Key]
				if ok {
					kv.shards[shardId].Data[op.Key] = origin_val + op.Value
				} else {
					kv.shards[shardId].Data[op.Key] = op.Value
				}
				res := OpResult{
					Error: OK,
				}
				kv.update(op, res)
				kv.replyChan(op.ServerSeq, res)
			case CHANGE_CONFIG:
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
				}
				kv.mu.Unlock()
			default:

			}
			// check whether need to snapshot
			// if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= 6*kv.maxraftstate {
			// snapshot := kv.serilizeState()
			// go kv.rf.Snapshot(command.CommandIndex, snapshot)
			// }
		} else if command.SnapshotValid {
			// update self state
			// kv.DeSerilizeState(command.Snapshot)
		}
	}
}

// check whethe change from last_config to cur_config is all done
func (kv *ShardKV) CheckStatus() bool {
	for i := 0; i < len(kv.cur_config.Shards); i++ {
		if kv.cur_config.Shards[i] != kv.gid {
			if status, ok := kv.shardStatus[i]; ok {
				if status != EXPIRED {
					return false
				}
			}
		} else {
			if status, ok := kv.shardStatus[i]; ok {
				if status != WORKING {
					return false
				}
			}
		}
	}
	return true
}

func (kv *ShardKV) ResetStatus() {
	for i := 0; i < len(kv.cur_config.Shards); i++ {
		if kv.last_config.Shards[i] != kv.gid && kv.cur_config.Shards[i] == kv.gid {
			if kv.last_config.Shards[i] != 0 {
				// the data is in other group
				kv.shardStatus[i] = ACQUING
			} else {
				kv.shardStatus[i] = WORKING
			}
		} else if kv.last_config.Shards[i] == kv.gid && kv.cur_config.Shards[i] != kv.gid {
			// the data is moved to other group
			kv.shardStatus[i] = SENDING
		}
	}
}

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
		new_config := kv.mck.Query(kv.cur_config.Num + 1)
		kv.mu.Lock()
		has_new_config := new_config.Num > kv.cur_config.Num
		kv.mu.Unlock()
		if has_new_config {
			op := Op{
				Type:      CHANGE_CONFIG,
				NewConfig: new_config,
			}
			kv.rf.Start(op)
		}
	}
}

// 周期性根据当前状态检查是否需要从其他group拉取shard
func (kv *ShardKV) CheckAcqShard() {

	for {
		time.Sleep(100 * time.Millisecond)
		kv.mu.Lock()
		for i, s := kv.shardStatus {
			if s == ACQUING {
				// send rpc to get the shard
				go func(servers []string) {

				}(kv.last_config.Groups[kv.shards[i]])
			}
		}

		kv.mu.Unlock()
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister
	kv.cur_config.Num = 0

	// 定期从Master拉取配置
	go kv.PullConfig()
	go kv.process()

	return kv
}
