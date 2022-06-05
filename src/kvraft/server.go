package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
)

const (
	TIMEOUT = 100 // if the raft donot response the request in 1000 ms, we think it is
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      string
	Key       string
	Value     string
	ClientId  int64
	ClientSeq uint64
	ServerId  int
	ServerSeq int64
}

type OpResult struct {
	Error Err
	Value string
}

type KVServer struct {
	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg
	dead      int32 // set by Kill()
	persister *raft.Persister

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data map[string]string

	chans map[int64]chan OpResult
	// 这两个数据在重启之后Reply会被重建，所以不需要持久化存储
	// 但是如果加入了快照，则这两个需要被持久化
	client_to_last_process_seq    map[int64]uint64
	client_to_last_process_result map[int64]OpResult
}

func (kv *KVServer) serilizeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	kv.mu.Lock()
	e.Encode(kv.client_to_last_process_seq)
	e.Encode(kv.client_to_last_process_result)
	kv.mu.Unlock()
	return w.Bytes()
}

func (kv *KVServer) DeSerilizeState(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var data map[string]string
	var client_to_last_process_seq map[int64]uint64
	var client_to_last_process_result map[int64]OpResult
	if d.Decode(&data) != nil ||
		d.Decode(&client_to_last_process_seq) != nil ||
		d.Decode(&client_to_last_process_result) != nil {
		Debug(dError, "S%d KVServer Read Persist Error!", kv.me)
	} else {
		kv.data = data
		kv.mu.Lock()
		kv.client_to_last_process_seq = client_to_last_process_seq
		kv.client_to_last_process_result = client_to_last_process_result
		kv.mu.Unlock()
		Debug(dPersist, "S%d KVServer ReadPersist. Data: %v, Seq: %v, Res: %v", kv.me,
			kv.data, kv.client_to_last_process_seq, kv.client_to_last_process_result)
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DebugReceiveGet(kv, args)
	defer DebugReplyGet(kv, args, reply)
	kv.mu.Lock()
	last_process_seq, ok := kv.client_to_last_process_seq[args.ClientId]
	if ok {
		if last_process_seq == args.ClientSeq {
			reply.Err = kv.client_to_last_process_result[args.ClientId].Error
			reply.Value = kv.client_to_last_process_result[args.ClientId].Value
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
		ServerId:  kv.me,
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

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DebugReceivePutAppend(kv, args)
	defer DebugReplyPutAppend(kv, args, reply)
	kv.mu.Lock()
	last_process_seq, ok := kv.client_to_last_process_seq[args.ClientId]
	if ok {
		if last_process_seq == args.ClientSeq {
			reply.Err = kv.client_to_last_process_result[args.ClientId].Error
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
		ServerId:  kv.me,
		ServerSeq: nrand(),
	}
	if _, _, ok1 := kv.rf.Start(op); ok1 {
		rec_chan := make(chan OpResult, 1)
		kv.mu.Lock()
		kv.chans[op.ServerSeq] = rec_chan
		kv.mu.Unlock()
		timer := time.After(TIMEOUT * time.Millisecond)
		select {
		case <-timer:
			// timeout!
			reply.Err = "TIMEOUT"
		case res := <-rec_chan:
			// this op has be processed!
			reply.Err = res.Error
		}
		kv.mu.Lock()
		delete(kv.chans, op.ServerSeq)
		kv.mu.Unlock()
	} else {
		reply.Err = ErrWrongLeader
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) replyChan(server_seq int64, res OpResult) {
	kv.mu.Lock()
	send_chan, ok := kv.chans[server_seq]
	kv.mu.Unlock()
	if ok {
		send_chan <- res
	}
}

// check whether need to process
func (kv *KVServer) check_dup(op Op) (bool, OpResult) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	client_last_process_seq, ok := kv.client_to_last_process_seq[op.ClientId]
	if ok {
		if op.ClientSeq <= client_last_process_seq {
			// need not to process
			return false, kv.client_to_last_process_result[op.ClientId]
		}
	}
	// need to process
	return true, OpResult{}
}

func (kv *KVServer) update(op Op, res OpResult) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.client_to_last_process_seq[op.ClientId] = op.ClientSeq
	kv.client_to_last_process_result[op.ClientId] = res
}

func (kv *KVServer) process() {
	for command := range kv.applyCh {
		if command.CommandValid {
			op := command.Command.(Op)
			need_process, res := kv.check_dup(op)
			if !need_process {
				kv.replyChan(op.ServerSeq, res)
				continue
			}
			switch op.Type {
			case GET:
				val, ok := kv.data[op.Key]
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
				kv.data[op.Key] = op.Value
				res := OpResult{
					Error: OK,
				}
				kv.update(op, res)
				kv.replyChan(op.ServerSeq, res)
			case APPEND:
				origin_val, ok := kv.data[op.Key]
				if ok {
					kv.data[op.Key] = origin_val + op.Value
				} else {
					kv.data[op.Key] = op.Value
				}
				res := OpResult{
					Error: OK,
				}
				kv.update(op, res)
				kv.replyChan(op.ServerSeq, res)
			default:
				Debug(dError, "S%d KVServer Process Unknown OP: %v", kv.me, op)
			}
			// check whether need to snapshot
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= 6*kv.maxraftstate {
				snapshot := kv.serilizeState()
				go kv.rf.Snapshot(command.CommandIndex, snapshot)
				Debug(dSnap, "S%d KVServer Create Snapshot! IDX:%d, Snapshot:%v", kv.me, command.CommandIndex, snapshot)
			}
		} else if command.SnapshotValid {
			// update self state
			kv.DeSerilizeState(command.Snapshot)
		} else {
			Debug(dError, "S%d KVServer Process Unknown Command: %v", kv.me, command)
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string)
	kv.chans = make(map[int64]chan OpResult)
	kv.client_to_last_process_seq = make(map[int64]uint64)
	kv.client_to_last_process_result = make(map[int64]OpResult)
	snapshot := persister.ReadSnapshot()
	kv.DeSerilizeState(snapshot)
	kv.persister = persister

	// You may need initialization code here.

	go kv.process()

	return kv
}
