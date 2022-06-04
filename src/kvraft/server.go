package kvraft

import (
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
	TIMEOUT = 1000 // if the raft donot response the request in 1000 ms, we think it is
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  string
	Key   string
	Value string
}

type OpResult struct {
	err   Err
	value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data map[string]string

	chans map[int]chan OpResult
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DebugReceiveGet(kv, args)
	defer DebugReplyGet(kv, args, reply)
	op := Op{
		Type: GET,
		Key:  args.Key,
	}
	index, _, ok := kv.rf.Start(op)
	if ok {
		rec_chan := make(chan OpResult, 1)
		kv.mu.Lock()
		kv.chans[index] = rec_chan
		kv.mu.Unlock()
		timer := time.After(TIMEOUT * time.Millisecond)
		select {
		case <-timer:
			// timeout!
			reply.Err = "TIMEOUT"
		case res := <-rec_chan:
			// this op has be processed!
			reply.Err = res.err
			reply.Value = res.value
		}
		kv.mu.Lock()
		delete(kv.chans, index)
		kv.mu.Unlock()
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DebugReceivePutAppend(kv, args)
	defer DebugReplyPutAppend(kv, args, reply)
	op := Op{
		Type:  args.Op,
		Key:   args.Key,
		Value: args.Value,
	}
	index, _, ok := kv.rf.Start(op)

	if ok {
		rec_chan := make(chan OpResult, 1)
		kv.mu.Lock()
		kv.chans[index] = rec_chan
		kv.mu.Unlock()
		timer := time.After(TIMEOUT * time.Millisecond)
		select {
		case <-timer:
			reply.Err = "TIMEOUT"
		case res := <-rec_chan:
			reply.Err = res.err
		}
		kv.mu.Lock()
		delete(kv.chans, index)
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

func (kv *KVServer) replyChan(idx int, res OpResult) {
	kv.mu.Lock()
	send_chan, ok := kv.chans[idx]
	kv.mu.Unlock()
	if ok {
		send_chan <- res
	}
}

func (kv *KVServer) process() {
	for command := range kv.applyCh {
		if command.CommandValid {
			op := command.Command.(Op)
			switch op.Type {
			case GET:
				val, ok := kv.data[op.Key]
				res := OpResult{}
				if ok {
					res.value = val
					res.err = OK
				} else {
					res.err = ErrNoKey
				}
				kv.replyChan(command.CommandIndex, res)
			case PUT:
				kv.data[op.Key] = op.Value
				res := OpResult{
					err: OK,
				}
				kv.replyChan(command.CommandIndex, res)
			case APPEND:
				origin_val, ok := kv.data[op.Key]
				if ok {
					kv.data[op.Key] = origin_val + op.Value
				} else {
					kv.data[op.Key] = op.Value
				}
				res := OpResult{
					err: OK,
				}
				kv.replyChan(command.CommandIndex, res)
			default:
			}
		} else if command.SnapshotValid {

		} else {

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
	kv.chans = make(map[int]chan OpResult)

	// You may need initialization code here.

	go kv.process()

	return kv
}
