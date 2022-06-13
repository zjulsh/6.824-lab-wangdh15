package shardkv

import "6.824/shardctrler"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	ClientSeq uint64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId  int64
	ClientSeq uint64
}

type GetReply struct {
	Err   Err
	Value string
}

type GetShardArgs struct {
	ShardId   int
	ConfigNum int
}

type GetShardReply struct {
	Shard Shard
	Error Err
}

type Status string

const (
	WORKING Status = "woring"
	ACQUING Status = "acquring"
	EXPIRED Status = "expired"
	INVALID Status = "invalid"
)

const (
	GET           = "Get"
	PUT           = "Put"
	APPEND        = "Append"
	PUTAPP        = "PUTAPPEND"
	CHANGE_CONFIG = "CHANGE_CONFIG"
	GET_NEW_SHARD = "GET_NEW_SHARD"
	CHANGE_SHARD  = "CHANGE_SHARD"
)

type Shard struct {
	Data               map[string]string
	Client_to_last_req map[int64]uint64
	Client_to_last_res map[int64]OpResult
	Status             Status
}

func (sd *Shard) Copy() Shard {
	res := Shard{
		Data:               make(map[string]string),
		Client_to_last_req: make(map[int64]uint64),
		Client_to_last_res: make(map[int64]OpResult),
	}
	for k, v := range sd.Data {
		res.Data[k] = v
	}
	for k, v := range sd.Client_to_last_req {
		res.Client_to_last_req[k] = v
	}
	for k, v := range sd.Client_to_last_res {
		res.Client_to_last_res[k] = v
	}
	res.Status = sd.Status
	return res
}

type CfgiData [shardctrler.NShards]Shard // store the shard for on config num

func (cfgidata *CfgiData) Copy() CfgiData {
	res := CfgiData{}
	for k, v := range *cfgidata {
		res[k] = v.Copy()
	}
	return res
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type string

	Key   string
	Value string

	NewConfig shardctrler.Config

	NewShardCfgNum int
	NewShardId     int
	NewShard       Shard

	ClientId  int64
	ClientSeq uint64
	ServerSeq int64
}

type OpResult struct {
	Error Err
	Value string
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
