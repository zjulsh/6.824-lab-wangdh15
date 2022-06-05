package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	last_leader int
	id          int64
	seq         uint64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.last_leader = 0
	ck.id = nrand()
	ck.seq = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	Debug(dCLPutAppend, "C%d Begin Send Get. Key: %s.", ck.id, key)
	cur_req_seq := ck.seq
	ck.seq++
	cur_try_server := ck.last_leader
	for {
		// time.Sleep(100 * time.Millisecond)
		ch := make(chan GetReply, 1)
		// need to start a goroutine, because the network is not reliable
		go func(ch chan GetReply, cur_try_server int, clientId int64, clientSeq uint64) {
			args := GetArgs{
				Key:       key,
				ClientId:  clientId,
				ClientSeq: clientSeq,
			}
			reply := GetReply{}
			Debug(dCLPutAppend, "C%d Send Get to S%d. Arg: %v", ck.id, cur_try_server, args)
			ck.servers[cur_try_server].Call("KVServer.Get", &args, &reply)
			Debug(dCLPutAppend, "C%d Get Get Reply From S%d. Arg: %v, Reply: %v", ck.id, cur_try_server, args, reply)
			ch <- reply
		}(ch, cur_try_server, ck.id, cur_req_seq)
		// need to become a para
		time_out := time.After(100 * time.Millisecond)
		select {
		case reply := <-ch:
			Debug(dCLPutAppend, "C%d Get Server Reply! Key: %s, Rey: %v", ck.id, key, reply)
			if reply.Err == OK {
				ck.last_leader = cur_try_server
				return reply.Value
			} else if reply.Err == ErrNoKey {
				ck.last_leader = cur_try_server
				return ""
			}
			// try the next server
			cur_try_server = (cur_try_server + 1) % len(ck.servers)
		case <-time_out:
			cur_try_server = (cur_try_server + 1) % len(ck.servers)
			Debug(dCLPutAppend, "C%d Send Get Timeout! Key: %s", ck.id, key)
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	Debug(dCLPutAppend, "C%d Begin Send PutAppend. Key: %s, Val: %s, Op: %s.", ck.id, key, value, op)
	cur_req_seq := ck.seq
	ck.seq++
	cur_try_server := ck.last_leader
	for {
		// time.Sleep(100 * time.Millisecond)
		ch := make(chan PutAppendReply, 1)
		// need to start a goroutine, because the network is not reliable
		go func(ch chan PutAppendReply, cur_try_server int, clientId int64, clientSeq uint64) {
			args := PutAppendArgs{
				Key:       key,
				ClientId:  clientId,
				ClientSeq: clientSeq,
				Op:        op,
				Value:     value,
			}
			reply := PutAppendReply{}
			Debug(dCLPutAppend, "C%d Send PutAppend to S%d. Arg: %v", ck.id, cur_try_server, args)
			ck.servers[cur_try_server].Call("KVServer.PutAppend", &args, &reply)
			Debug(dCLPutAppend, "C%d Receive PutAppend Reply From S%d. Arg: %v, Reply: %v", ck.id, cur_try_server, args, reply)
			ch <- reply
		}(ch, cur_try_server, ck.id, cur_req_seq)
		// need to become a para
		time_out := time.After(100 * time.Millisecond)
		select {
		case reply := <-ch:
			if reply.Err == OK {
				ck.last_leader = cur_try_server
				return
			} else {
				cur_try_server = (cur_try_server + 1) % len(ck.servers)
			}
		case <-time_out:
			cur_try_server = (cur_try_server + 1) % len(ck.servers)
			Debug(dCLPutAppend, "C%d Send PutAppend Timeout! Key: %s, Val: %s, Op: %s", ck.id, key, value, op)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
