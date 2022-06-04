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

	// You will have to modify this function.
	args := GetArgs{
		Key: key,
	}
	cur_try_server := ck.last_leader
	for {
		time.Sleep(100 * time.Millisecond)
		cur_try_server = (cur_try_server + 1) % len(ck.servers)
		reply := GetReply{}
		ck.servers[cur_try_server].Call("KVServer.Get", &args, &reply)
		if reply.Err == OK {
			ck.last_leader = cur_try_server
			return reply.Value
		} else if reply.Err == ErrNoKey {
			ck.last_leader = cur_try_server
			return ""
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
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
	}
	cur_try_server := ck.last_leader
	for {
		time.Sleep(100 * time.Millisecond)
		reply := PutAppendReply{}
		cur_try_server = (cur_try_server + 1) % len(ck.servers)
		ck.servers[cur_try_server].Call("KVServer.PutAppend", &args, &reply)
		if reply.Err == OK {
			ck.last_leader = cur_try_server
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
