package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"sync"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu sync.Mutex
	leader	int
	clientId 	int64
	requestId	int
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
	ck.leader = 0
	ck.requestId = 0
	ck.clientId = nrand()

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", args, &reply)
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := GetArgs {Key: key, ClientId: ck.clientId, RequestId: ck.requestId}
	ck.requestId++
	//ck.mu.Unlock()
	//DPrintf("[%d] send GET with [%d]", ck.clientId, args.RequestId)
	//!!! dead loop
	ret := ""
	for i := ck.leader; true; i = (i + 1) % len(ck.servers) {
		reply := GetReply{}
		server := ck.servers[i]
		if server.Call("RaftKV.Get", &args, &reply) {
			if !reply.WrongLeader {
				ck.leader = i
				if reply.Err == OK {
					//DPrintf("[%d] Receive GET reply with [%d]", ck.clientId, args.RequestId)
					ret = reply.Value
					break
				} else {
					ret = ""
					break
				}
			}
		}
	}
	return ret
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", args, &reply)
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := PutAppendArgs {Key: key, Value: value, Op: op, ClientId: ck.clientId, RequestId: ck.requestId}
	ck.requestId++
	//ck.mu.Unlock()
	//DPrintf("[%d] send PUTAPPEND with [%d]", ck.clientId, args.RequestId)
	//!!! dead loop
	for i := ck.leader; true; i = (i + 1) % len(ck.servers) {
		//!!! Attention: where to declare a variable
		reply := PutAppendReply{}
		server := ck.servers[i]
		if server.Call("RaftKV.PutAppend", &args, &reply) {
			if !reply.WrongLeader {
				//DPrintf("[%d] Receive PUTAPPEND reply with [%d]", ck.clientId, args.RequestId)
				ck.leader = i
				return
			}
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
