package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
	"bytes"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType	string
	Args	interface{}
}

type Result struct {
	opType	string
	args  interface{}
	reply interface{}
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	database map[string]string		// for storing data
	ack 	map[int64]int			// for recording requestId of every clients
	messages map[int]chan Result	// for transferring result according to request
	persister *raft.Persister
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	index, _, isLeader := kv.rf.Start(Op{OpType: "Get", Args: *args})
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	kv.mu.Lock()
	if _, ok := kv.messages[index]; !ok {
		kv.messages[index] = make(chan Result, 1)

	}
	chanMsg := kv.messages[index]
	kv.mu.Unlock()

	select {
	case msg := <- chanMsg:
		if recArgs, ok := msg.args.(GetArgs); !ok {
			reply.WrongLeader = true
		} else {
			if args.ClientId != recArgs.ClientId || args.RequestId != recArgs.RequestId {
				reply.WrongLeader = true
			} else {
				*reply = msg.reply.(GetReply)
				reply.WrongLeader = false
			}
		}
	case <- time.After(time.Second * 1):
		reply.WrongLeader = true
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//DPrintf("PutAppend(): key[%s], value[%s]", args.Key, args.Value)
	index, _, isLeader := kv.rf.Start(Op{OpType: "PutAppend", Args: *args})
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	kv.mu.Lock()
	if _, ok := kv.messages[index]; !ok {
		kv.messages[index] = make(chan Result, 1)

	}
	chanMsg := kv.messages[index]
	kv.mu.Unlock()

	select {
	case msg := <- chanMsg:
		if tmpArgs, ok := msg.args.(PutAppendArgs); !ok {
			reply.WrongLeader = true
		} else {
			if args.ClientId != tmpArgs.ClientId || args.RequestId != tmpArgs.RequestId {
				reply.WrongLeader = true
			} else {
				reply.Err = msg.reply.(PutAppendReply).Err
				reply.WrongLeader = false
			}
		}
	case <- time.After(time.Second * 1):
		reply.WrongLeader = true
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
func  StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	//!!! Register error!
	gob.Register(PutAppendArgs{})
	gob.Register(GetArgs{})
	gob.Register(PutAppendReply{})
	gob.Register(GetReply{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.database = make(map[string]string)
	kv.ack = make(map[int64]int)
	kv.messages = make(map[int]chan Result)

	//!!! be careful of channel with 0 capacity
	go kv.Update()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	return kv
}

// ! added by Yang
// receive command form raft to update database
func (kv *RaftKV) Update() {
	for true {
		msg := <- kv.applyCh
		if msg.UseSnapshot {
			kv.UseSnapShot(msg.Snapshot)
		} else {
			request := msg.Command.(Op)
			//!!! value and type is a type of variable
			var result Result
			var clientId int64
			var requestId int
			if request.OpType == "Get" {
				args := request.Args.(GetArgs)
				clientId = args.ClientId
				requestId = args.RequestId
				result.args = args
			} else {
				args := request.Args.(PutAppendArgs)
				clientId = args.ClientId
				requestId = args.RequestId
				result.args = args
			}
			result.opType = request.OpType
			//!!! even duplicated, the request have to be sent a reply
			result.reply = kv.Apply(request, kv.IsDuplicated(clientId, requestId))
			kv.SendResult(msg.Index, result)
			kv.CheckSnapshot(msg.Index)
		}
	}
}

func (kv *RaftKV) UseSnapShot(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var LastIncludedIndex int
	var LastIncludedTerm int
	kv.database = make(map[string]string)
	kv.ack = make(map[int64]int)

	r := bytes.NewBuffer(snapshot)
	d := gob.NewDecoder(r)
	d.Decode(&LastIncludedIndex)
	d.Decode(&LastIncludedTerm)
	d.Decode(&kv.database)
	d.Decode(&kv.ack)
}

func (kv * RaftKV) CheckSnapshot(index int) {
	if kv.maxraftstate != -1 && float64(kv.rf.GetPersistSize()) > float64(kv.maxraftstate)*0.8 {
		w := new(bytes.Buffer)
		e := gob.NewEncoder(w)
		e.Encode(kv.database)
		e.Encode(kv.ack)
		data := w.Bytes()
		go kv.rf.StartSnapshot(data, index)
	}
}

func (kv *RaftKV) SendResult(index int, result Result) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.messages[index]; !ok {
		kv.messages[index] = make(chan Result, 1)
	} else {
		select {
		case <- kv.messages[index]:
		default:
		}
	}
	kv.messages[index] <- result
}


func (kv *RaftKV) Apply(request Op, isDuplicated bool) interface{} {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch request.Args.(type) {
	case GetArgs:
		var reply GetReply
		args := request.Args.(GetArgs)
		if value, ok := kv.database[args.Key]; ok {
			reply.Err = OK
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
		}
		//DPrintf("[%d] Apply get request: [%d]", kv.me, args.RequestId)
		return reply
	case PutAppendArgs:
		var reply PutAppendReply
		args := request.Args.(PutAppendArgs)
		//!!! attention of the operated variable
		if !isDuplicated {
			if args.Op == "Put" {
				kv.database[args.Key] = args.Value
			} else {
				kv.database[args.Key] += args.Value
			}
		}
		reply.Err = OK
		//DPrintf("[%d] Apply putappend request: [%d]", kv.me, args.RequestId)
		return reply
	}
	return nil
}

func (kv *RaftKV) IsDuplicated(clientId int64, requestId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if value, ok := kv.ack[clientId]; ok && value >= requestId {
		return true
	}
	kv.ack[clientId] = requestId
	return false
}