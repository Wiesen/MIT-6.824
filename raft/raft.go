package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import (
	"labrpc"
	"time"
	"math/rand"
	"bytes"
	"encoding/gob"
	"os"
	"runtime/debug"
)

const RaftElectionTimeoutLow = 150 * time.Millisecond
const RaftElectionTimeoutHigh = 300 * time.Millisecond
const RaftHeartbeatPeriod = 100 * time.Millisecond

type Role int

const (
	Follower	Role 	= 0
	Candidate  			= 1
	Leader 				= 2
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//! A LogEntry object
type LogEntry struct {
	Command interface{}
	Term    int
	Index	int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu          sync.Mutex
	peers       []*labrpc.ClientEnd
	persister   *Persister
	me          int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent on all server
	currentTerm int
	votedFor    int
	logTable    []LogEntry

					// For transfer
	leaderId	int

	// Volatile state on all servers
	commitIndex int
	lastApplied int
	role		Role

	// Volatile state on leaders
	// Reinitialized after election
	nextIndex   []int
	matchIndex  []int

	// For communication
	chanRole		chan Role
	chanCommitted	chan ApplyMsg

	// For election timer
	chanHeartbeat	chan bool
	chanGrantVote	chan bool

	// For leader to control follower
	chanAppend      []chan bool
}

/*------------- Persistence function ---------------*/
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logTable)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logTable)
	//DPrintf("[%d] read persist [%d]", rf.me, rf.logTable[len(rf.logTable)-1].Index)
}

func (rf *Raft) GetPersistSize() int {
	return rf.persister.RaftStateSize()
}

/*------------- Lab 3: snapshot functionality  ---------------*/
type InstallSnapshotArgs struct {
	Term int	// leader’s term
	LeaderId int	//so follower can redirect clients
	LastIncludedIndex int	// the snapshot replaces all entries
	LastIncludedTerm int // term of lastIncludedIndex
	Data []byte // raw bytes of the snapshot chunk, starting at offset
	//Offset int	// byte offset where chunk is positioned
	//Done bool	// true if this is the last chunk
}

type InstallSnapshotReply struct {
	Term int	// currentTerm, for leader to update itself
}

func (rf *Raft) readSnapshot(data []byte) {
	if len(data) == 0 {
		return
	}
	var LastIncludedIndex int
	var LastIncludedTerm int
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&LastIncludedIndex)
	d.Decode(&LastIncludedTerm)
	rf.commitIndex = LastIncludedIndex
	rf.lastApplied = LastIncludedIndex
	rf.logTable = rf.truncateLog(LastIncludedIndex, LastIncludedTerm)
	msg := ApplyMsg{UseSnapshot: true, Snapshot: data}
	rf.chanCommitted <- msg
}

func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	rf.chanHeartbeat <- true

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = Follower
		rf.chanRole <- Follower
	}
	reply.Term = args.Term

	baseIndex := rf.logTable[0].Index
	if args.LastIncludedIndex < baseIndex {
		return
	}

	rf.logTable = rf.truncateLog(args.LastIncludedIndex, args.LastIncludedTerm)
	rf.persister.SaveSnapshot(args.Data)
	msg := ApplyMsg{UseSnapshot: true, Snapshot: args.Data}
	rf.chanCommitted <- msg
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
}

func (rf *Raft) sendInstallSnapshot(server int, args InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	var ret bool
	c := make(chan bool)
	go func() {
		ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
		c <- ok
	}()
	select {
	case ret = <- c:
	case <-time.After(RaftHeartbeatPeriod):
		ret = false
	}
	return ret
}

func (rf *Raft) StartSnapshot(snapshot []byte, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.logTable[0].Index || index > rf.logTable[len(rf.logTable)-1].Index {
		// in case having installed a snapshot from leader before snapshotting
		// second condition is a hack
		return
	}

	rf.logTable = rf.logTable[index-rf.logTable[0].Index:]
	rf.persist()

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.logTable[0].Index)
	e.Encode(rf.logTable[0].Term)
	data := w.Bytes()
	data = append(data, snapshot...)
	rf.persister.SaveSnapshot(data)
}

func (rf *Raft) truncateLog(index int, term int) []LogEntry {
	var newLogEntries []LogEntry
	newLogEntries = append(newLogEntries, LogEntry{Index: index, Term: term})
	//!!! Be careful of variable name overwrite
	for  i:= len(rf.logTable) - 1; i >= 0; i-- {
		if rf.logTable[i].Index == index && rf.logTable[i].Term == term {
			newLogEntries = append(newLogEntries, rf.logTable[i+1:]...)
			break
		}
	}
	return newLogEntries
}

/*------------- Lab 2: Raft Basic function  ---------------*/
//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term    int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term    int
	VoteGranted bool
}

//
// example AppendEntries RPC arguments structure.
//
type AppendEntriesArgs struct {
	// Your data here.
	Term    int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entry []LogEntry
	LeaderCommit int
}

//
// example AppendEntries RPC reply structure.
//
type AppendEntriesReply struct {
	// Your data here.
	Term    int
	Success bool
	// optimized
	NextIndex int

}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = Follower
		rf.chanRole <- Follower
	}

	reply.Term = args.Term

	//! denies its vote if its own log is more up-to-date than that of the candidate
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
	} else if rf.logTable[len(rf.logTable)-1].Term > args.LastLogTerm {	//! term
		reply.VoteGranted = false
	} else if rf.logTable[len(rf.logTable)-1].Index > args.LastLogIndex &&	//! index
			rf.logTable[len(rf.logTable)-1].Term == args.LastLogTerm {
		reply.VoteGranted = false
	} else {
		rf.chanGrantVote <- true
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should probably
// pass &reply.
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	var ret bool
	c := make(chan bool)
	go func() {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		c <- ok
	}()
	select {
	case ret = <- c:
	case <-time.After(RaftHeartbeatPeriod):
		ret = false
	}
	return ret
}

//
// example AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.chanHeartbeat <- true

	//! update current term and leader and but doesn't vote
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.leaderId = args.LeaderId
		rf.role = Follower
		rf.chanRole <- Follower
	}

	reply.Term = args.Term
	baseLogIndex := rf.logTable[0].Index

	if args.PrevLogIndex < baseLogIndex {
		reply.Success = false
		reply.NextIndex = baseLogIndex + 1
		//DPrintf("unmatch in server [%d]: AppendEntries: args.PrevLogIndex [%d] < baseLogIndex [%d]",
		//	rf.me, args.PrevLogIndex, baseLogIndex)
		return
	}

	if baseLogIndex + len(rf.logTable) <= args.PrevLogIndex {
		reply.Success = false
		reply.NextIndex = baseLogIndex + len(rf.logTable)
		//DPrintf("unmatch in server [%d]: AppendEntries: totallength [%d] < PrevLogIndex [%d]",
		//	rf.me, reply.NextIndex-1, args.PrevLogIndex)
		return
	}

	// optimized for backing quickly
	// include the term of the conflicting entry and the first index it stores for that term.
	if rf.logTable[args.PrevLogIndex - baseLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		nextIndex := args.PrevLogIndex
		failTerm := rf.logTable[nextIndex - baseLogIndex].Term
		//! the loop must execute one time when nextIndex > 0
		for nextIndex >= baseLogIndex && rf.logTable[nextIndex-baseLogIndex].Term == failTerm {
			nextIndex--
		}
		// reply.NextIndex == baseLogIndex when need snapshot
		reply.NextIndex = nextIndex + 1
		//DPrintf("unmatch in server [%d]: AppendEntries: backing to [%d]",
		//	rf.me, reply.NextIndex)
		return
	}

	var updateLogIndex int
	if len(args.Entry) != 0 {
		//!!! erase those conflicting with a new one all that following
		DPrintln(rf.me, "receive append:", args)
		for i, entry := range args.Entry {
			curIndex := entry.Index - baseLogIndex
			if curIndex < len(rf.logTable) {
				if curIndex < baseLogIndex || curIndex > rf.logTable[len(rf.logTable)-1].Index {
					//DPrintf("logTable: from [%d] to [%d] while PreLogIndex [%d] and entry index [%d]",
					//	baseLogIndex, rf.logTable[len(rf.logTable) - 1].Index, args.PrevLogIndex, entry.Index)
				}
				if entry.Term != rf.logTable[curIndex].Term {
					//DPrintf("[%d] in term [%d] replace entry [%d] [%d]",
					//	rf.me, rf.currentTerm, curIndex, rf.logTable[curIndex].Command)
					rf.logTable = append(rf.logTable[:curIndex], entry)
				}
			} else {
				//DPrintf("[%d] in term [%d] append entry [%d] [%d]",
				//	rf.me, rf.currentTerm, curIndex, rf.logTable[curIndex].Command)
				rf.logTable = append(rf.logTable, args.Entry[i:]...)
				break
			}
		}
		reply.NextIndex = rf.logTable[len(rf.logTable)-1].Index + 1
		updateLogIndex = reply.NextIndex - 1
	} else {
		reply.NextIndex = args.PrevLogIndex + 1
		updateLogIndex = args.PrevLogIndex
	}
	reply.Success = true
	//!!! the last new entry is update log index
	rf.updateFollowCommit(args.LeaderCommit, updateLogIndex)
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	var ret bool
	c := make(chan bool)
	go func() {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		c <- ok
	}()
	select {
	case ret = <- c:
	case <-time.After(RaftHeartbeatPeriod):
		ret = false
	}
	return ret
}

func (rf *Raft) doAppendEntries(server int) {
	//! make a lock for every follower to serialize
	select {
	case <- rf.chanAppend[server]:
	default: return
	}
	defer func() {rf.chanAppend[server] <- true}()

	for rf.role == Leader {
		rf.mu.Lock()
		baseLogIndex := rf.logTable[0].Index
		//DPrintf("[%d] append to [%d]: base [%d], length [%d] and next [%d]",
		//	rf.me, server, baseLogIndex, len(rf.logTable), rf.nextIndex[server])
		if rf.nextIndex[server] <= baseLogIndex {
			var args InstallSnapshotArgs
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.LastIncludedIndex = rf.logTable[0].Index
			args.LastIncludedTerm = rf.logTable[0].Term
			args.Data = rf.persister.snapshot
			rf.mu.Unlock()
			var reply InstallSnapshotReply
			if rf.sendInstallSnapshot(server, args, &reply) {
				if rf.role != Leader {
					return
				}
				if args.Term != rf.currentTerm || reply.Term > args.Term {
					if reply.Term > args.Term {
						rf.mu.Lock()
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.role = Follower
						rf.chanRole <- Follower
						rf.persist()
						rf.mu.Unlock()
					}
					return
				}
				rf.matchIndex[server] = baseLogIndex
				rf.nextIndex[server] = baseLogIndex + 1
				return
			}
		} else {
			// Basic argument
			var args AppendEntriesArgs
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[server] - 1
			if args.PrevLogIndex - baseLogIndex < 0 || args.PrevLogIndex - baseLogIndex >= len(rf.logTable) {
				DPrintln("base:", baseLogIndex, "largest:", rf.logTable[len(rf.logTable)-1].Index,
					"PrevLogIndex:", args.PrevLogIndex, "nextIndex:", rf.nextIndex[server])
				debug.PrintStack()
				os.Exit(-1)
			}
			args.PrevLogTerm = rf.logTable[args.PrevLogIndex - baseLogIndex].Term
			args.LeaderCommit = rf.commitIndex
			// Entries
			if rf.nextIndex[server] < baseLogIndex + len(rf.logTable) {
				//if again {
				//	again = false
				//	DPrintf("[%d] in term [%d] send entry to [%d]: [%d] [%d] to [%d] [%d]",
				//		rf.me, rf.currentTerm,
				//		server, rf.nextIndex[server], rf.logTable[rf.nextIndex[server]].Command,
				//		len(rf.logTable) - 1, rf.logTable[len(rf.logTable) - 1].Command)
				//}
				args.Entry = rf.logTable[rf.nextIndex[server] - baseLogIndex:]
			}
			rf.mu.Unlock()

			// Reply
			var reply AppendEntriesReply
			if rf.sendAppendEntries(server, args, &reply) {
				rf.DoAppendEntriesReply(server, args, reply)
				return
			} else {
				//DPrintf("doAppendEntries: no reply from follower [%d]", server)
			}
		}
	}
}

func (rf *Raft) DoAppendEntriesReply(server int, args AppendEntriesArgs, reply AppendEntriesReply) {
	//!!!after a long time: in case of role changing while appending
	if rf.role != Leader {
		return
	}
	if args.Term != rf.currentTerm || reply.Term > args.Term {
		if reply.Term > args.Term {
			rf.mu.Lock()
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.role = Follower
			rf.chanRole <- Follower
			rf.persist()
			rf.mu.Unlock()
		}
		return
	}
	//!!!after a long time: heartbeat RPC reply fail index as well
	if reply.Success {
		rf.matchIndex[server] = reply.NextIndex - 1
		rf.nextIndex[server] = reply.NextIndex
	} else {
		//! out of range
		////rf.matchIndex[server] = 0
		//if reply.NextIndex > rf.matchIndex[server] + 1 {
		//	rf.nextIndex[server] = reply.NextIndex
		//} else {
		//	rf.nextIndex[server] = rf.matchIndex[server] + 1
		//}
		rf.nextIndex[server] = reply.NextIndex
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index, term, isLeader := -1, -1, false
	if rf.role == Leader {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		defer rf.persist()
		//!!! lab3 Part B
		index = len(rf.logTable) + rf.logTable[0].Index
		term = rf.currentTerm
		isLeader = true
		entry := LogEntry{Command:command, Term:term, Index: index}
		rf.logTable = append(rf.logTable, entry)
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.leaderId = -1
	rf.role = Follower
	rf.logTable = append(rf.logTable, LogEntry{Term: 0, Index: 0})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers), len(peers))
	rf.matchIndex = make([]int, len(peers), len(peers))

	rf.chanHeartbeat = make(chan bool)
	rf.chanGrantVote = make(chan bool)

	rf.chanRole = make(chan Role)
	rf.chanCommitted = applyCh

	rf.chanAppend = make([]chan bool, len(peers))
	for i := range rf.chanAppend {
		rf.chanAppend[i] = make(chan bool, 1)
		rf.chanAppend[i] <- true
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())
	//! set seed at the beginning
	rand.Seed(time.Now().UnixNano())

	go rf.changeRole()
	go rf.startElectTimer()

	return rf
}

func (rf *Raft) changeRole() {
	role := rf.role
	for true {
		switch role {
		case Leader:
			//DPrintf("[%d] run as leader", rf.me)
			for i := range rf.peers {
				rf.nextIndex[i] = rf.logTable[len(rf.logTable)-1].Index + 1
				rf.matchIndex[i] = 0
			}
			go rf.doHeartbeat()
			role = <-rf.chanRole
		case Candidate:
			chanQuitElect := make(chan bool)
			go rf.startElection(chanQuitElect)
			role = <-rf.chanRole
			close(chanQuitElect)
		case Follower:
			role = <-rf.chanRole
		}
	}
}

// once an entry from the current term has been committed in this way,
// then all prior entries are committed indirectly because of the Log Matching Property.
func (rf *Raft) updateLeaderCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// update commitIndex
	oldIndex := rf.commitIndex
	newIndex := oldIndex
	for i := len(rf.logTable)-1; rf.logTable[i].Index>oldIndex && rf.logTable[i].Term==rf.currentTerm; i-- {
		countServer := 1
		for server := range rf.peers {
			if server != rf.me && rf.matchIndex[server] >= rf.logTable[i].Index {
				countServer++
			}
		}
		if countServer * 2 > len(rf.peers) {
			newIndex = rf.logTable[i].Index
			break
		}
	}
	if oldIndex == newIndex {
		return
	}

	rf.commitIndex = newIndex
	//! update the log added in previous term
	baseIndex := rf.logTable[0].Index
	for i := oldIndex + 1; i <= newIndex; i++ {
		DPrintln(rf.me, "commit entry", rf.logTable[i-baseIndex].Command)
		rf.chanCommitted <- ApplyMsg{Index:i, Command:rf.logTable[i-baseIndex].Command}
		rf.lastApplied = i
	}
}

func (rf *Raft) updateFollowCommit(leaderCommit int, lastIndex int) {
	oldVal := rf.commitIndex
	if leaderCommit > rf.commitIndex {
		if leaderCommit < lastIndex {
			rf.commitIndex = leaderCommit
		} else {
			rf.commitIndex = lastIndex
		}
	}
	//! update the log added in previous term
	baseIndex := rf.logTable[0].Index
	for oldVal++; oldVal <= rf.commitIndex; oldVal++ {
		//DPrintf("[%d] follower apply: [%d] [%d]", rf.me, oldVal, rf.logTable[oldVal-baseIndex].Command)
		rf.chanCommitted <- ApplyMsg{Index:oldVal, Command:rf.logTable[oldVal-baseIndex].Command}
		rf.lastApplied = oldVal
	}
}

//
// leader sends to each server
// repeat during idle periods to prevent election timeout
//
func (rf *Raft) doHeartbeat() {
	for index := range rf.peers {
		if index == rf.me {
			go func() {
				heartbeatTimer := time.NewTimer(RaftHeartbeatPeriod)
				for rf.role == Leader {
					rf.chanHeartbeat <- true
					rf.updateLeaderCommit()
					heartbeatTimer.Reset(RaftHeartbeatPeriod)
					<-heartbeatTimer.C
				}
			}()
		} else {
			go func(server int) {
				heartbeatTimer := time.NewTimer(RaftHeartbeatPeriod)
				for rf.role == Leader {
					rf.doAppendEntries(server)
					heartbeatTimer.Reset(RaftHeartbeatPeriod)
					<-heartbeatTimer.C
				}
			}(index)
		}
	}
}

func (rf *Raft) startElection(chanQuitElect chan bool) {
	// 1. increment current term
	// 2. vote for self
	// 3. reset election timer
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	var args RequestVoteArgs
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	lastLog := rf.logTable[len(rf.logTable)-1]
	args.LastLogIndex = lastLog.Index
	args.LastLogTerm = lastLog.Term
	rf.persist()
	rf.mu.Unlock()
	// 4. send RequestVote RPC to all other servers
	chanGather := make(chan bool, len(rf.peers))
	chanGather <- true
	for index := range rf.peers {
		if rf.me != index {
			go func (index int) {
				var reply RequestVoteReply
				if rf.sendRequestVote(index, args, &reply) {
					if args.Term < reply.Term {
						rf.mu.Lock()
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.role = Follower
						rf.chanRole <- Follower
						rf.persist()
						rf.mu.Unlock()
						return
					} else if reply.VoteGranted {
						chanGather <- true
					}
				} else {
					chanGather <- false
				}
			}(index)
		}
	}

	yes, no, countTotal := 0, 0, 0
	isLoop := true
	for isLoop {
		select {
		case ok := <- chanGather:
			countTotal++
			if ok {
				yes++
			} else {
				no++
			}
			if yes * 2 > len(rf.peers) {
				rf.role = Leader
				rf.chanRole <- Leader
				isLoop = false
			} else if no * 2 > len(rf.peers) {
				isLoop = false
			}
		case <- chanQuitElect:
			isLoop = false
		}
	}
}

func (rf *Raft) startElectTimer() {
	floatInterval := int(RaftElectionTimeoutHigh - RaftElectionTimeoutLow)
	timeout := time.Duration(rand.Intn(floatInterval)) + RaftElectionTimeoutLow
	electTimer := time.NewTimer(timeout)
	for {
		select {
		case <- rf.chanHeartbeat:
		case <- rf.chanGrantVote:
		case <-electTimer.C:
			rf.role = Candidate
			rf.chanRole <- Candidate
		}
		rf.resetElectTimer(electTimer)
	}
}

func (rf *Raft) resetElectTimer(timer *time.Timer) {
	floatInterval := int(RaftElectionTimeoutHigh - RaftElectionTimeoutLow)
	timeout := time.Duration(rand.Intn(floatInterval)) + RaftElectionTimeoutLow
	timer.Reset(timeout)
}