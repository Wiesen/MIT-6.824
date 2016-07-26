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
)

// import "bytes"
// import "encoding/gob"

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

/*-------------------- get -------------------*/
func (rf *Raft) getCurrentTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

func (rf *Raft) getVotedFor() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.votedFor
}

func (rf *Raft) getLeaderId() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.leaderId
}

func (rf *Raft) getCommitIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.commitIndex
}

func (rf *Raft) getLastApplied() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastApplied
}

func (rf *Raft) getRole() Role {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.role
}

/*-------------------- set -------------------*/
func (rf *Raft) setCurrentTerm(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < term {
		rf.currentTerm = term
	}
}

func (rf *Raft) setVotedFor(id int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = id
}

func (rf *Raft) setLeaderId(id int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.leaderId = id
}

func (rf *Raft) setCommitIndex(index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.commitIndex < index {
		rf.commitIndex = index
	}
}

func (rf *Raft) setRole(role Role) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = role
}

/*------------- outer function ---------------*/
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
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logTable)
}


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
	FailTerm int
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
	} else if len(rf.logTable)-1 > args.LastLogIndex &&					//! index
			rf.logTable[len(rf.logTable)-1].Term == args.LastLogTerm {
		reply.VoteGranted = false
	} else {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.chanGrantVote <- true
		//DPrintf("[%d] granted vote to [%d]", rf.me, args.CandidateId)
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

	//! update current term and leader and but doesn't vote
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.leaderId = args.LeaderId
		rf.role = Follower
		rf.chanRole <- Follower
		DPrintf("[%d] update to term [%d]", rf.me, rf.currentTerm)
	}

	rf.chanHeartbeat <- true
	reply.Term = args.Term

	// optimized for backing quickly
	// include the term of the conflicting entry and the first index it stores for that term.
	if len(rf.logTable) <= args.PrevLogIndex {
		reply.Success = false
		nextIndex := len(rf.logTable) - 1
		reply.FailTerm = rf.logTable[nextIndex].Term
		for nextIndex >= 0 && rf.logTable[nextIndex].Term == reply.FailTerm {
			nextIndex--
		}
		reply.NextIndex = nextIndex + 1
		return
	} else if rf.logTable[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		nextIndex := args.PrevLogIndex
		reply.FailTerm = rf.logTable[nextIndex].Term
		for nextIndex >= 0 && rf.logTable[nextIndex].Term == reply.FailTerm {
			nextIndex--
		}
		reply.NextIndex = nextIndex + 1
		return
	}

	if len(args.Entry) != 0 {
		basicIndex := args.PrevLogIndex + 1
		//!!! erase those conflicting with a new one all that following
		//var i int
		//var entry LogEntry
		//for i, entry = range args.Entry {
		//	if basicIndex + i < len(rf.logTable) {
		//		if entry.Term != rf.logTable[basicIndex + i].Term {
		//			rf.logTable = rf.logTable[:basicIndex + i]
		//			break
		//		}
		//	} else {
		//		break
		//	}
		//}
		//rf.logTable = append(rf.logTable, args.Entry[i:]...)
		for i, entry := range args.Entry {
			if basicIndex + i < len(rf.logTable) {
				if entry.Term != rf.logTable[basicIndex + i].Term {
					//rf.logTable[basicIndex + i] = entry
					rf.logTable = append(rf.logTable[:basicIndex+i], entry)
					DPrintf("[%d] in term [%d] replace entry [%d] [%d]",
						rf.me, rf.currentTerm, basicIndex + i, rf.logTable[basicIndex + i].Command)
				}
			} else {
				rf.logTable = append(rf.logTable, entry)
				DPrintf("[%d] in term [%d] append entry [%d] [%d]",
					rf.me, rf.currentTerm, basicIndex + i, rf.logTable[basicIndex + i].Command)
			}
		}
	}
	reply.Success = true
	reply.NextIndex = len(rf.logTable)
	rf.updateFollowCommit(args.LeaderCommit, len(rf.logTable) - 1)
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

	for ok := false; !ok && rf.getRole() == Leader; {
		var args AppendEntriesArgs
		// Basic argument
		args.Term = rf.getCurrentTerm()
		args.LeaderId = rf.me
		args.PrevLogIndex =  rf.nextIndex[server] - 1
		args.PrevLogTerm = rf.logTable[args.PrevLogIndex].Term
		args.LeaderCommit = rf.getCommitIndex()

		// Entries
		if rf.nextIndex[server] < len(rf.logTable) {
			//if again {
			//	again = false
			//	DPrintf("[%d] in term [%d] send entry to [%d]: [%d] [%d] to [%d] [%d]",
			//		rf.me, rf.currentTerm,
			//		server, rf.nextIndex[server], rf.logTable[rf.nextIndex[server]].Command,
			//		len(rf.logTable) - 1, rf.logTable[len(rf.logTable) - 1].Command)
			//}
			args.Entry = rf.logTable[rf.nextIndex[server]:]
		}

		// Reply
		var reply AppendEntriesReply
		if rf.sendAppendEntries(server, args, &reply) {
			//!!!after a long time: in case of role changing while appending
			if rf.getRole() != Leader || args.Term != rf.getCurrentTerm() {
				break
			}

			if reply.Term > args.Term {
				rf.setCurrentTerm(reply.Term)
				rf.setVotedFor(-1)
				rf.setRole(Follower)
				rf.chanRole <- Follower
				rf.persist()
				break
			}

			//!!!after a long time: heartbeat RPC reply fail index as well
			if reply.Success {
				ok = true
				if len(args.Entry) > 0 {
					rf.matchIndex[server] = rf.nextIndex[server] + len(args.Entry) - 1
					rf.nextIndex[server] = rf.matchIndex[server] + 1
					//rf.matchIndex[server] = reply.NextIndex - 1
					//rf.nextIndex[server] = reply.NextIndex
				}
			} else {
				//! out of range
				if reply.NextIndex > rf.matchIndex[server] + 1 {
					rf.nextIndex[server] = reply.NextIndex
				} else {
					rf.nextIndex[server] = rf.matchIndex[server] + 1
				}
			}
		} else {
			//!!!after a long time:
			// for unreliable, continue to try until receiving reply
			continue
		}
	}
	rf.chanAppend[server] <- true
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
	index := -1
	term := -1
	isLeader := false

	if rf.getRole() == Leader {
		index = len(rf.logTable)
		term = rf.getCurrentTerm()
		isLeader = true
		entry := LogEntry{Command:command, Term:term};

		rf.mu.Lock()
		rf.logTable = append(rf.logTable, entry);
		DPrintf("[%d] leader append: [%d] [%d]", rf.me, len(rf.logTable) - 1, entry.Command)
		rf.persist()
		rf.mu.Unlock()

		go rf.Replica()
	}
	return index, term, isLeader
}

func (rf *Raft) Replica() {
	// replica log
	for server := range rf.peers {
		if server != rf.me {
			go rf.doAppendEntries(server)
		}
	}
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
	rf.logTable = append(rf.logTable, LogEntry{Term:0})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers), len(peers))
	rf.matchIndex = make([]int, len(peers), len(peers))

	rf.chanHeartbeat = make(chan bool)
	rf.chanGrantVote = make(chan bool)

	rf.chanRole = make(chan Role, 1)
	rf.chanCommitted = applyCh

	rf.chanAppend = make([]chan bool, len(peers))
	for i := range rf.chanAppend {
		rf.chanAppend[i] = make(chan bool, 1)
		rf.chanAppend[i] <- true
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//! set seed at the beginning
	rand.Seed(time.Now().UnixNano())

	rf.changeRole()
	go rf.startElectTimer()

	return rf
}

func (rf *Raft) changeRole() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	switch rf.role {
	case Leader:
		for i := range rf.peers {
			rf.nextIndex[i] = len(rf.logTable)
			rf.matchIndex[i] = 0
		}
		go rf.runAsLeader()
	case Candidate:
		go rf.runAsCandidate()
	case Follower:
		go rf.runAsFollower()
	}
}

func (rf *Raft) runAsLeader() {
	DPrintf("[%d] run as leader", rf.me)
	defer  DPrintf("[%d] lost leader", rf.me)
	go rf.doHeartbeat()
	for rf.role == Leader {
		rf.role = <- rf.chanRole
	}
	rf.changeRole()
}

func (rf *Raft) runAsCandidate() {
	for rf.role == Candidate {
		chanQuitElect := make(chan bool)
		go rf.startElection(chanQuitElect)
		rf.role = <- rf.chanRole
		close(chanQuitElect)
	}
	rf.changeRole()
}

func (rf *Raft) runAsFollower() {
	for rf.role == Follower {
		rf.role = <- rf.chanRole
	}
	rf.changeRole()
}

// once an entry from the current term has been committed in this way,
// then all prior entries are committed indirectly because of the Log Matching Property.
func (rf *Raft) updateLeaderCommit() {
	defer rf.persist()
	// update commitIndex
	oldIndex := rf.getCommitIndex()
	newIndex := oldIndex
	for i := len(rf.logTable)-1; i>oldIndex && rf.logTable[i].Term==rf.getCurrentTerm(); i-- {
		countServer := 1
		for server := range rf.peers {
			if server != rf.me && rf.matchIndex[server] >= i {
				countServer++
			}
		}
		if countServer * 2 > len(rf.peers) {
			newIndex = i
			break
		}
	}
	if oldIndex == newIndex {
		return
	}
	rf.setCommitIndex(newIndex)
	rf.persist()

	//! update the log added in previous term
	for i := oldIndex + 1; i <= newIndex; i++ {
		DPrintf("[%d] leader apply: [%d] [%d]", rf.me, i, rf.logTable[i].Command)
		rf.chanCommitted <- ApplyMsg{Index:i, Command:rf.logTable[i].Command}
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
	for oldVal++; oldVal <= rf.commitIndex; oldVal++ {
		DPrintf("[%d] follower apply: [%d] [%d]", rf.me, oldVal, rf.logTable[oldVal].Command)
		rf.chanCommitted <- ApplyMsg{Index:oldVal, Command:rf.logTable[oldVal].Command}
		rf.lastApplied = oldVal
	}
}

//
// leader sends to each server
// repeat during idle periods to prevent election timeout
//
func (rf *Raft) doHeartbeat() {
	for index, _ := range rf.peers {
		if index == rf.me {
			go func() {
				heartbeatTimer := time.NewTimer(RaftHeartbeatPeriod)
				for rf.getRole() == Leader {
					rf.chanHeartbeat <- true
					rf.updateLeaderCommit()
					heartbeatTimer.Reset(RaftHeartbeatPeriod)
					<-heartbeatTimer.C
				}
			}()
		} else {
			go func(server int) {
				heartbeatTimer := time.NewTimer(RaftHeartbeatPeriod)
				for rf.getRole() == Leader {
					rf.doAppendEntries(server)
					heartbeatTimer.Reset(RaftHeartbeatPeriod)
					<-heartbeatTimer.C
				}
			}(index)
		}
	}
}

func (rf *Raft) startElection(chanQuitElect chan bool) {

	var args RequestVoteArgs

	// 1. increment current term
	// 2. vote for self
	// 3. reset election timer
	rf.currentTerm++
	rf.votedFor = rf.me
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.logTable) - 1
	args.LastLogTerm = rf.logTable[args.LastLogIndex].Term
	rf.persist()
	// 4. send RequestVote RPC to all other servers
	chanGather := make(chan bool, len(rf.peers))
	chanGather <- true
	for index, _ := range rf.peers {
		if rf.me != index {
			go func (index int) {
				var reply RequestVoteReply
				if rf.sendRequestVote(index, args, &reply) {
					if args.Term < reply.Term {
						rf.setCurrentTerm(reply.Term)
						rf.setVotedFor(-1)
						//rf.setRole(Follower)
						rf.persist()
						rf.chanRole <- Follower
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
				//rf.setRole(Leader)
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
			rf.resetElectTimer(electTimer)
		case <- rf.chanGrantVote:
			rf.resetElectTimer(electTimer)
		case <-electTimer.C:
			//rf.setRole(Candidate)
			rf.chanRole <- Candidate
			rf.resetElectTimer(electTimer)
		}
	}
}

func (rf *Raft) resetElectTimer(timer *time.Timer) {
	floatInterval := int(RaftElectionTimeoutHigh - RaftElectionTimeoutLow)
	timeout := time.Duration(rand.Intn(floatInterval)) + RaftElectionTimeoutLow
	timer.Reset(timeout)
}