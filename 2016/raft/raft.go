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
	"log"
	"time"
	"math/rand"
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
	muFollower      []sync.Mutex
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
	Entries []LogEntry
	LeaderCommit int
}

//
// example AppendEntries RPC reply structure.
//
type AppendEntriesReply struct {
	// Your data here.
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.chanRole <- Follower
	}

	reply.Term = args.Term

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
	}else if len(rf.logTable)-1 < args.LastLogIndex ||
			(len(rf.logTable)-1 == args.LastLogIndex &&
			rf.logTable[args.LastLogIndex].Term == args.LastLogTerm) {
		//! candidate's log should be at least as up-to-dates as receiver's log
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.chanGrantVote <- true
		log.Println(rf.me, "granted vote to", args.CandidateId)
	} else {
		reply.VoteGranted = false
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// example AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here.
	rf.mu.Lock()
	defer  rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	//! update current term and only one leader granted in one term
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.leaderId = args.LeaderId
		rf.chanRole <- Follower
	}

	rf.chanHeartbeat <- true
	reply.Term = args.Term

	if len(rf.logTable) <= args.PrevLogIndex || rf.logTable[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}
	if len(args.Entries) != 0 {
		// append entries
		reply.Success = true
		newEntryIndex := args.PrevLogIndex + 1
		if len(rf.logTable) > newEntryIndex {
			if args.Entries[0].Term != rf.logTable[newEntryIndex].Term {
				rf.logTable = rf.logTable[:newEntryIndex]
				rf.logTable = append(rf.logTable, args.Entries[0])
			}
		} else if len(rf.logTable) == newEntryIndex {
			rf.logTable = append(rf.logTable, args.Entries[0])
		}
	}

	rf.updateFollowCommit(args.LeaderCommit, args.PrevLogIndex + 1)
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	var ret bool
	c := make(chan bool)
	go func() {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		c <- ok
	}()
	select {
	case res := <- c:
		ret = res
	case <-time.After(RaftHeartbeatPeriod):
		ret = false
	}
	return ret
}

func (rf *Raft) doAppendEntries(server int) {
	//! make a lock for every follower to serialize
	rf.muFollower[server].Lock()
	defer rf.muFollower[server].Unlock()
	isAppend := true
	for isAppend && rf.getRole() == Leader {
		var args AppendEntriesArgs
		// Basic argument
		args.Term = rf.getCurrentTerm()
		args.LeaderId = rf.me
		args.PrevLogIndex =  rf.nextIndex[server] - 1
		args.PrevLogTerm = rf.logTable[args.PrevLogIndex].Term
		args.LeaderCommit = rf.getCommitIndex()
		// Entries
		lastLogIndex := len(rf.logTable) - 1
		if (lastLogIndex >= rf.nextIndex[server]) {
			args.Entries = append(args.Entries, rf.logTable[rf.nextIndex[server]])
		} else {
			isAppend = false
		}
		// Reply
		var reply AppendEntriesReply
		if rf.sendAppendEntries(server, args, &reply) {
			//log.Println(rf.me, "receive reply from", server, reply.Success)
			if reply.Term > args.Term {
				rf.setCurrentTerm(reply.Term)
				rf.setVotedFor(-1)
				rf.chanRole <- Follower
				return
			}
			if isAppend {
				if reply.Success {
					rf.matchIndex[server] = rf.nextIndex[server]
					rf.nextIndex[server]++
				} else if rf.nextIndex[server]-1 > rf.matchIndex[server] {
					//! out of range
					rf.nextIndex[server]--
				}
			}
		} else {
			return
		}
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
	index := -1
	term := -1
	isLeader := false

	if rf.getRole() == Leader {
		index = len(rf.logTable)
		term = rf.getCurrentTerm()
		isLeader = true
		entry := LogEntry{Command:command, Term:term};
		rf.logTable = append(rf.logTable, entry);
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
	rf.role = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.leaderId = -1
	rf.logTable = append(rf.logTable, LogEntry{Term:0})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers), len(peers))
	rf.matchIndex = make([]int, len(peers), len(peers))

	rf.chanHeartbeat = make(chan bool)
	rf.chanGrantVote = make(chan bool)

	rf.chanRole = make(chan Role)
	rf.chanCommitted = make(chan ApplyMsg)

	rf.muFollower = make([]sync.Mutex, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//! set seed at the beginning
	rand.Seed(time.Now().UnixNano())

	rf.changeRole()
	go rf.startElectTimer()
	go rf.applyEntry(applyCh)

	return rf
}

func (rf *Raft) applyEntry(ch chan ApplyMsg) {
	for {
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
		}
		msg := <- rf.chanCommitted
		ch <- msg
		log.Println(rf.me, msg.Index, msg.Command)
	}
}

func (rf *Raft) leaderReInit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.logTable)
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) changeRole() {
	//! use state machine
	switch rf.role {
	case Leader:
		log.Println(rf.me, "run as leader")
		rf.leaderReInit()
		go rf.runAsLeader()
	case Candidate:
		log.Println(rf.me, "run as candidate")
		go rf.runAsCandidate()
	case Follower:
		log.Println(rf.me, "run as follower")
		go rf.runAsFollower()
	}
}

func (rf *Raft) runAsLeader() {
	go rf.doHeartbeat()
	for rf.role == Leader {
		select {
		case role := <- rf.chanRole:
			if role == Follower {
				rf.role = role
			}
		}
	}
	rf.changeRole()
}

func (rf *Raft) runAsCandidate() {
	for rf.role == Candidate {
		chanQuitElect := make(chan bool)
		go rf.startElection(chanQuitElect)
		select {
		case role := <- rf.chanRole:
			switch role {
			case Leader:
				rf.role = role
			case Candidate:
				close(chanQuitElect)
			case Follower:
				rf.role = role
				close(chanQuitElect)
			}
		}
	}
	rf.changeRole()
}

func (rf *Raft) runAsFollower() {
	for rf.role == Follower {
		select {
		case role := <- rf.chanRole:
			if role == Candidate {
				rf.role = role
			}
		}
	}
	rf.changeRole()
}

// once an entry  from the current term has been committed in this way,
// then all prior entries are committed indirectly because of the Log Matching Property.
func (rf *Raft) updateLeaderCommit() {
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
		if countServer > len(rf.peers) / 2 {
			newIndex = i
			break
		}
	}
	if oldIndex == newIndex {
		return
	}
	//! update the log added in previous term
	for i := oldIndex + 1; i <= newIndex; i++ {
		rf.chanCommitted <- ApplyMsg{Index:i, Command:rf.logTable[i].Command}
	}
	rf.setCommitIndex(newIndex)
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
		rf.chanCommitted <- ApplyMsg{Index:oldVal, Command:rf.logTable[oldVal].Command}
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
	args.LastLogIndex = rf.commitIndex
	args.LastLogTerm = rf.logTable[args.LastLogIndex].Term

	// 4. send RequestVote RPC to all other servers
	chanGather := make(chan bool, len(rf.peers))
	chanGather <- true
	for index, _ := range rf.peers {
		if rf.me != index {
			go func (index int) {
				var reply RequestVoteReply
				rf.sendRequestVote(index, args, &reply)
				if args.Term < reply.Term {
					rf.setCurrentTerm(reply.Term)
					rf.setVotedFor(-1)
					rf.chanRole <- Follower
					return
				} else if reply.VoteGranted {
					chanGather <- true
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
			if yes > len(rf.peers) / 2 {
				rf.chanRole <- Leader
				isLoop = false
			} else if no > len(rf.peers) / 2 {
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
			if rf.role == Leader {
				electTimer.Stop()
			} else {
				log.Println(rf.me, "is logically timeout")
				rf.setVotedFor(-1)
				rf.chanRole <- Candidate
				rf.resetElectTimer(electTimer)
			}
		}
	}
}


func (rf *Raft) resetElectTimer(timer *time.Timer) {
	floatInterval := int(RaftElectionTimeoutHigh - RaftElectionTimeoutLow)
	timeout := time.Duration(rand.Intn(floatInterval)) + RaftElectionTimeoutLow
	timer.Reset(timeout)
}