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
// There are some of the Algorithm logic or methods of this raft implementation are copied from https://github.com/tiraffe/MIT-6.824-2017/blob/master/src/raft/raft.go

import (
	"bytes"
	"cst/src/labgob"
	"cst/src/labrpc"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"
)

const (
	StateLeader = iota
	StateFollower
	StateCandidate

	HEART_BEAT_INTERVAL   = 50
	MAX_ELECTION_INTERVAL = 1000
	MIN_ELECTION_INTERVAL = 500

	raftDebug = 0
)

func RDPrintf(format string, a ...interface{}) (n int, err error) {
	if raftDebug > 0 {
		log.Printf(format, a...)
	}
	return
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	IsSnapshot   bool
	SnapshotData []byte
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	CurrentTerm   int
	VotedFor      int
	ElectionTimer *time.Timer
	Killed        bool
	//
	LogOffset   int
	Log         []Entry
	CommitIndex int
	LastApplied int
	NextIndex   []int
	MatchIndex  []int
	//
	State         int
	ApplyCh       chan ApplyMsg
	commitCondVar *sync.Cond
}

// Entry is a basic log entry
type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

func (rf *Raft) getLogWithOffset(i int) Entry {
	// RDPrintf("LogOffset: %d, Index: %d, Length: %d \n", rf.LogOffset, i-rf.LogOffset, len(rf.Log))
	return rf.Log[i-rf.LogOffset]
}

func (rf *Raft) LastIndex() int {
	return rf.Log[len(rf.Log)-1].Index
}

func (rf *Raft) LastTerm() int {
	return rf.Log[len(rf.Log)-1].Term
}

func (rf *Raft) getLogLengthWithOffset() int {
	return rf.LogOffset + len(rf.Log)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (rf *Raft) getLogSlice(start, end int) []Entry {
	if start > end {
		start = max(start-rf.LogOffset, 0)
		return rf.Log[start : start+1]
	}

	start = max(start-rf.LogOffset, 0)
	end = min(end-rf.LogOffset, len(rf.Log))
	return rf.Log[start:end]
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	term = rf.CurrentTerm
	isleader = rf.State == StateLeader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	e.Encode(rf.LogOffset)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
	d.Decode(&rf.Log)
	d.Decode(&rf.LogOffset)
	rf.LastApplied = rf.LogOffset

	// snapShotData := rf.persister.ReadSnapshot()
	// if snapShotData == nil || len(snapShotData) < 1 {
	// 	return
	// }

	// // rf.CommitIndex = rf.LogOffset
	// rf.LastApplied = rf.LogOffset
	// go func() {
	// 	rf.ApplyCh <- ApplyMsg{CommandValid: false, CommandIndex: rf.LogOffset, IsSnapshot: true, SnapshotData: snapShotData}
	// }()
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
//RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	checkUptoDate := func() bool {
		lastTerm := rf.LastTerm()
		lastIndex := rf.LastIndex()

		if args.LastLogTerm == lastTerm {
			return args.LastLogIndex >= lastIndex
		}
		return args.LastLogTerm > lastTerm
	}

	if rf.CurrentTerm > args.Term {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}

	if rf.CurrentTerm < args.Term {
		rf.CurrentTerm = args.Term
		rf.changeStateTo(StateFollower)
	}
	reply.Term = args.Term
	reply.VoteGranted = false

	if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateID) && checkUptoDate() {
		rf.changeStateTo(StateFollower)
		rf.ElectionTimer.Reset(generateRandomElectionTimeOut())
		reply.VoteGranted = true
		rf.VotedFor = args.CandidateID
	}

	rf.persist()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
//AppendEntriesArgs is a AppendEntries RPC argument structure
type AppendEntriesArgs struct {
	Term              int // leader's term
	LeaderID          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []Entry
	LeaderCommitIndex int
}

//
//AppendEntriesReply is a AppendEntries RPC reply structure
type AppendEntriesReply struct {
	Term          int
	ConflictIndex int
	Success       bool
}

//AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.ConflictIndex = args.PrevLogIndex
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	} else { // args.Term >= rf.CurrentTerm
		if args.Term > rf.CurrentTerm {
			rf.CurrentTerm = args.Term
		}

		if rf.State == StateLeader {
			rf.changeStateTo(StateFollower)
			rf.ElectionTimer = time.NewTimer(generateRandomElectionTimeOut())
			go rf.checkElectionTimeout()
		} else {
			rf.changeStateTo(StateFollower)
			rf.ElectionTimer.Reset(generateRandomElectionTimeOut())
		}
	}

	reply.Term = args.Term
	if args.PrevLogIndex < rf.LogOffset || args.PrevLogIndex > rf.LastIndex() {
		reply.ConflictIndex = rf.getLogLengthWithOffset()
		reply.Success = false
		return
	}

	if rf.getLogWithOffset(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.Success = false
		i := args.PrevLogIndex
		conflictTerm := rf.getLogWithOffset(args.PrevLogIndex).Term
		for i > rf.LogOffset && rf.getLogWithOffset(i-1).Term == conflictTerm {
			i--
		}
		reply.ConflictIndex = i
		return
	}

	reply.Success = true

	if len(args.Entries) > 0 {
		nonMatchingFirstIndex := min(rf.getLogLengthWithOffset(), len(args.Entries)+args.PrevLogIndex+1)
		i := args.PrevLogIndex + 1
		for i < rf.getLogLengthWithOffset() && i < len(args.Entries)+args.PrevLogIndex+1 {
			if rf.getLogWithOffset(i).Term != args.Entries[i-args.PrevLogIndex-1].Term {
				nonMatchingFirstIndex = i
				break
			} else {
				i++
			}
		}

		if nonMatchingFirstIndex != args.PrevLogIndex+len(args.Entries)+1 {
			RDPrintf("Raft #%d has non-matching index at %d\n", rf.me, nonMatchingFirstIndex)
			rf.Log = rf.getLogSlice(0, nonMatchingFirstIndex)
			rf.Log = append(rf.Log, args.Entries[nonMatchingFirstIndex-args.PrevLogIndex-1:]...)
			RDPrintf("Raft #%d appends non-matching index at %d\n", rf.me, nonMatchingFirstIndex)
		}
	}

	if args.LeaderCommitIndex > rf.CommitIndex {
		rf.CommitIndex = min(args.LeaderCommitIndex, rf.LastIndex())
		RDPrintf("Raft #%d has commit index at %d\n", rf.me, rf.CommitIndex)
		rf.commitCondVar.Broadcast()
	}

	rf.persist()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type InstallSnapshotArgs struct {
	Term              int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

//InstallSnapshot RPC handler
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		return
	} else {
		if args.Term > rf.CurrentTerm {
			rf.CurrentTerm = args.Term
		}

		if rf.State == StateLeader {
			rf.changeStateTo(StateFollower)
			rf.ElectionTimer = time.NewTimer(generateRandomElectionTimeOut())
			go rf.checkElectionTimeout()
		} else {
			rf.changeStateTo(StateFollower)
			rf.ElectionTimer.Reset(generateRandomElectionTimeOut())
		}
	}

	reply.Term = args.Term
	rf.SaveSnapshot(args.Data)

	if rf.LastIndex() >= args.LastIncludedIndex && args.LastIncludedIndex >= rf.LogOffset &&
		args.LastIncludedTerm == rf.getLogWithOffset(args.LastIncludedIndex).Term {
		rf.Log = rf.getLogSlice(args.LastIncludedIndex, rf.getLogLengthWithOffset())
	} else {
		rf.Log = make([]Entry, 1)
		rf.Log[0].Index = args.LastIncludedIndex
		rf.Log[0].Term = args.LastIncludedTerm
	}

	rf.LogOffset = args.LastIncludedIndex

	if args.LastIncludedIndex > rf.CommitIndex {
		rf.CommitIndex = args.LastIncludedIndex
	}

	rf.LastApplied = 0
	rf.commitCondVar.Broadcast()

	rf.persist()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
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

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.CurrentTerm

	if rf.State == StateLeader {
		isLeader = true
		index = rf.getLogLengthWithOffset()
		newEntry := Entry{
			Index:   index,
			Term:    term,
			Command: command,
		}
		rf.Log = append(rf.Log, newEntry)
		RDPrintf("Client append new entry at index #%d \n", index)
		rf.persist()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.Killed = true
	RDPrintf("raft #%d has been ----Killed!---- at Term#%d \n", rf.me, rf.CurrentTerm)
}

func generateRandomElectionTimeOut() time.Duration {
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)
	return time.Millisecond * time.Duration(r.Intn(MAX_ELECTION_INTERVAL-MIN_ELECTION_INTERVAL)+MIN_ELECTION_INTERVAL)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Killed = true
	//
	rf.LogOffset = 0
	rf.Log = make([]Entry, 1)
	rf.Log[0].Index = 0
	rf.Log[0].Term = 0
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.NextIndex = make([]int, len(rf.peers))
	rf.MatchIndex = make([]int, len(rf.peers))
	//
	rf.State = StateFollower
	//
	rf.ApplyCh = applyCh
	rf.commitCondVar = sync.NewCond(&rf.mu)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	//
	rf.ElectionTimer = time.NewTimer(generateRandomElectionTimeOut())

	go rf.Run()

	return rf
}

//Run is the main thread of the server
func (rf *Raft) Run() {

	RDPrintf("Raft #%d start running.\n", rf.me)
	rf.Killed = false

	go rf.checkElectionTimeout()
	go rf.runApplyMsg()
}

// Election Check by Election time period
func (rf *Raft) checkElectionTimeout() {
	for {
		if rf.Killed {
			break
		}

		<-rf.ElectionTimer.C

		if _, isLeader := rf.GetState(); isLeader {
			rf.ElectionTimer.Stop()
			return
		}

		rf.mu.Lock()
		rf.changeStateTo(StateCandidate)
		rf.mu.Unlock()

		rf.ElectionTimer.Reset(generateRandomElectionTimeOut())
	}
}

// Sending Heart Beats by time period
func (rf *Raft) runHeartBeats() {
	for {
		if rf.Killed {
			break
		}

		if rf.State == StateLeader {
			rf.broadCastAppendEntries()
		} else {
			break
		}

		time.Sleep(HEART_BEAT_INTERVAL * time.Millisecond)
	}
}

// Apply the Log by commitIndex
func (rf *Raft) runApplyMsg() {
	for {
		if rf.Killed {
			break
		}

		rf.commitApply()
	}
}

func (rf *Raft) startElection() {
	if !rf.Killed {
		RDPrintf("Raft #%d start Election! Term #%d. \n ", rf.me, rf.CurrentTerm)
	}
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.persist()
	go rf.broadCastRequestVote()
}

// Vote Procedure on Candidate
func (rf *Raft) broadCastRequestVote() {

	voteChan := make(chan bool)
	rf.mu.Lock()
	prevTerm := rf.CurrentTerm
	args := &RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.LastIndex(),
		LastLogTerm:  rf.LastTerm(),
	}
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(server int) {
			reply := &RequestVoteReply{-1, false}

			ok := rf.sendRequestVote(server, args, reply)
			rf.updateCurrentTerm(reply.Term)
			select {
			case voteChan <- ok && reply.VoteGranted:
				if ok {
					RDPrintf("Raft #%d recived vote result as %v from Server #%d at Term #%d!\n", rf.me, reply.VoteGranted, server, prevTerm)
				} else {
					RDPrintf("Raft #%d didn't recive vote result from Server #%d at Term #%d!\n", rf.me, server, prevTerm)
				}
			}
		}(i)
	}

	total := len(rf.peers)
	receivedVote := 1
	for i := 0; i < total-1; i++ {
		if <-voteChan {
			receivedVote++
		}

		if receivedVote >= (total+1)/2 {
			break
		}
	}

	if receivedVote >= (total+1)/2 {
		rf.mu.Lock()
		if rf.State == StateCandidate && prevTerm >= rf.CurrentTerm {
			rf.changeStateTo(StateLeader)
			go rf.runHeartBeats()
		}
		rf.mu.Unlock()
	}
}

// Sending Log Repliction or Snapshot to Follower
func (rf *Raft) broadCastAppendEntries() {
	sendLogEntriesToClient := func(server int) bool {

		rf.mu.Lock()
		nextIndex := rf.NextIndex[server]
		if nextIndex > rf.LogOffset {
			argsEntries := make([]Entry, max(rf.getLogLengthWithOffset()-rf.NextIndex[server], 0))
			copy(argsEntries, rf.getLogSlice(rf.NextIndex[server], rf.getLogLengthWithOffset()))
			args := &AppendEntriesArgs{
				Term:              rf.CurrentTerm,
				LeaderID:          rf.me,
				PrevLogIndex:      rf.NextIndex[server] - 1,
				PrevLogTerm:       rf.getLogWithOffset(rf.NextIndex[server] - 1).Term,
				Entries:           argsEntries,
				LeaderCommitIndex: rf.CommitIndex,
			}
			rf.mu.Unlock()

			reply := &AppendEntriesReply{-1, -1, false}

			if rf.State != StateLeader {
				return true
			}

			ok := rf.sendAppendEntries(server, args, reply)
			rf.updateCurrentTerm(reply.Term)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if !rf.Killed {
					RDPrintf("Server %d sends Log Entries to %d success. Term #%d. \n", rf.me, server, rf.CurrentTerm)
				}

				if reply.Success {
					if len(args.Entries) > 0 {
						rf.MatchIndex[server] = args.PrevLogIndex + len(args.Entries)
						rf.NextIndex[server] = rf.MatchIndex[server] + 1
						rf.updateLeaderCommitIndex()
					}
				} else {
					if rf.State == StateLeader {
						rf.NextIndex[server] = reply.ConflictIndex
						return false
					}
				}
			} else {
				if !rf.Killed {
					RDPrintf("Server %d sends Log Entries to %d failed. Term #%d. \n", rf.me, server, rf.CurrentTerm)
				}
			}
		} else {
			args := &InstallSnapshotArgs{
				Term:              rf.CurrentTerm,
				LastIncludedIndex: rf.LogOffset,
				LastIncludedTerm:  rf.Log[0].Term,
				Data:              rf.persister.ReadSnapshot(),
			}
			rf.mu.Unlock()

			reply := &InstallSnapshotReply{-1}

			if rf.State != StateLeader {
				return true
			}

			ok := rf.sendInstallSnapshot(server, args, reply)
			rf.updateCurrentTerm(reply.Term)

			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if !rf.Killed {
					RDPrintf("Server %d sends Install Snapshot to %d success. Term #%d. \n", rf.me, server, rf.CurrentTerm)
				}

				if rf.State == StateLeader {
					rf.MatchIndex[server] = rf.LogOffset
					rf.NextIndex[server] = rf.MatchIndex[server] + 1
				}

			} else {
				if !rf.Killed {
					RDPrintf("Server %d sends Install Snapshot to %d failed. Term #%d. \n", rf.me, server, rf.CurrentTerm)
				}
			}
		}
		return true
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			for {
				if sendLogEntriesToClient(server) {
					break
				}
				time.Sleep(HEART_BEAT_INTERVAL * time.Millisecond)
			}
		}(i)
	}
}

// A commom check and update state machine, copy from https://github.com/tiraffe/MIT-6.824-2017/blob/master/src/raft/raft.go
func (rf *Raft) updateCurrentTerm(Term int) {
	rf.mu.Lock()
	if Term > rf.CurrentTerm {
		rf.CurrentTerm = Term
		if rf.State == StateLeader {
			rf.changeStateTo(StateFollower)
			rf.ElectionTimer = time.NewTimer(generateRandomElectionTimeOut())
			go rf.checkElectionTimeout()
		} else {
			rf.changeStateTo(StateFollower)
			rf.ElectionTimer.Reset(generateRandomElectionTimeOut())
		}
		rf.persist()
	}
	rf.mu.Unlock()
}

func (rf *Raft) setUpLeader() {
	rf.NextIndex = make([]int, len(rf.peers))
	rf.MatchIndex = make([]int, len(rf.peers))

	for i := range rf.NextIndex {
		rf.NextIndex[i] = rf.getLogLengthWithOffset()
	}
}

// Note: server can change to it's same state
func (rf *Raft) changeStateTo(newState int) {
	states := []string{"StateLeader", "StateFollower", "StateCandidate"}
	preState := rf.State

	if !rf.Killed && preState != newState {
		RDPrintf("Attempt to Change Raft #%d from state %s to state %s. Term #%d. \n",
			rf.me, states[preState], states[newState], rf.CurrentTerm)
	}

	switch newState {
	case StateCandidate:
		rf.State = StateCandidate
		rf.startElection()
	case StateFollower:
		rf.State = StateFollower
		rf.VotedFor = -1
	case StateLeader:
		rf.State = StateLeader
		rf.setUpLeader()
	}

	if !rf.Killed && preState != newState {
		RDPrintf("Raft #%d changed form %s to %s! Term #%d. \n",
			rf.me, states[preState], states[newState], rf.CurrentTerm)
	}
}

// Update the Leader commit Index
// Method described in paper not work in my case, copy the code from https://github.com/tiraffe/MIT-6.824-2017/blob/master/src/raft/raft.go
func (rf *Raft) updateLeaderCommitIndex() {
	if rf.State == StateLeader {
		total := len(rf.MatchIndex)
		tmpmatchIndex := make([]int, total)
		copy(tmpmatchIndex, rf.MatchIndex)
		sort.Ints(tmpmatchIndex)
		medianIndex := tmpmatchIndex[(total+1)/2]

		for medianIndex > rf.CommitIndex && medianIndex >= rf.LogOffset {
			if medianIndex <= rf.LastIndex() && rf.getLogWithOffset(medianIndex).Term == rf.CurrentTerm {
				rf.CommitIndex = medianIndex
				rf.commitCondVar.Broadcast()
				break
			}
			medianIndex--
		}
	}

	// if rf.State == StateLeader {
	// 	//fmt.Printf("Raft #%d as a leader checking commitindex. Term #%d.\n", rf.me, rf.CurrentTerm)
	// 	total := len(rf.MatchIndex)
	// 	for N := rf.getLogLengthWithOffset() - 1; N > rf.CommitIndex; N-- {
	// 		if entry := rf.getLogWithOffset(N); entry.Term == rf.CurrentTerm && entry.Index > rf.CommitIndex {
	// 			count := 1
	// 			for i := range rf.MatchIndex {
	// 				if i != rf.me && rf.MatchIndex[i] >= entry.Index {
	// 					count++
	// 				}

	// 				if count >= (total+1)/2 {
	// 					rf.CommitIndex = entry.Index
	// 					if !rf.Killed {
	// 						RDPrintf("Raft #%d as a leader change commitindex to %d! Term #%d. \n", rf.me, N, rf.CurrentTerm)
	// 					}
	// 					break
	// 				}
	// 			}
	// 		} else {
	// 			break
	// 		}
	// 	}
	// }
}

func (rf *Raft) commitApply() {

	rf.mu.Lock()

	for rf.LastApplied == rf.CommitIndex {
		rf.commitCondVar.Wait()
	}

	if rf.CommitIndex > rf.LastApplied {
		if rf.LastApplied < rf.LogOffset {
			if !rf.Killed {
				RDPrintf("Raft #%d apply the snapshot! Term #%d. \n", rf.me, rf.CurrentTerm)
			}

			rf.ApplyCh <- ApplyMsg{CommandValid: false, CommandIndex: rf.LogOffset, IsSnapshot: true, SnapshotData: rf.persister.ReadSnapshot()}

			rf.LastApplied = rf.LogOffset
		} else {
			for i := rf.LastApplied + 1; i <= rf.CommitIndex; i++ {
				if !rf.Killed {
					RDPrintf("Raft #%d apply command to at index %d! Term #%d. \n", rf.me, i, rf.CurrentTerm)
				}

				rf.ApplyCh <- ApplyMsg{CommandValid: true, Command: rf.getLogWithOffset(i).Command, CommandIndex: rf.getLogWithOffset(i).Index}
			}

			rf.LastApplied = rf.CommitIndex
		}
	}

	rf.mu.Unlock()
}

// SaveStateAndSnapshot is for outter server save the snapshot
// func (rf *Raft) SaveStateAndSnapshot(SnapShotData []byte) {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	rf.saveStateAndSnapshot(SnapShotData)
// }

// SaveSnapshot is for outter server save the snapshot
func (rf *Raft) SaveSnapshot(SnapShotData []byte) {
	rf.persister.SaveSnapshot(SnapShotData)
}

// func (rf *Raft) getSnapshotData() []byte {
// 	snapshot := Snapshot{}
// 	r := bytes.NewBuffer(rf.persister.ReadSnapshot())
// 	d := labgob.NewDecoder(r)
// 	d.Decode(&snapshot)
// 	return snapshot.Data
// }

//GetRaftStateSize is a function for get the persister's state size
func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

//LocalCompactSnapshot is a function for a server update it's snapshot locally
func (rf *Raft) LocalCompactSnapshot(appliedIndex int) {
	rf.mu.Lock()

	if appliedIndex <= rf.LogOffset || appliedIndex >= rf.getLogLengthWithOffset() || appliedIndex > rf.CommitIndex {
		rf.mu.Unlock()
		return
	}

	rf.Log = rf.getLogSlice(appliedIndex, rf.getLogLengthWithOffset())
	rf.LogOffset = appliedIndex
	// rf.LastApplied = appliedIndex
	// rf.SaveSnapshot(data)
	rf.persist()
	// rf.commitCondVar.Broadcast()
	rf.mu.Unlock()
}
