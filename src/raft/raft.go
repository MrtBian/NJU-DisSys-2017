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
	"../labrpc"
	//"log"
	"time"
	"math/rand"
	//"bytes"
	//"encoding/gob"
	//"fmt"
)

// import "bytes"
// import "encoding/gob"

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

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]
	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	chanHeartbeat chan bool //传递收到心跳信息的通道
	chanGrantVote chan bool //传递收到投票完成的通道
	chanLeader    chan bool //传递收到成为Leader的通道

	State string //节点状态

	CurrentTerm int
	VotedFor    int
	VoteCount   int
	Log         []LogEntry

	CommitIndex int
	LastApplied int

	NextIndex  []int
	MatchIndex []int

	Timer *time.Timer
}

func (rf *Raft) ResetTimer() {
	rint := rand.Intn(150) + 150
	rf.Timer = time.NewTimer(time.Duration(rint) * time.Millisecond)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	isleader = rf.State == "Leader"
	//fmt.Println(isleader,rf.me,rf.State)
	term = rf.CurrentTerm
	return term, isleader
}

//func (rf *Raft) getLastIndex() int {
//	return rf.Log[len(rf.Log)-1].Index
//}
//func (rf *Raft) getLastTerm() int {
//	return rf.Log[len(rf.Log)-1].Term
//}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	//// Example:
	//w := new(bytes.Buffer)
	//e := gob.NewEncoder(w)
	//e.Encode(rf.CurrentTerm)
	//e.Encode(rf.VotedFor)
	////e.Encode(rf.Log)
	//data := w.Bytes()
	//rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	//r := bytes.NewBuffer(data)
	//d := gob.NewDecoder(r)
	//d.Decode(&rf.CurrentTerm)
	//d.Decode(&rf.VotedFor)
	//d.Decode(&rf.Log)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	// Your data here.
	Term      int
	Success   bool
	NextIndex int
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	//fmt.Println("RVInfo:")
	//fmt.Println(args.CandidateId,rf.me)
	//fmt.Println(args.Term,rf.CurrentTerm)
	//fmt.Println("RVInfoEnd")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		return
	}
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		//preState := rf.State
		rf.State = "Follower"
		//fmt.Println("RV.Node:",rf.me," ",preState,"->",rf.State," Term:",rf.CurrentTerm)
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = true
		rf.chanGrantVote <- true
		rf.VotedFor = args.CandidateId
	}
	if rf.VotedFor==-1||rf.VotedFor == args.CandidateId {
		reply.VoteGranted = true
		rf.chanGrantVote <- true
		rf.VotedFor = args.CandidateId
	}
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
func (rf *Raft) handleRequestVote(reply RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.CurrentTerm {
		rf.CurrentTerm = reply.Term
		rf.State = "Follower"
		rf.VotedFor = -1
	}
	if reply.VoteGranted {
		rf.VoteCount++
		if rf.VoteCount > len(rf.peers)/2 && rf.State == "Candidate" {
			rf.chanLeader <- true
		}
	}
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		return
	}
	rf.chanHeartbeat <- true
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		//preState := rf.State
		rf.State = "Follower"
		rf.VotedFor = -1
		//fmt.Println("AE.Node:",rf.me," ",preState,"->",rf.State," Term:",rf.CurrentTerm)
	}
	reply.Term = args.Term
	reply.Success = true
	return
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	return ok
}

func (rf *Raft) handleAppendEntries(reply AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Success {
		return
	}
	if reply.Term > rf.CurrentTerm {
		rf.CurrentTerm = reply.Term
		rf.State = "Follower"
		//fmt.Println("SA.Node:", rf.me, "Leader->", rf.State, " Term:", rf.CurrentTerm)
		rf.VotedFor = -1
		return
	}
}

func (rf *Raft) broadcastRequestVote() {
	var args RequestVoteArgs
	rf.mu.Lock()
	args.Term = rf.CurrentTerm
	args.CandidateId = rf.me
	rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me && rf.State == "Candidate" {
			go func(i int) {
				var reply RequestVoteReply
				rf.sendRequestVote(i, args, &reply)
				rf.handleRequestVote(reply)
			}(i)
		}
	}
}

/**
 * Log replication
 */
func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := range rf.peers {
		if i != rf.me && rf.State == "Leader" {
			var args AppendEntriesArgs
			args.Term = rf.CurrentTerm
			args.LeaderId = rf.me
			go func(i int, args AppendEntriesArgs) {
				var reply AppendEntriesReply
				rf.sendAppendEntries(i, args, &reply)
				rf.handleAppendEntries(reply)
			}(i, args)
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
	isLeader := rf.State == "Leader"

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

func (rf *Raft) FollowerLoop() {
	rf.ResetTimer()
	select {
	case <-rf.chanHeartbeat:
		//fmt.Println("Node:", rf.me, " heartbeat, Term:", rf.CurrentTerm)
	case <-rf.chanGrantVote:
		//fmt.Println("Node:",rf.me," grantvote")
	case <-rf.Timer.C:
		//fmt.Println("Node:", rf.me, " F.TimeOut, Term:", rf.CurrentTerm)
		rf.State = "Candidate"
	}
}
func (rf *Raft) CandidateLoop() {
	rf.mu.Lock()
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.VoteCount = 1
	rf.ResetTimer()
	rf.mu.Unlock()
	go rf.broadcastRequestVote()
	select {
	case <-rf.Timer.C:
		//fmt.Println("Node:", rf.me, "C.TimeOut, Term:", rf.CurrentTerm)
	case <-rf.chanHeartbeat:
		rf.State = "Follower"
		//fmt.Println("Node:", rf.me, "Candidate->Follower, Term:", rf.CurrentTerm)
	case <-rf.chanLeader:
		rf.mu.Lock()
		//fmt.Println("Node:", rf.me, "Candidate->Leader, Term:", rf.CurrentTerm)
		rf.State = "Leader"
		rf.mu.Unlock()
	}
}
func (rf *Raft) LeaderLoop() {
	go rf.broadcastAppendEntries()
	//fmt.Println("Node:", rf.me, "send heartbeat, Term:", rf.CurrentTerm)
	time.Sleep(50 * time.Millisecond)
}
func (rf *Raft) Loop() {
	for {
		state := rf.State
		switch state {
		case "Follower":
			rf.FollowerLoop()
		case "Candidate":
			rf.CandidateLoop()
		case "Leader":
			rf.LeaderLoop()
		}
		//fmt.Println("Node:", rf.me, " State:", rf.State, "Term:", rf.CurrentTerm)
	}
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

	// Your initialization code here.
	rf.State = "Follower"
	rf.VotedFor = -1
	rf.CurrentTerm = 0
	//rf.Log = append(rf.Log, LogEntry{Term: 0})
	//fmt.Println("Node:",rf.me,"is created Term:",rf.CurrentTerm)
	rf.chanHeartbeat = make(chan bool, 100)
	rf.chanGrantVote = make(chan bool, 100)
	rf.chanLeader = make(chan bool, 100)
	rf.CommitIndex = -1
	rf.LastApplied = -1
	rf.NextIndex = make([]int, len(peers))
	rf.NextIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		rf.Loop()
	}()
	return rf
}
