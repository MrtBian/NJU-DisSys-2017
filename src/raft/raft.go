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
	"bytes"
	"encoding/gob"
	"fmt"
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

	chanCommit    chan bool
	chanHeartbeat chan bool
	chanGrantVote chan bool
	chanLeader    chan bool
	chanApply     chan ApplyMsg

	State string

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

func (rf *Raft) ResetTimer(){
	rint := rand.Int63()%500 +200
	rf.Timer = time.NewTimer(time.Duration(rint)*time.Microsecond)
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


func (rf *Raft) getLastIndex() int {
	return rf.Log[len(rf.Log) - 1].Index
}
func (rf *Raft) getLastTerm() int {
	return rf.Log[len(rf.Log) - 1].Term

}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	 w := new(bytes.Buffer)
	 e := gob.NewEncoder(w)
	 e.Encode(rf.CurrentTerm)
	 e.Encode(rf.VotedFor)
	 //e.Encode(rf.Log)
	 data := w.Bytes()
	 rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
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
	Term        int
	Success     bool
	NextIndex	int
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
	fmt.Println("RVInfo:")
	fmt.Println(args.CandidateId,rf.me)
	fmt.Println(args.Term,rf.CurrentTerm)
	fmt.Println("RVInfoEnd")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	//fmt.Println(rf.me,rf.State,rf.CurrentTerm)
	reply.VoteGranted = false
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		return
	}
	if args.Term > rf.CurrentTerm {
		rf.chanGrantVote <- true
		rf.CurrentTerm = args.Term
		preState := rf.State
		rf.State = "Follower"
		rf.VotedFor = args.CandidateId
		rf.ResetTimer()
		fmt.Println("RV.Node:",rf.me," ",preState,"->",rf.State," Term:",rf.CurrentTerm)
		reply.VoteGranted = true
		reply.Term = rf.CurrentTerm
	}
	reply.Term = rf.CurrentTerm


}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	fmt.Println(rf.me,rf.State,rf.CurrentTerm)
	reply.Success = false
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.NextIndex = rf.getLastIndex() + 1
		return
	}
	rf.chanHeartbeat <- true
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		preState := rf.State
		rf.State = "Follower"
		rf.VotedFor = -1
		//Reset Timer
		rf.ResetTimer()
		fmt.Println("AE.Node:",rf.me," ",preState,"->",rf.State," Term:",rf.CurrentTerm)
	}
	reply.Term = args.Term

	if args.PrevLogIndex > rf.getLastIndex() {
		reply.NextIndex = rf.getLastIndex() + 1
		return
	}

	baseIndex := rf.Log[0].Index

	if args.PrevLogIndex > baseIndex {
		term := rf.Log[args.PrevLogIndex - baseIndex].Term
		if args.PrevLogTerm != term {
			for i := args.PrevLogIndex - 1; i >= baseIndex; i-- {
				if rf.Log[i - baseIndex].Term != term {
					reply.NextIndex = i + 1
					break
				}
			}
			return
		}
	}
	if args.PrevLogIndex < baseIndex {

	} else {
		//Append any new entries not already in the log
		rf.Log = rf.Log[: args.PrevLogIndex + 1 - baseIndex]
		rf.Log = append(rf.Log, args.Entries...)
		reply.Success = true
		reply.NextIndex = rf.getLastIndex() + 1
	}

	return
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		term := rf.CurrentTerm
		if rf.State != "Candidate" {
			return ok
		}
		if args.Term != term {
			return ok
		}
		if reply.Term > term {
			rf.CurrentTerm = reply.Term
			rf.State = "Follower"
			rf.VotedFor = -1
			rf.persist()
		}
		if reply.VoteGranted {
			rf.VoteCount++
			if  rf.VoteCount > len(rf.peers) / 2 && rf.State == "Candidate"{
				rf.State = "Follower"
				rf.chanLeader <- true
			}
		}
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int,args AppendEntriesArgs,reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.State != "Leader" {
			return ok
		}
		if args.Term != rf.CurrentTerm {
			return ok
		}

		if reply.Term > rf.CurrentTerm {
			rf.CurrentTerm = reply.Term
			rf.State = "Follower"
			rf.VotedFor = -1
			rf.persist()
			return ok
		}
		if reply.Success {
			if len(args.Entries) > 0 {
				rf.NextIndex[server] = args.Entries[len(args.Entries) - 1].Index + 1
				//reply.NextIndex
				//rf.nextIndex[server] = reply.NextIndex
				rf.MatchIndex[server] = rf.NextIndex[server] - 1
			}
		} else {
			rf.NextIndex[server] = reply.NextIndex
		}
	}
	return ok
}

func (rf *Raft) broadcastRequestVote() {
	var args RequestVoteArgs
	rf.mu.Lock()
	args.Term = rf.CurrentTerm
	args.CandidateId = rf.me
	args.LastLogTerm = rf.getLastTerm()
	args.LastLogIndex = rf.getLastIndex()
	rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me && rf.State == "Candidate" {
			go func(i int) {
				var reply RequestVoteReply
				rf.sendRequestVote(i, args, &reply)
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


	N := rf.CommitIndex
	last := rf.getLastIndex()
	baseIndex := rf.Log[0].Index
	//If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
	for i := rf.CommitIndex + 1; i <= last; i++ {
		num := 1
		for j := range rf.peers {
			if j != rf.me && rf.MatchIndex[j] >= i && rf.Log[i - baseIndex].Term == rf.CurrentTerm {
				num++
			}
		}
		if 2 * num > len(rf.peers) {
			N = i
		}
	}
	if N != rf.CommitIndex {
		rf.CommitIndex = N
		rf.chanCommit <- true
	}

	for i := range rf.peers {
		if i != rf.me && rf.State == "Leader" {

			//copy(args.Entries, rf.log[args.PrevLogIndex + 1:])

			if rf.NextIndex[i] > baseIndex {
				var args AppendEntriesArgs
				args.Term = rf.CurrentTerm
				args.LeaderId = rf.me
				args.PrevLogIndex = rf.NextIndex[i] - 1
				//	fmt.Printf("baseIndex:%d PrevLogIndex:%d\n",baseIndex,args.PrevLogIndex )
				args.PrevLogTerm = rf.Log[args.PrevLogIndex - baseIndex].Term
				//args.Entries = make([]LogEntry, len(rf.log[args.PrevLogIndex + 1:]))
				args.Entries = make([]LogEntry, len(rf.Log[args.PrevLogIndex + 1 - baseIndex:]))
				copy(args.Entries, rf.Log[args.PrevLogIndex + 1 - baseIndex:])
				args.LeaderCommit = rf.CommitIndex
				go func(i int, args AppendEntriesArgs) {
					var reply AppendEntriesReply
					rf.sendAppendEntries(i, args, &reply)
				}(i, args)
			} else {
				//var args InstallSnapshotArgs
				//args.Term = rf.currentTerm
				//args.LeaderId = rf.me
				//args.LastIncludedIndex = rf.log[0].LogIndex
				//args.LastIncludedTerm = rf.log[0].LogTerm
				//args.Data = rf.persister.snapshot
				//go func(server int, args InstallSnapshotArgs) {
				//	reply := &InstallSnapshotReply{}
				//	rf.sendInstallSnapshot(server, args, reply)
				//}(i, args)
			}
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
	rf.Log = append(rf.Log, LogEntry{Term: 0})
	//fmt.Println("Node:",rf.me,"is created Term:",rf.CurrentTerm)
	rf.chanCommit = make(chan bool, 100)
	rf.chanHeartbeat = make(chan bool, 100)
	rf.chanGrantVote = make(chan bool, 100)
	rf.chanLeader = make(chan bool, 100)
	rf.chanApply = applyCh

	rf.CommitIndex = -1
	rf.LastApplied = -1
	rf.NextIndex = make([]int, len(peers))
	rf.NextIndex = make([]int, len(peers))
	rand.Seed(time.Now().Unix())
	//200ms-500ms
	rint := rand.Intn(300) +200
	//fmt.Println(rint)
	rf.Timer = time.NewTimer(time.Duration(rint)*time.Microsecond)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			switch rf.State {
			case "Follower":
				select {
				case <-rf.chanHeartbeat:
					fmt.Println("Node:",rf.me," heartbeat")
					rf.ResetTimer()
				case <-rf.chanGrantVote:
					fmt.Println("Node:",rf.me," grantvote")
					rf.ResetTimer()
				case <-time.After(time.Duration(rand.Int63()%500+200) * time.Millisecond):
					rf.State = "Candidate"
				//case <-rf.Timer.C:
				//	fmt.Println("Node:",rf.me,"F.TimeOut")
				//	rf.State = "Candidate"
				}
			case "Leader":
				rf.broadcastAppendEntries()
				fmt.Println("Node:",rf.me,"send heartbeat")
				time.Sleep(50*time.Microsecond)
			case "Candidate":
				rf.mu.Lock()
				rf.CurrentTerm++
				rf.VotedFor = rf.me
				rf.VoteCount = 1
				rf.persist()
				rf.mu.Unlock()
				//args := RequestVoteArgs{
				//	Term:rf.CurrentTerm,
				//	CandidateId:rf.me,
				//
				//}
				//reply := &RequestVoteReply{}
				//rf.sendRequestVote(rf.me,args,reply)
				go rf.broadcastRequestVote()
				select {
				case <-time.After(time.Duration(rand.Int63()%500+200) * time.Millisecond):fmt.Println("Node:",rf.me,"C.TimeOut")
				case <-rf.chanHeartbeat:
					rf.State = "Follower"
					fmt.Println("Node:",rf.me,"Candidate->Follower",rf.State," Term:",rf.CurrentTerm)
				case <-rf.chanLeader:
					rf.mu.Lock()
					//fmt.Println(rf.me,"L")
					rf.State = "Leader"

					rf.NextIndex = make([]int, len(rf.peers))
					rf.MatchIndex = make([]int, len(rf.peers))
					for i := range rf.peers {
						//The leader maintains a nextIndex for each follower, which is the index of the next log entry the leader will send to that follower.
						// When a leader first comes to power, it initializes all nextIndex values to the index just after the last one in its log
						rf.NextIndex[i] = rf.getLastIndex() + 1
						rf.MatchIndex[i] = 0
					}
					rf.mu.Unlock()
				}
			}
			//fmt.Printf("E.Node:%v State:%s Term:%v VoteFor:%v VoteCount:%v\n",rf.me,rf.State,rf.CurrentTerm,rf.VotedFor,rf.VoteCount)
		}
	}()

	return rf
}
