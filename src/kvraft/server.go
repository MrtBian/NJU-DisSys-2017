package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 0

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
<<<<<<< HEAD
<<<<<<< HEAD
	Key      string
	Value    string
	Op       string // "Get", "Put" or "Append"
	ClientID int64  // client id
	SeqNo    int    // request sequence number
}

type LatestReply struct {
	Seq   int      // latest request
	Reply GetReply // latest reply
=======
>>>>>>> parent of df1a00b... finish
=======
>>>>>>> parent of df1a00b... finish
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
<<<<<<< HEAD
<<<<<<< HEAD
	persist       *raft.Persister
	db            map[string]string
	snapshotIndex int
	notifyChs     map[int]chan struct{} // per log

	// shutdown chan
	shutdownCh chan struct{}

	// duplication detection table
	duplicate map[int64]*LatestReply
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// not leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = ""
		return
	}

	DPrintf("[%d]: leader %d receive rpc: Get(%q).\n", kv.me, kv.me, args.Key)

	kv.mu.Lock()
	// duplicate put/append request
	if dup, ok := kv.duplicate[args.ClientID]; ok {
		// filter duplicate
		if args.SeqNo <= dup.Seq {
			kv.mu.Unlock()
			reply.WrongLeader = false
			reply.Err = OK
			reply.Value = dup.Reply.Value
			return
		}
	}

	cmd := Op{Key: args.Key, Op: "Get", ClientID: args.ClientID, SeqNo: args.SeqNo}
	index, term, _ := kv.rf.Start(cmd)

	ch := make(chan struct{})
	kv.notifyChs[index] = ch

	kv.mu.Unlock()

	reply.WrongLeader = false
	reply.Err = OK

	// wait for Raft to complete agreement
	select {
	case <-ch:
		// lose leadership
		curTerm, isLeader := kv.rf.GetState()
		// what if still leader, but different term? let client retry
		if !isLeader || term != curTerm {
			reply.WrongLeader = true
			reply.Err = ""
			return
		}

		kv.mu.Lock()
		if value, ok := kv.db[args.Key]; ok {
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
		}
		kv.mu.Unlock()
	case <-kv.shutdownCh:
	}
=======
}
=======
}

>>>>>>> parent of df1a00b... finish


func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
<<<<<<< HEAD
>>>>>>> parent of df1a00b... finish
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
<<<<<<< HEAD
	// not leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = ""
		return
	}

	DPrintf("[%d]: leader %d receive rpc: PutAppend(%q => (%q,%q), (%d-%d).\n", kv.me, kv.me,
		args.Op, args.Key, args.Value, args.ClientID, args.SeqNo)

	kv.mu.Lock()
	// duplicate put/append request
	if dup, ok := kv.duplicate[args.ClientID]; ok {
		// filter duplicate
		if args.SeqNo <= dup.Seq {
			kv.mu.Unlock()
			reply.WrongLeader = false
			reply.Err = OK
			return
		}
	}

	// new request
	cmd := Op{Key: args.Key, Value: args.Value, Op: args.Op, ClientID: args.ClientID, SeqNo: args.SeqNo}
	index, term, _ := kv.rf.Start(cmd)
	ch := make(chan struct{})
	kv.notifyChs[index] = ch
	kv.mu.Unlock()

	reply.WrongLeader = false
	reply.Err = OK

	// wait for Raft to complete agreement
	select {
	case <-ch:
		// lose leadership
		curTerm, isLeader := kv.rf.GetState()
		if !isLeader || term != curTerm {
			reply.WrongLeader = true
			reply.Err = ""
			return
		}
	case <-kv.shutdownCh:
		return
	}
=======
>>>>>>> parent of df1a00b... finish
=======
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
>>>>>>> parent of df1a00b... finish
}

// applyDaemon receive applyMsg from Raft layer, apply to Key-Value state machine
// then notify related client if is leader
func (kv *RaftKV) applyDaemon() {
	for {
		select {
		case <-kv.shutdownCh:
			DPrintf("[%d]: server %d is shutting down.\n", kv.me, kv.me)
			return
		case msg, ok := <-kv.applyCh:
			if ok {

					// notify channel
					if notifyCh, ok := kv.notifyChs[msg.Index]; ok && notifyCh != nil {
						close(notifyCh)
						delete(kv.notifyChs, msg.Index)
					}
				}
			}
		}
	}



//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	close(kv.shutdownCh)
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
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
<<<<<<< HEAD

	// You may need initialization code here.
	// store key-value pairs
	kv.db = make(map[string]string)
	kv.notifyChs = make(map[int]chan struct{})
	kv.persist = persister
=======
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
<<<<<<< HEAD
>>>>>>> parent of df1a00b... finish

	// shutdown channel
	kv.shutdownCh = make(chan struct{})
=======
>>>>>>> parent of df1a00b... finish

<<<<<<< HEAD
	// duplication detection table: client->seq no.-> reply
	kv.duplicate = make(map[int64]*LatestReply)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applyDaemon()

	return kv
}
<<<<<<< HEAD
=======
	return kv
}
>>>>>>> parent of df1a00b... finish
=======
>>>>>>> parent of df1a00b... finish
