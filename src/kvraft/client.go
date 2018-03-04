package raftkv

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
import "../labrpc"
import "crypto/rand"
>>>>>>> parent of 60e9924... fix
import (
	"math/big"
	"sync"
)
=======
import "labrpc"
import "crypto/rand"
import "math/big"
>>>>>>> parent of df1a00b... finish
=======
import "labrpc"
import "crypto/rand"
import "math/big"
>>>>>>> parent of df1a00b... finish


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD

	leader int   // remember last leader
	seq    int   // RPC sequence number
	id     int64 // client id
=======
>>>>>>> parent of df1a00b... finish
=======
>>>>>>> parent of df1a00b... finish
=======
	id        int64
	requestID int
	mu        sync.Mutex
	preLeader int

>>>>>>> parent of 60e9924... fix
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
	ck.leader = len(servers)
	ck.seq = 1
	ck.id = generateID()

	DPrintf("Clerk: %d\n", ck.id)

=======
>>>>>>> parent of df1a00b... finish
=======
>>>>>>> parent of df1a00b... finish
=======
	ck.id = nrand()
	ck.preLeader = 0
	ck.requestID = 0
>>>>>>> parent of 60e9924... fix
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
	cnt := len(ck.servers)
=======
	ck.mu.Lock()
	args := GetArgs{Key: key, ClientID: ck.id}
	args.RequestID = ck.requestID
	ck.requestID++
	ck.mu.Unlock()
>>>>>>> parent of 60e9924... fix
	for {
		reply := GetReply{}
		ok := ck.servers[ck.preLeader].Call("RaftKV.Get", &args, &reply)
		if ok && reply.WrongLeader == false {
			return reply.Value
		}
		ck.preLeader = (ck.preLeader + 1) % len(ck.servers)
	}
<<<<<<< HEAD
=======
>>>>>>> parent of df1a00b... finish
=======
>>>>>>> parent of df1a00b... finish
	return ""
=======
>>>>>>> parent of 60e9924... fix
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
	cnt := len(ck.servers)
=======
	ck.mu.Lock()
	args := PutAppendArgs{Key: key, Value: value, Op: op, ClientID: ck.id}
	args.RequestID = ck.requestID
	ck.requestID++
	ck.mu.Unlock()
>>>>>>> parent of 60e9924... fix
	for {

		reply := PutAppendReply{}
		ok := ck.servers[ck.preLeader].Call("RaftKV.PutAppend", &args, &reply)
		if ok && reply.WrongLeader == false {
			return
		}
		ck.preLeader = (ck.preLeader + 1) % len(ck.servers)
	}
=======
>>>>>>> parent of df1a00b... finish
=======
>>>>>>> parent of df1a00b... finish
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
