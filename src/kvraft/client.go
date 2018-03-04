package raftkv

<<<<<<< HEAD
import (
	"math/big"
	"crypto/rand"
	"../labrpc"
	"time"
)
=======
import "labrpc"
import "crypto/rand"
import "math/big"
>>>>>>> parent of df1a00b... finish

var clients = make(map[int64]bool)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
<<<<<<< HEAD

	leader int   // remember last leader
	seq    int   // RPC sequence number
	id     int64 // client id
=======
>>>>>>> parent of df1a00b... finish
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func generateID() int64 {
	for {
		x := nrand()
		if clients[x] {
			continue
		}
		clients[x] = true
		return x
	}
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers

	// You'll have to add code here.
<<<<<<< HEAD
	ck.leader = len(servers)
	ck.seq = 1
	ck.id = generateID()

	DPrintf("Clerk: %d\n", ck.id)

=======
>>>>>>> parent of df1a00b... finish
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
	DPrintf("Clerk: Get: %q\n", key)
	// You will have to modify this function.
<<<<<<< HEAD
	cnt := len(ck.servers)
	for {
		args := &GetArgs{Key: key, ClientID: ck.id, SeqNo: ck.seq}
		reply := new(GetReply)

		ck.leader %= cnt
		done := make(chan bool, 1)
		go func() {
			ok := ck.servers[ck.leader].Call("RaftKV.Get", args, reply)
			done <- ok
		}()
		select {
		case <-time.After(200 * time.Millisecond): // rpc timeout: 200ms
			ck.leader++
			continue
		case ok := <-done:
			if ok && !reply.WrongLeader {
				ck.seq++
				if reply.Err == OK {
					return reply.Value
				}
				return ""
			}
			ck.leader++
		}
	}
=======
>>>>>>> parent of df1a00b... finish
	return ""
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
	DPrintf("Clerk: PutAppend: %q => (%q,%q) from: %d\n", op, key, value, ck.id)
	// You will have to modify this function.
<<<<<<< HEAD
	cnt := len(ck.servers)
	for {
		args := &PutAppendArgs{Key: key, Value: value, Op: op, ClientID: ck.id, SeqNo: ck.seq}
		reply := new(PutAppendReply)

		ck.leader %= cnt
		done := make(chan bool, 1)
		go func() {
			ok := ck.servers[ck.leader].Call("RaftKV.PutAppend", args, reply)
			done <- ok
		}()
		select {
		case <-time.After(200 * time.Millisecond): // rpc timeout: 200ms
			ck.leader++
			continue
		case ok := <-done:
			if ok && !reply.WrongLeader && reply.Err == OK {
				ck.seq++
				return
			}
			ck.leader++
		}
	}
=======
>>>>>>> parent of df1a00b... finish
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}