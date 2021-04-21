package kvraft

import (
	"6.824/labrpc"
	"crypto/rand"
	"math/big"
	"sync/atomic"
)
//import "log"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	currentLeader int 
	clientID int64
	serialID int64
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
	ck.clientID = nrand()
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{
		Key : key,
		ClientID : ck.clientID,
		SerialID : atomic.AddInt64(&ck.serialID,1),
	}
	

	DPrintf("CLIENT[%v] -> Get[%v] -> KVSERVER", ck.clientID, args.SerialID)

	for i := 0; ;i++{
		reply := GetReply{}
		ok := ck.servers[ck.currentLeader].Call("KVServer.Get", &args, &reply)

		if ok{

			if reply.Err == OK{
				//log.Printf("KV server response: OK")
				return reply.Value

			}else if reply.Err == ErrWrongLeader{
				//log.Printf("KV server response: Wrong Leader Error")
 
			}else if reply.Err == ErrTimeOut{
				//log.Printf("KV server[%v] response: Timeout Error",ck.currentLeader)
				
			}else{
				//log.Printf("KV server response: Key Error")
				break
			}
			
		}
		ck.currentLeader++
		ck.currentLeader %= len(ck.servers)
	}

	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	args := PutAppendArgs{
		Key : key,
		Value: value,
		Op: op,
		ClientID : ck.clientID,
		SerialID : atomic.AddInt64(&ck.serialID,1),
	}
	

	DPrintf("CLIENT[%v] -> PutAppend[%v] -> KVSERVER", ck.clientID, args.SerialID)
	for i := 0; ;i++{
		reply := PutAppendReply{}
		ok := ck.servers[ck.currentLeader].Call("KVServer.PutAppend", &args, &reply)

		if ok{

			if reply.Err == OK{
			//	log.Printf("KV server response: OK")
				return 
			}else if reply.Err == ErrWrongLeader{
				//log.Printf("KV server response: Wrong Leader Error")

			}else if reply.Err == ErrTimeOut{
				//log.Printf("KV server[%v] response: Timeout Error",ck.currentLeader)
				
			}else{
				//log.Printf("KV server response: Key Error")
				break
			} 

		}
		ck.currentLeader++
		ck.currentLeader %= len(ck.servers)

	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
