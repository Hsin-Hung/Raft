package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
import "sync/atomic"
import "log"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

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
	reply := GetReply{}

	for i := 0; ;i++{

		ok := ck.servers[i%len(ck.servers)].Call("KVServer.Get", &args, &reply)

		if ok{

			if reply.Err == OK{
				log.Printf("KV server response: OK")
				return reply.Value

			}else if reply.Err == ErrWrongLeader{

				log.Printf("KV server response: Wrong Leader Error")
				continue 

			}else{
				log.Printf("KV server response: Key Error")
			}

			break 
		}

	}

	return reply.Value
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
	reply := PutAppendReply{}

	for i := 0; ;i++{

		ok := ck.servers[i%len(ck.servers)].Call("KVServer.PutAppend", &args, &reply)

		if ok{

			if reply.Err == OK{
				log.Printf("KV server response: OK")
				return 
			}else if reply.Err == ErrWrongLeader{

				//log.Printf("KV server response: Wrong Leader Error")
				continue 

			}else{
				log.Printf("KV server response: Key Error")
			}

			break 
		}


	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) getLeader() int{


return -1



}