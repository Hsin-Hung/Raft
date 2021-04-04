package kvraft

import (
	"time"
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string 
	Key string
	Value string 

}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	waitingIndex map [int]chan bool
	storage map [string]string
	isLeader bool
	term int 

}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	op := Op{
		OpType: "Get",
		Key: args.Key,
	}

	index, term, isLeader := kv.rf.Start(op)
	kv.mu.Lock()
	kv.term = term
	kv.isLeader = isLeader 
	kv.mu.Unlock()
	if isLeader{
		log.Printf("%v request index %v", op.OpType, index)
		kv.mu.Lock()
		kv.waitingIndex[index-1] = make(chan bool)
		kv.mu.Unlock()

		select {

		case <- kv.waitingIndex[index-1]:
			{

				reply.Err = ErrNoKey
				if val, success := kv.executeOp(op); success{
					log.Printf("got it")
					reply.Err = OK
					reply.Value = val
				}


			}
		case <- time.NewTimer(10 * time.Second).C:
			log.Printf("TIMEOUT")
			reply.Err = ErrNoKey

	}


	}else{

		reply.Err = ErrWrongLeader

	}
	log.Printf("done")

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	op := Op{
		OpType: args.Op,
		Key: args.Key,
		Value: args.Value,
	}

	index, term, isLeader := kv.rf.Start(op)

	kv.mu.Lock()
	kv.term = term
	kv.isLeader = isLeader 
	kv.mu.Unlock()

	if isLeader{

		log.Printf("%v request index %v", op.OpType, index)
		kv.mu.Lock()
		kv.waitingIndex[index-1] = make(chan bool)
		kv.mu.Unlock()

		select {

			case <- kv.waitingIndex[index-1]:
				log.Printf("got it")
				reply.Err = OK
				kv.executeOp(op)

			case <- time.NewTimer(10 * time.Second).C:
				log.Printf("TIMEOUT")
				reply.Err = ErrNoKey

		}


	}else{

		reply.Err = ErrWrongLeader

	}

	log.Printf("done")

}

func (kv *KVServer) executeOp(op Op) (string, bool){

	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch op.OpType{

		case "Get":
			{
				if val, ok := kv.storage[op.Key]; ok{

					return val, true

				}else{

					return "", false 

				}
				
			}
		case "Put":
			{
				kv.storage[op.Key] = op.Value
				return "", true
			}

		case "Append":
			{
				if val, ok := kv.storage[op.Key]; ok{

					kv.storage[op.Key] = val + op.Value

				}else{

					kv.storage[op.Key] = op.Value

				}
				return "", true

			}

	}

	return "", false

}


func (kv *KVServer) applyChListener(){
	
	for {
		
		log.Printf("waiting for apply ch")
		applyMsg := <- kv.applyCh
		log.Printf("got apply ch %v", applyMsg.CommandIndex)
		kv.mu.Lock()
		kv.waitingIndex[applyMsg.CommandIndex] <- true
		kv.mu.Unlock()
		log.Printf("send thru")
		time.Sleep(10 * time.Millisecond)

	}

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.storage = make(map[string]string)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.waitingIndex = make(map[int]chan bool)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	log.Printf("start kv server")
	// You may need initialization code here.
	go kv.applyChListener()

	return kv
}
