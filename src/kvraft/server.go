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
	ClientID int64
	SerialID int64

}

type OpData struct{

	ErrResult Err
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
	clientLastCmd map[int64]int64
	waitingIndex map [int]chan OpData
	storage map [string]string
	isLeader bool
	term int 

}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	op := Op{
		OpType: "Get",
		Key: args.Key,
		ClientID: args.ClientID,
		SerialID: args.SerialID,
	}

	index, _, isLeader := kv.rf.Start(op)
	// kv.mu.Lock()
	// kv.term = term
	// kv.isLeader = isLeader 
	// kv.mu.Unlock()

	if isLeader{
		DPrintf("KVSERVER[%v] receives GET[%v] from CLIENT[%v]",kv.me, args.SerialID, args.ClientID)
		kv.mu.Lock()
		c := make(chan OpData, 1)
		kv.waitingIndex[index] = c
		kv.mu.Unlock()

		select {

		case opData := <- c:
			{
				DPrintf("KVSERVER[%v] RECEIVES COMMITTED %v Command[%v] with index[%v]",kv.me, op.OpType, op.SerialID, index)

				reply.Err = opData.ErrResult
				reply.Value = opData.Value
				
				kv.mu.Lock()
				delete(kv.waitingIndex, index)
				kv.mu.Unlock()

			}
		case <- time.After(1000 * time.Millisecond):
			DPrintf("KVSERVER[%v] TIMEOUT on %v Command[%v] with index[%v]",kv.me, op.OpType, op.SerialID, index)
			kv.mu.Lock()
			delete(kv.waitingIndex, index)
			kv.mu.Unlock()
			reply.Err = ErrTimeOut

		}


	}else{

		reply.Err = ErrWrongLeader

	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	op := Op{
		OpType: args.Op,
		Key: args.Key,
		Value: args.Value,
		ClientID: args.ClientID,
		SerialID: args.SerialID,
	}

	index, _, isLeader := kv.rf.Start(op)

	// kv.mu.Lock()
	// kv.term = term
	// kv.isLeader = isLeader 
	// kv.mu.Unlock()

	if isLeader{
		DPrintf("KVSERVER[%v] receives %v[%v] from CLIENT[%v]",kv.me, args.Op, args.SerialID, args.ClientID)
		kv.mu.Lock()
		c := make(chan OpData, 1)
		kv.waitingIndex[index] = c
		kv.mu.Unlock()

		select {

			case opData := <- c:
				DPrintf("KVSERVER[%v] RECEIVES COMMITTED %v Command[%v] with index[%v]",kv.me, op.OpType, op.SerialID, index)

				reply.Err = opData.ErrResult
				kv.mu.Lock()
				delete(kv.waitingIndex, index)
				kv.mu.Unlock()

			case <- time.After(1000 * time.Millisecond):
				DPrintf("KVSERVER[%v] TIMEOUT on %v Command[%v] with index[%v]",kv.me, op.OpType, op.SerialID, index)
				kv.mu.Lock()
				delete(kv.waitingIndex, index)
				kv.mu.Unlock()
				reply.Err = ErrTimeOut

		}


	}else{

		reply.Err = ErrWrongLeader

	}

}

func (kv *KVServer) executeOp(op Op) (string, Err){

	switch op.OpType{

		case "Get":
			{
				if val, ok := kv.storage[op.Key]; ok{

					return val, OK

				}else{

					return "", ErrNoKey

				}
				
			}
		case "Put":
			{
				if sid, ok := kv.clientLastCmd[op.ClientID]; !ok || (ok && op.SerialID > sid){
					kv.clientLastCmd[op.ClientID] = op.SerialID
					kv.storage[op.Key] = op.Value

				}
				
				return "", OK
			}

		case "Append":
			{
				if _, ok := kv.storage[op.Key]; ok{

					if sid, ok := kv.clientLastCmd[op.ClientID]; !ok || (ok && op.SerialID > sid){

						kv.clientLastCmd[op.ClientID] = op.SerialID
						kv.storage[op.Key] += op.Value

					}
					

				}else{

					if sid, ok := kv.clientLastCmd[op.ClientID]; !ok || (ok && op.SerialID > sid){
						
						kv.clientLastCmd[op.ClientID] = op.SerialID
						kv.storage[op.Key] = op.Value

					}

				}
				return "", OK

			}

	}

	return "", ErrWrongLeader

}


func (kv *KVServer) applyChListener(){
	
	for !kv.killed(){
		
		DPrintf("KVSERVER[%v] waiting for APPLY",kv.me)
		applyMsg := <- kv.applyCh
		DPrintf("KVSERVER[%v] got APPLY[%v] for index[%v]",kv.me, applyMsg.Command ,applyMsg.CommandIndex)
		if !applyMsg.CommandValid{
			continue 
		}

		op := applyMsg.Command.(Op)
		_, checkLeader := kv.rf.GetState()
		
		kv.mu.Lock()
		c, ok := kv.waitingIndex[applyMsg.CommandIndex]
		val, err := kv.executeOp(op)
		kv.mu.Unlock()

		if checkLeader && ok{
			c <- OpData{
				ErrResult: err,
				Value: val,
			}
		}

		DPrintf("KVSERVER[%v] sent through WaitingIndex for index[%v]",kv.me ,applyMsg.CommandIndex)

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
	kv.clientLastCmd = make(map[int64]int64)
	kv.waitingIndex = make(map[int]chan OpData)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	DPrintf("START KVSERVER[%v]",kv.me)
	// You may need initialization code here.
	go kv.applyChListener()

	return kv
}
