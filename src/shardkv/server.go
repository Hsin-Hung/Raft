package shardkv


import "6.824/labrpc"
import "6.824/raft"
import "sync"
import "6.824/labgob"
import "time"
import "log"
import "bytes"

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

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	clientLastCmd map[int64]int64 // this will deal with duplicate command, it stores the most recent cmd for each client 
	waitingIndex map [int]chan OpData	// this is the channel for sending back cmd execution for each index 
	storage map [string]string // database for kvserver 

	lastIncludedIndex int

	killCh chan bool // to kill apply chan loop when server gets killed
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		OpType: "Get",
		Key: args.Key,
		ClientID: args.ClientID,
		SerialID: args.SerialID,
	}

	// start this cmd by commiting in raft first
	index, _, isLeader := kv.rf.Start(op)

	if isLeader{
		DPrintf("KVSERVER[%v] receives GET[%v] from CLIENT[%v]",kv.me, args.SerialID, args.ClientID)

		kv.mu.Lock()
		// create a channel to send back the result of this operation after it gets applied
		c := make(chan OpData, 1)
		kv.waitingIndex[index] = c
		kv.mu.Unlock()

		select {

		case opData := <- c:
			{
				DPrintf("KVSERVER[%v] RECEIVES COMMITTED %v Command[%v] with index[%v]",kv.me, op.OpType, op.SerialID, index)
				reply.Err = opData.ErrResult
				reply.Value = opData.Value
				

			}
		case <- time.After(1000 * time.Millisecond):
			DPrintf("KVSERVER[%v] TIMEOUT on %v Command[%v] with index[%v]",kv.me, op.OpType, op.SerialID, index)
			reply.Err = ErrTimeOut

		}

		kv.mu.Lock()
		delete(kv.waitingIndex, index)
		kv.mu.Unlock()


	}else{

		reply.Err = ErrWrongLeader

	}

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		OpType: args.Op,
		Key: args.Key,
		Value: args.Value,
		ClientID: args.ClientID,
		SerialID: args.SerialID,
	}

	// start this cmd by commiting in raft first 
	index, _, isLeader := kv.rf.Start(op)

	if isLeader{
		DPrintf("KVSERVER[%v] receives %v[%v] from CLIENT[%v]",kv.me, args.Op, args.SerialID, args.ClientID)
		kv.mu.Lock()
		// create a channel to send back the result of this operation after it gets applied
		c := make(chan OpData, 1)
		kv.waitingIndex[index] = c
		kv.mu.Unlock()

		select {

			case opData := <- c:
				DPrintf("KVSERVER[%v] RECEIVES COMMITTED %v Command[%v] with index[%v]",kv.me, op.OpType, op.SerialID, index)
				reply.Err = opData.ErrResult

			case <- time.After(1000 * time.Millisecond):
				DPrintf("KVSERVER[%v] TIMEOUT on %v Command[%v] with index[%v]",kv.me, op.OpType, op.SerialID, index)
				reply.Err = ErrTimeOut

		}
		
		kv.mu.Lock()
		delete(kv.waitingIndex, index)
		kv.mu.Unlock()

	}else{

		reply.Err = ErrWrongLeader

	}

}

// execute the given command on kvserver 
func (kv *ShardKV) executeOp(op Op) (string, Err){

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

//routine to listen for applied cmd from raft 
func (kv *ShardKV) applyChListener(){
	
	for {
		
		select {

		case applyMsg := <- kv.applyCh:
			{

				// deal with applied snap shot
				if applyMsg.SnapshotValid{
					index := applyMsg.SnapshotIndex
					data := applyMsg.Snapshot
					storage := make(map[string]string)
					clientLastCMD := make(map[int64]int64)
					if !(index <=-1 || data == nil || len(data) < 1){ // bootstrap without any state?
						r := bytes.NewBuffer(data)
						d := labgob.NewDecoder(r)
						if d.Decode(&storage) != nil ||
						d.Decode(&clientLastCMD) != nil {
							log.Printf("error decode")
							return
						}
					}
				
					kv.mu.Lock()
					kv.storage = storage
					kv.clientLastCmd = clientLastCMD
					kv.lastIncludedIndex = index 
					kv.mu.Unlock()
					// kv.readSnapShot()
					continue 
				}
				//ignore all non-valid cmd 
				if !applyMsg.CommandValid{
					continue 
				}
		
				op := applyMsg.Command.(Op)
				_, checkLeader := kv.rf.GetState()
				
				kv.mu.Lock()
				c, ok := kv.waitingIndex[applyMsg.CommandIndex]
				val, err := kv.executeOp(op)
				kv.lastIncludedIndex = applyMsg.CommandIndex
				kv.mu.Unlock()
		
				if checkLeader && ok{
					DPrintf("KVSERVER[%v] sent through WaitingIndex for index[%v]",kv.me ,applyMsg.CommandIndex)
					c <- OpData{
						ErrResult: err,
						Value: val,
					}
				}


			}
		case <- kv.killCh:
			return

		}


	}

}


//start snap shotting when state gets too large 
func (kv *ShardKV) snapshotRoutine(){

	for {

		if kv.rf.RaftStateSize() >= kv.maxraftstate && kv.maxraftstate != -1{
			go kv.saveSnapShot()
		}
		time.Sleep(5 * time.Millisecond)
	}

}



func (kv *ShardKV) saveSnapShot(){

	kv.mu.Lock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.storage)
	e.Encode(kv.clientLastCmd)
	index := kv.lastIncludedIndex
	data := w.Bytes()
	kv.mu.Unlock()
	kv.rf.Snapshot(index, data)

}

func (kv *ShardKV) readSnapShot(){
	index := kv.rf.GetLastIncludedIndex()
	data := kv.rf.ReadSnapshot()

	storage := make(map[string]string)
	clientLastCMD := make(map[int64]int64)
	if !(index <=-1 || data == nil || len(data) < 1){ // bootstrap without any state?
		r := bytes.NewBuffer(data)
		d := labgob.NewDecoder(r)
		if d.Decode(&storage) != nil ||
		d.Decode(&clientLastCMD) != nil {
			log.Printf("error decode")
			return
		}
	}

	kv.mu.Lock()
	kv.storage = storage
	kv.clientLastCmd = clientLastCMD
	kv.lastIncludedIndex = index 
	kv.mu.Unlock()
}
//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func (kv *ShardKV) MakeClerk(ctrlers []*labrpc.ClientEnd){





}


func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.storage = make(map[string]string)
	kv.clientLastCmd = make(map[int64]int64)
	kv.waitingIndex = make(map[int]chan OpData)
	kv.killCh = make(chan bool)
	// Use something like this to talk to the shardctrler:
	//kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	DPrintf("START KVSERVER[%v]",kv.me)
	kv.readSnapShot()
	go kv.applyChListener()
	go kv.snapshotRoutine()
	return kv
}
