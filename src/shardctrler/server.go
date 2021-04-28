package shardctrler


import (
	"sort"
	"6.824/raft"
	"6.824/labrpc"
	"sync"
	"6.824/labgob"
	"log"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs []Config // indexed by config num
	shard_dis map[int][]int // GID -> []Shards
	unassigned_shards []int // shards that are not assigned to any GID

	clientLastCmd map[int64]int64 // this will deal with duplicate command, it stores the most recent cmd for each client 
	waitingIndex map [int]chan OpData	// this is the channel for sending back cmd execution for each index 

	lastIncludedIndex int
}

const (
	Join = iota
	Leave
	Move
	Query
)


type Op struct {
	// Your data here.
	OpType int 
	Servers map[int][]string // for join
	Gids []int // for leave
	Shard int // for move
	Gid int // for move
	Num int // for query 
	ClientID int64
	SerialID int64
}

type OpData struct{

	ErrResult Err
	Value string 
	Config      Config
}

func (sc *ShardCtrler) balanceShards(gids []int, shards [NShards]int, groups map[int][]string, unassigned_shards []int) [NShards]int{

	below_avg := make([]int, 0) // gids that are below the average shards, which means they need to be assign more shards 

	majors := NShards/len(groups) + 1 // majority of gids need this many shards 
	num_majors := NShards % len(groups) // num of gids that need 'majors' shards

	minors := majors - 1 // the rest of gids need this many shards 
	num_minors := len(groups) - num_majors // num of gids that need 'minors' shards
	
	for _, gid := range gids{

		v := sc.shard_dis[gid]

		if len(v)==majors && num_majors>0{
			num_majors--
		}else if len(v)==minors && num_minors>0{
			num_minors--
		}else if len(v) > majors && num_majors>0{		
			sc.shard_dis[gid] = v[len(v)-majors:]
			unassigned_shards = append(unassigned_shards, v[:len(v)-majors]...)
			num_majors--
		}else if len(v) > minors && num_minors>0{
			sc.shard_dis[gid] = v[len(v)-minors:]
			unassigned_shards = append(unassigned_shards, v[:len(v)-minors]...)
			num_minors--
		}else if len(v) < minors {
			below_avg = append(below_avg, gid)
		}

	}

	//go through all gids that need re-distribution 
	for _, gid := range below_avg{

		num_shards := len(sc.shard_dis[gid])

		if num_majors>0{

			for i:=0; i < majors-num_shards;i++{

				s := unassigned_shards[0]
				unassigned_shards =  unassigned_shards[1:]
				sc.shard_dis[gid] = append(sc.shard_dis[gid], s)
				shards[s] = gid 

			}

			num_majors--

		}else if num_minors>0{

			for i:=0; i < minors-num_shards;i++{

				s := unassigned_shards[0]
				unassigned_shards =  unassigned_shards[1:]
				sc.shard_dis[gid] = append(sc.shard_dis[gid	], s)
				shards[s] = gid 

			}

			num_minors--

		}


	}

	sc.unassigned_shards = unassigned_shards

	return shards


}

// execute join logics 
func (sc *ShardCtrler) exeJoin(op Op) OpData{

	opData := OpData{
		ErrResult: ErrWrongLeader,
	}

	// detect duplicate command
	if sid, ok := sc.clientLastCmd[op.ClientID]; !ok || (ok && op.SerialID > sid){

		opData.ErrResult = OK
		sc.clientLastCmd[op.ClientID] = op.SerialID

		currentConfig := sc.configs[len(sc.configs)-1]
		currentGruops := make(map[int][]string)
		currentShards := currentConfig.Shards
		unassigned_shards := sc.unassigned_shards
		gids := make([]int,0)

		for k, v := range currentConfig.Groups{
			currentGruops[k] = v
		}
	
		for k, v := range op.Servers{
			currentGruops[k] = v
			sc.shard_dis[k] = make([]int,0)
		}

		for k, _ := range currentGruops{
			gids = append(gids, k)
		}
		sort.Ints(gids)
	
		currentShards = sc.balanceShards(gids, currentShards, currentGruops, unassigned_shards)
	
		newConfig := Config{
			Num: len(sc.configs),
			Shards: currentShards,
			Groups: currentGruops,
		}
		
		sc.configs = append(sc.configs, newConfig)
	
	}

	return opData

}

// execute leave logics 
func (sc *ShardCtrler) exeLeave(op Op) OpData{
	opData := OpData{
		ErrResult: ErrWrongLeader,
	}

	if sid, ok := sc.clientLastCmd[op.ClientID]; !ok || (ok && op.SerialID > sid){

		opData.ErrResult = OK
		sc.clientLastCmd[op.ClientID] = op.SerialID
		currentConfig := sc.configs[len(sc.configs)-1]
		currentGruops := make(map[int][]string)
		currentShards := currentConfig.Shards
		unassigned_shards := sc.unassigned_shards
		gids := make([]int,0)
	
		for k, v := range currentConfig.Groups{
			currentGruops[k] = v
		}
	
		for _, gid := range op.Gids{
	
			delete(currentGruops, gid)
	
			for _, s := range sc.shard_dis[gid]{
	
				unassigned_shards = append(unassigned_shards, s)
	
			}
	
			delete(sc.shard_dis, gid)
	
		}

		for k, _ := range currentGruops{
			gids = append(gids, k)
		}
		sort.Ints(gids)
	
		if (len(currentGruops)==0){
	
			newConfig := Config{
				Num: len(sc.configs),
				Shards: currentShards,
				Groups: currentGruops,
			}
		
			sc.unassigned_shards = unassigned_shards
			sc.configs = append(sc.configs, newConfig)
			return opData
	
		}
		currentShards = sc.balanceShards(gids, currentShards, currentGruops, unassigned_shards)
	
		newConfig := Config{
			Num: len(sc.configs),
			Shards: currentShards,
			Groups: currentGruops,
		}
	
		sc.configs = append(sc.configs, newConfig)

	}
	return opData
	

}

//execute move logic
func (sc *ShardCtrler) exeMove(op Op) OpData{
	opData := OpData{
		ErrResult: ErrWrongLeader,
	}

	if sid, ok := sc.clientLastCmd[op.ClientID]; !ok || (ok && op.SerialID > sid){
		opData.ErrResult = OK
		sc.clientLastCmd[op.ClientID] = op.SerialID

	currentConfig := sc.configs[len(sc.configs)-1]
	currentGruops := make(map[int][]string)
	currentShards := currentConfig.Shards

	for k, v := range currentConfig.Groups{
		currentGruops[k] = v
	}

	original_gid := currentShards[op.Shard]
	sc.shard_dis[op.Gid] = append(sc.shard_dis[op.Gid], op.Shard)
	for i, v := range sc.shard_dis[original_gid]{

		if v == op.Shard{
			// remove the shard from this original gid's shard distribution 
			sc.shard_dis[original_gid] = append(sc.shard_dis[original_gid][:i], sc.shard_dis[original_gid][i+1:]...)
			break

		}

	}

	currentShards[op.Shard] = op.Gid

	newConfig := Config{
		Num: len(sc.configs),
		Shards: currentShards,
		Groups: currentGruops,
	}

	sc.configs = append(sc.configs, newConfig)

}

return opData
	
}

//execute query logic
func (sc *ShardCtrler) exeQuery(op Op) OpData{
	opData := OpData{}

	if sid, ok := sc.clientLastCmd[op.ClientID]; !ok || (ok && op.SerialID > sid){
		opData.ErrResult = OK
		sc.clientLastCmd[op.ClientID] = op.SerialID
		if op.Num == -1 || op.Num >= len(sc.configs){

			opData.Config = sc.configs[len(sc.configs)-1]
	
		}else{
	
			opData.Config = sc.configs[op.Num]
	
		}

	}

	return opData
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	
	op := Op{

		OpType: Join,
		Servers: args.Servers,
		ClientID: args.ClientID,
		SerialID: args.SerialID,

	}


	index, _, isLeader := sc.rf.Start(op)


	if isLeader{
		sc.mu.Lock()
		c := make(chan OpData, 1)
		sc.waitingIndex[index] = c
		sc.mu.Unlock()

		select {

		case opData := <- c:
			{
				reply.Err = opData.ErrResult
				reply.WrongLeader = false

			}
		case <- time.After(1000 * time.Millisecond):
			reply.WrongLeader = true
		}

		sc.mu.Lock()
		delete(sc.waitingIndex, index)
		sc.mu.Unlock()

	}else{

		reply.WrongLeader = true

	}

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here. GIDs []int
	
	op := Op{

		OpType: Leave,
		Gids: args.GIDs,
		ClientID: args.ClientID,
		SerialID: args.SerialID,

	}


	index, _, isLeader := sc.rf.Start(op)


	if isLeader{
		sc.mu.Lock()
		c := make(chan OpData, 1)
		sc.waitingIndex[index] = c
		sc.mu.Unlock()

		select {

		case opData := <- c:
			{
				reply.Err = opData.ErrResult
				reply.WrongLeader = false
			}
		case <- time.After(1000 * time.Millisecond):
			reply.WrongLeader = true
		}
		sc.mu.Lock()
		delete(sc.waitingIndex, index)
		sc.mu.Unlock()
	}else{

		reply.WrongLeader = true

	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.

	
	op := Op{

		OpType: Move,
		Shard: args.Shard,
		Gid: args.GID,
		ClientID: args.ClientID,
		SerialID: args.SerialID,

	}


	index, _, isLeader := sc.rf.Start(op)


	if isLeader{
		sc.mu.Lock()
		c := make(chan OpData, 1)
		sc.waitingIndex[index] = c
		sc.mu.Unlock()

		select {

		case opData := <- c:
			{

				reply.Err = opData.ErrResult
				reply.WrongLeader = false

			}
		case <- time.After(1000 * time.Millisecond):
			reply.WrongLeader = true
		}
		sc.mu.Lock()
		delete(sc.waitingIndex, index)
		sc.mu.Unlock()
	}else{

		reply.WrongLeader = true

	}

}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here. Num int 
	op := Op{
		OpType: Query,
		Num: args.Num,
		ClientID: args.ClientID,
		SerialID: args.SerialID,
	}

	index, _, isLeader := sc.rf.Start(op)

	if isLeader{
		sc.mu.Lock()
		c := make(chan OpData, 1)
		sc.waitingIndex[index] = c
		sc.mu.Unlock()
		select {

		case opData := <- c:
			{

				reply.Err = opData.ErrResult
				reply.Config = opData.Config
				reply.WrongLeader = false

			}
		case <- time.After(1000 * time.Millisecond):
			reply.WrongLeader = true
		}

		sc.mu.Lock()
		delete(sc.waitingIndex, index)
		sc.mu.Unlock()



	}else{

		reply.WrongLeader = true

	}

}

func (sc *ShardCtrler) executeOp(op Op) OpData{


	switch(op.OpType){

	case Join:
		return sc.exeJoin(op)
	case Leave:
		return sc.exeLeave(op)
	case Move:
		return sc.exeMove(op)
	case Query:
		return sc.exeQuery(op)
	default:
		log.Printf("No such Operation type !")

	}


	return OpData{}
}

//routine to listen for applied cmd from raft 
func (sc *ShardCtrler) applyChListener(){
	
	for {

		applyMsg := <- sc.applyCh

				if !applyMsg.CommandValid{
					continue 
				}
				op := applyMsg.Command.(Op)

				_, checkLeader := sc.rf.GetState()
				
				sc.mu.Lock()
				c, ok := sc.waitingIndex[applyMsg.CommandIndex]
				opData := sc.executeOp(op)
				sc.lastIncludedIndex = applyMsg.CommandIndex
				sc.mu.Unlock()
		
				if checkLeader && ok{
					c <- opData
				}

	}

}


//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(JoinReply{})
	labgob.Register(LeaveArgs{})
	labgob.Register(LeaveReply{})
	labgob.Register(MoveArgs{})
	labgob.Register(MoveReply{})
	labgob.Register(QueryArgs{})
	labgob.Register(QueryReply{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	// Your code here.
	sc.clientLastCmd = make(map[int64]int64)
	sc.waitingIndex = make(map[int]chan OpData)
	sc.shard_dis = make(map[int][]int)
	sc.unassigned_shards = make([]int,0)
	for i := 0; i < NShards; i++{
		sc.unassigned_shards = append(sc.unassigned_shards, i)
	}
	go sc.applyChListener()
	return sc
}
